package usde

import (
	"context"
	"fmt"
	"reflect"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// ResourceNeedsUpdating calculates the upgrade strategy to use during the
// resource reconciliation process based on configuration and user preference (see design doc for details).
// It returns the strategy to use and a boolean pointer indicating if the specs are different (if the specs were not compared, then nil will be returned).
func ResourceNeedsUpdating(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {

	metadataNeedsUpdating, metadataUpgradeStrategy, err := resourceMetadataNeedsUpdating(ctx, newSpec, existingSpec)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	specNeedsUpdating, specUpgradeStrategy, err := resourceSpecNeedsUpdating(ctx, newSpec, existingSpec)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}
	if !metadataNeedsUpdating && !specNeedsUpdating {
		return false, apiv1.UpgradeStrategyNoOp, nil
	}
	return true, getMostConservativeStrategy([]apiv1.UpgradeStrategy{metadataUpgradeStrategy, specUpgradeStrategy}), nil

}

func resourceSpecNeedsUpdating(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {

	numaLogger := logger.FromContext(ctx)

	// Get USDE Config
	usdeConfig := config.GetConfigManagerInstance().GetUSDEConfig()

	// Get apply paths based on the spec type (Pipeline, ISBS)
	applyPaths := []string{}
	if reflect.DeepEqual(newSpec.GroupVersionKind(), numaflowv1.PipelineGroupVersionKind) {
		applyPaths = usdeConfig.PipelineSpecExcludedPaths
	} else if reflect.DeepEqual(newSpec.GroupVersionKind(), numaflowv1.ISBGroupVersionKind) {
		applyPaths = usdeConfig.ISBServiceSpecExcludedPaths
	}

	numaLogger.WithValues("usdeConfig", usdeConfig, "applyPaths", applyPaths).Debug("started deriving upgrade strategy")

	// Split newSpec
	newSpecOnlyApplyPaths, newSpecWithoutApplyPaths, err := util.SplitObject(newSpec.Spec.Raw, applyPaths, []string{}, ".")
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"newSpecOnlyApplyPaths", newSpecOnlyApplyPaths,
		"newSpecWithoutApplyPaths", newSpecWithoutApplyPaths,
	).Debug("split new spec")

	// Split existingSpec
	existingSpecOnlyApplyPaths, existingSpecWithoutApplyPaths, err := util.SplitObject(existingSpec.Spec.Raw, applyPaths, []string{}, ".")
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"existingSpecOnlyApplyPaths", existingSpecOnlyApplyPaths,
		"existingSpecWithoutApplyPaths", existingSpecWithoutApplyPaths,
	).Debug("split existing spec")

	// Compare specs without the apply fields and check user's strategy to return their preferred strategy
	if !reflect.DeepEqual(newSpecWithoutApplyPaths, existingSpecWithoutApplyPaths) {
		upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newSpec.Namespace)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, err
		}

		numaLogger.WithValues(
			"upgradeStrategy", upgradeStrategy,
			"newSpecWithoutApplyPaths", newSpecWithoutApplyPaths,
			"existingSpecWithoutApplyPaths", existingSpecWithoutApplyPaths,
		).Debug("the specs without the 'apply' paths are different")

		return true, upgradeStrategy, nil
	}

	// Compare specs with the apply fields
	if !reflect.DeepEqual(newSpecOnlyApplyPaths, existingSpecOnlyApplyPaths) {
		numaLogger.WithValues(
			"newSpecOnlyApplyPaths", newSpecOnlyApplyPaths,
			"existingSpecOnlyApplyPaths", existingSpecOnlyApplyPaths,
		).Debug("the specs with only the 'apply' paths are different")

		return true, apiv1.UpgradeStrategyApply, nil
	}

	numaLogger.Debug("the specs are equal, no update needed")

	// Return NoOp if no differences were found between the new and existing specs
	return false, apiv1.UpgradeStrategyNoOp, nil

}

func getMostConservativeStrategy(strategies []apiv1.UpgradeStrategy) apiv1.UpgradeStrategy {
	strategy := apiv1.UpgradeStrategyNoOp
	for _, s := range strategies {
		if strategyRating[s] > strategyRating[strategy] {
			strategy = s
		}
	}
	return strategy
}

var (
	strategyRating map[apiv1.UpgradeStrategy]int = map[apiv1.UpgradeStrategy]int{
		apiv1.UpgradeStrategyNoOp:        0,
		apiv1.UpgradeStrategyApply:       1,
		apiv1.UpgradeStrategyPPND:        2,
		apiv1.UpgradeStrategyProgressive: 2,
	}
)

func resourceMetadataNeedsUpdating(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {
	upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newSpec.Namespace)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	// TODO: make this configurable to look for particular Labels and Annotations rather than this specific one
	instanceIDNew := newSpec.Annotations[common.AnnotationKeyNumaflowInstanceID]
	instanceIDExisting := existingSpec.Annotations[common.AnnotationKeyNumaflowInstanceID]
	if instanceIDNew != instanceIDExisting {
		return true, upgradeStrategy, nil
	} else {
		return false, apiv1.UpgradeStrategyNoOp, nil
	}
}

// return the upgrade strategy that represents what the user prefers to do when there's a concern for data loss
func getDataLossUpggradeStrategy(ctx context.Context, namespace string) (apiv1.UpgradeStrategy, error) {
	userUpgradeStrategy, err := GetUserStrategy(ctx, namespace)
	if err != nil {
		return apiv1.UpgradeStrategyError, err
	}

	switch userUpgradeStrategy {
	case config.PPNDStrategyID:
		return apiv1.UpgradeStrategyPPND, nil
	case config.ProgressiveStrategyID:
		return apiv1.UpgradeStrategyProgressive, nil
	case config.NoStrategyID:
		return apiv1.UpgradeStrategyApply, nil
	default:
		return apiv1.UpgradeStrategyError, fmt.Errorf("invalid Upgrade Strategy: %v", userUpgradeStrategy)
	}
}

func GetUserStrategy(ctx context.Context, namespace string) (config.USDEUserStrategy, error) {
	numaLogger := logger.FromContext(ctx)

	namespaceConfig := config.GetConfigManagerInstance().GetNamespaceConfig(namespace)

	var userUpgradeStrategy config.USDEUserStrategy = config.GetConfigManagerInstance().GetUSDEConfig().DefaultUpgradeStrategy
	if namespaceConfig != nil {
		if !namespaceConfig.UpgradeStrategy.IsValid() {
			numaLogger.WithValues("upgrade strategy", namespaceConfig.UpgradeStrategy).Warnf("invalid Upgrade strategy for namespace %s", namespace)
		} else {
			userUpgradeStrategy = namespaceConfig.UpgradeStrategy
		}
	}
	return userUpgradeStrategy, nil
}
