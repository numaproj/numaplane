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
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// ResourceNeedsUpdating calculates the upgrade strategy to use during the
// resource reconciliation process based on configuration and user preference (see design doc for details).
// It returns the strategy to use and a boolean pointer indicating if the specs are different (if the specs were not compared, then nil will be returned).
func ResourceNeedsUpdating(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {

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

	// Compare specs without the apply fields and check user's strategy to either return PPND or Progressive
	if !reflect.DeepEqual(newSpecWithoutApplyPaths, existingSpecWithoutApplyPaths) {
		userUpgradeStrategy, err := GetUserStrategy(ctx, newSpec.Namespace)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, err
		}

		numaLogger.WithValues(
			"userUpgradeStrategy", userUpgradeStrategy,
			"newSpecWithoutApplyPaths", newSpecWithoutApplyPaths,
			"existingSpecWithoutApplyPaths", existingSpecWithoutApplyPaths,
		).Debug("the specs without the 'apply' paths are different")

		switch userUpgradeStrategy {
		case config.PPNDStrategyID:
			return true, apiv1.UpgradeStrategyPPND, nil
		case config.ProgressiveStrategyID:
			return true, apiv1.UpgradeStrategyProgressive, nil
		case config.NoStrategyID:
			return true, apiv1.UpgradeStrategyApply, nil
		default:
			return false, apiv1.UpgradeStrategyError, fmt.Errorf("invalid Upgrade Strategy: %v", userUpgradeStrategy)
		}
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
