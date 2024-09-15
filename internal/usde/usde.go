package usde

import (
	"context"
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
func ResourceNeedsUpdating(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject, comparisonExcludedPaths []string) (bool, UpgradeStrategy, error) {

	numaLogger := logger.FromContext(ctx)

	// Get Numaplane Config
	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

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
	newSpecOnlyApplyPaths, newSpecWithoutApplyPaths, err := util.SplitObject(newSpec.Spec.Raw, applyPaths, comparisonExcludedPaths, ".")
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"newSpecOnlyApplyPaths", newSpecOnlyApplyPaths,
		"newSpecWithoutApplyPaths", newSpecWithoutApplyPaths,
	).Debug("split new spec")

	// Split existingSpec
	existingSpecOnlyApplyPaths, existingSpecWithoutApplyPaths, err := util.SplitObject(existingSpec.Spec.Raw, applyPaths, comparisonExcludedPaths, ".")
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"existingSpecOnlyApplyPaths", existingSpecOnlyApplyPaths,
		"existingSpecWithoutApplyPaths", existingSpecWithoutApplyPaths,
	).Debug("split existing spec")

	// Compare specs without the apply fields and check user's strategy to either return PPND or Progressive
	if !reflect.DeepEqual(newSpecWithoutApplyPaths, existingSpecWithoutApplyPaths) {
		namespaceConfig := config.GetConfigManagerInstance().GetNamespaceConfig(newSpec.Namespace)
		userUpgradeStrategy := globalConfig.DefaultUpgradeStrategy
		if namespaceConfig != nil {
			userUpgradeStrategy = namespaceConfig.UpgradeStrategy
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
		default:
			return true, apiv1.UpgradeStrategyApply, nil
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

func GetUserStrategy(namespace string) config.USDEUserStrategy {
	namespaceConfig := config.GetConfigManagerInstance().GetNamespaceConfig(namespace)

	var userUpgradeStrategy config.USDEUserStrategy = ""
	if namespaceConfig != nil {
		userUpgradeStrategy = namespaceConfig.UpgradeStrategy
	}
	return userUpgradeStrategy
}
