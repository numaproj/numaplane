package usde

import (
	"context"
	"reflect"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type UpgradeStrategy string

const (
	UpgradeStrategyError       UpgradeStrategy = ""
	UpgradeStrategyNoOp        UpgradeStrategy = "NoOp"
	UpgradeStrategyApply       UpgradeStrategy = "DirectApply"
	UpgradeStrategyPPND        UpgradeStrategy = "PipelinePauseAndDrain"
	UpgradeStrategyProgressive UpgradeStrategy = "Progressive"
)

// DeriveUpgradeStrategy calculates the upgrade strategy to use during the
// resource reconciliation process based on configuration and user preference (see design doc for details).
// It returns the strategy to use and a boolean pointer indicating if the specs are different (if the specs were not compared, then nil will be returned).
func DeriveUpgradeStrategy(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject,
	inProgressUpgradeStrategy string, overrideToPPND, overrideToProgressive *bool) (UpgradeStrategy, *bool, error) {

	numaLogger := logger.FromContext(ctx)

	if UpgradeStrategy(inProgressUpgradeStrategy) == UpgradeStrategyPPND || (overrideToPPND != nil && *overrideToPPND) {
		return UpgradeStrategyPPND, nil, nil
	}

	if UpgradeStrategy(inProgressUpgradeStrategy) == UpgradeStrategyProgressive || (overrideToProgressive != nil && *overrideToProgressive) {
		// TODO-PROGRESSIVE: return UpgradeStrategyProgressive instead of UpgradeStrategyPPND
		return UpgradeStrategyPPND, nil, nil
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
	newSpecOnlyApplyPaths, newSpecWithoutApplyPaths, err := util.SplitObject(newSpec.Spec.Raw, applyPaths, ".")
	if err != nil {
		return UpgradeStrategyError, nil, err
	}

	numaLogger.WithValues("newSpecOnlyApplyPaths", newSpecOnlyApplyPaths, "newSpecWithoutApplyPaths", newSpecWithoutApplyPaths).Debug("split new spec")

	// Split existingSpec
	existingSpecOnlyApplyPaths, existingSpecWithoutApplyPaths, err := util.SplitObject(existingSpec.Spec.Raw, applyPaths, ".")
	if err != nil {
		return UpgradeStrategyError, nil, err
	}

	numaLogger.WithValues("existingSpecOnlyApplyPaths", existingSpecOnlyApplyPaths, "existingSpecWithoutApplyPaths", existingSpecWithoutApplyPaths).Debug("split existing spec")

	specsDiffer := true

	// Compare specs without the apply fields and check user's strategy to either return PPND or Progressive
	if !reflect.DeepEqual(newSpecWithoutApplyPaths, existingSpecWithoutApplyPaths) {
		userUpgradeStrategy := config.GetConfigManagerInstance().GetNamespaceConfig(newSpec.Namespace).UpgradeStrategy

		numaLogger.WithValues("userUpgradeStrategy", userUpgradeStrategy).Debug("the specs without the 'apply' paths are different")

		if userUpgradeStrategy == config.PPNDStrategyID {
			return UpgradeStrategyPPND, &specsDiffer, nil
		}

		// TODO-PROGRESSIVE: return UpgradeStrategyProgressive instead of UpgradeStrategyPPND
		return UpgradeStrategyPPND, &specsDiffer, nil
	}

	// Compare specs with the apply fields
	if !reflect.DeepEqual(newSpecOnlyApplyPaths, existingSpecOnlyApplyPaths) {
		numaLogger.Debug("the specs with only the 'apply' paths are different")
		return UpgradeStrategyApply, &specsDiffer, nil
	}

	numaLogger.Debug("the specs are equal, no update needed")

	// Return NoOp if no differences were found between the new and existing specs
	specsDiffer = false
	return UpgradeStrategyNoOp, &specsDiffer, nil
}
