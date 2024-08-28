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

// TODO: add debug and info statements throughout the new USDE code

type UpgradeStrategy int

const (
	UpgradeStrategyError UpgradeStrategy = iota - 1
	UpgradeStrategyNoOp
	UpgradeStrategyApply
	UpgradeStrategyPPND
	UpgradeStrategyProgressive
)

func (us UpgradeStrategy) String() string {
	switch us {
	case UpgradeStrategyError:
		return "Error"
	case UpgradeStrategyNoOp:
		return "NoOp"
	case UpgradeStrategyApply:
		return "DirectApply"
	case UpgradeStrategyPPND:
		return "PipelinePauseAndDrain"
	case UpgradeStrategyProgressive:
		return "Progressive"
	}

	return "Invalid"
}

func GetUpgradeStrategy(ctx context.Context, newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (UpgradeStrategy, error) {
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

	numaLogger.WithValues("usdeConfig", usdeConfig, "applyPaths", applyPaths).Debug("started calculating upgrade strategy")

	// Split newSpec
	newSpecOnlyApplyPaths, newSpecWithoutApplyPaths, err := util.SplitObject(newSpec.Spec.Raw, applyPaths, ".")
	if err != nil {
		return UpgradeStrategyError, err
	}

	numaLogger.WithValues("newSpecOnlyApplyPaths", newSpecOnlyApplyPaths, "newSpecWithoutApplyPaths", newSpecWithoutApplyPaths).Debug("split new spec")

	// Split existingSpec
	existingSpecOnlyApplyPaths, existingSpecWithoutApplyPaths, err := util.SplitObject(existingSpec.Spec.Raw, applyPaths, ".")
	if err != nil {
		return UpgradeStrategyError, err
	}

	numaLogger.WithValues("existingSpecOnlyApplyPaths", existingSpecOnlyApplyPaths, "existingSpecWithoutApplyPaths", existingSpecWithoutApplyPaths).Debug("split existing spec")

	// Compare specs without the apply fields and check user's strategy to either return PPND or Progressive
	if !reflect.DeepEqual(newSpecWithoutApplyPaths, existingSpecWithoutApplyPaths) {
		userUpgradeStrategy := config.GetConfigManagerInstance().GetNamespaceConfig(newSpec.Namespace).UpgradeStrategy

		numaLogger.WithValues("userUpgradeStrategy", userUpgradeStrategy).Debug("the specs without the 'apply' paths are different")

		if userUpgradeStrategy == config.PPNDStrategyID {
			return UpgradeStrategyPPND, nil
		}

		return UpgradeStrategyProgressive, nil
	}

	// Compare specs with the apply fields
	if !reflect.DeepEqual(newSpecOnlyApplyPaths, existingSpecOnlyApplyPaths) {
		numaLogger.Debug("the specs with only the 'apply' paths are different")
		return UpgradeStrategyApply, nil
	}

	numaLogger.Debug("the specs are equal, no update needed")

	// Return NoOp if no differences were found between the new and existing specs
	return UpgradeStrategyNoOp, nil
}
