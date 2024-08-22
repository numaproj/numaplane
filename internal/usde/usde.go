package usde

import (
	"reflect"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type UpgradeStrategy = int

const (
	UpgradeStrategyError UpgradeStrategy = iota - 1
	UpgradeStrategyNoOp
	UpgradeStrategyApply
	UpgradeStrategyPPND
	UpgradeStrategyProgressive
)

func GetUpgradeStrategy(newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (UpgradeStrategy, error) {
	// Get USDE Config
	usdeConfig := config.GetConfigManagerInstance().GetUSDEConfig()

	// Get apply paths based on the spec type (Pipeline, ISBS)
	applyPaths := []string{}
	if reflect.DeepEqual(newSpec.GroupVersionKind(), numaflowv1.PipelineGroupVersionKind) {
		applyPaths = usdeConfig.PipelineSpecExcludedPaths
	} else if reflect.DeepEqual(newSpec.GroupVersionKind(), numaflowv1.ISBGroupVersionKind) {
		applyPaths = usdeConfig.ISBServiceSpecExcludedPaths
	}

	// Split newSpec
	newSpecOnlyApplyPaths, newSpecWithoutApplyPaths, err := util.SplitObject(newSpec.Spec.Raw, applyPaths, ".")
	if err != nil {
		return UpgradeStrategyError, err
	}

	// Split existingSpec
	existingSpecOnlyApplyPaths, existingSpecWithoutApplyPaths, err := util.SplitObject(existingSpec.Spec.Raw, applyPaths, ".")
	if err != nil {
		return UpgradeStrategyError, err
	}

	// Compare specs without the apply fields and check user's strategy to either return PPND or Progressive
	if !reflect.DeepEqual(newSpecWithoutApplyPaths, existingSpecWithoutApplyPaths) {
		userUpgradeStrategy := config.GetConfigManagerInstance().GetNamespaceConfig(newSpec.Namespace).UpgradeStrategy
		if userUpgradeStrategy == config.PPNDStrategyID {
			return UpgradeStrategyPPND, nil
		}

		return UpgradeStrategyProgressive, nil
	}

	// Compare specs with the apply fields
	if !reflect.DeepEqual(newSpecOnlyApplyPaths, existingSpecOnlyApplyPaths) {
		return UpgradeStrategyApply, nil
	}

	// Return NoOp if no differences were found between the new and existing specs
	return UpgradeStrategyNoOp, nil
}
