package usde

import (
	"fmt"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type UpgradeStrategy = int

const (
	UpgradeStrategyNoOp UpgradeStrategy = iota
	UpgradeStrategyApply
	UpgradeStrategyPPND
	UpgradeStrategyProgressive
)

func GetUpgradeStrategy(newSpec *kubernetes.GenericObject, existingSpec *kubernetes.GenericObject) (UpgradeStrategy, error) {
	// TTODO:
	// 0. get usde config from config module
	// 1. separate each spec from args into 2 specs: one with apply fields and one without (total of 4 objects)
	// 2. compare specs without the apply fields
	// 		2a. if there are no differences:
	// 				2a1. compare the specs with the apply fields
	// 						- if no differences, return NoOp
	//						- if differences, return Apply strategy
	// 		2b. if there are differences,
	//				2b1. look at user preferred strategy
	// 						- if PPND, return ppnd
	//						- otherwise, return progressive

	// Get USDE Config
	usdeConfig := config.GetConfigManagerInstance().GetUSDEConfig()

	// Get apply paths based on the spec type (Pipeline, ISBS)
	applyPaths := []string{}
	if newSpec.GroupVersionKind() == apiv1.PipelineRolloutGroupVersionKind {
		applyPaths = usdeConfig.PipelineSpecExcludedPaths
	} else if newSpec.GroupVersionKind() == apiv1.ISBServiceRolloutGroupVersionKind {
		applyPaths = usdeConfig.ISBServiceSpecExcludedPaths
	}

	fmt.Println(applyPaths)

	return 0, nil
}
