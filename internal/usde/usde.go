package usde

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// ResourceNeedsUpdating calculates the upgrade strategy to use during the
// resource reconciliation process based on configuration and user preference (see design doc for details).
// It returns whether an update is needed and the strategy to use
func ResourceNeedsUpdating(ctx context.Context, newDef, existingDef *unstructured.Unstructured) (bool, apiv1.UpgradeStrategy, error) {
	numaLogger := logger.FromContext(ctx)

	metadataNeedsUpdating, metadataUpgradeStrategy, err := resourceMetadataNeedsUpdating(ctx, newDef, existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	specNeedsUpdating, specUpgradeStrategy, err := resourceSpecNeedsUpdating(ctx, newDef, existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"metadataUpgradeStrategy", metadataUpgradeStrategy,
		"specUpgradeStrategy", specUpgradeStrategy,
	).Debug("upgrade strategies")

	if !metadataNeedsUpdating && !specNeedsUpdating {
		return false, apiv1.UpgradeStrategyNoOp, nil
	}

	return true, getMostConservativeStrategy([]apiv1.UpgradeStrategy{metadataUpgradeStrategy, specUpgradeStrategy}), nil

}

func resourceSpecNeedsUpdating(ctx context.Context, newDef, existingDef *unstructured.Unstructured) (bool, apiv1.UpgradeStrategy, error) {

	numaLogger := logger.FromContext(ctx)

	// Get USDE Config
	usdeConfig := config.GetConfigManagerInstance().GetUSDEConfig()

	// Get data loss fields config based on the spec type (Pipeline, ISBS)
	dataLossFields := []config.SpecDataLossField{}
	if reflect.DeepEqual(newDef.GroupVersionKind(), numaflowv1.PipelineGroupVersionKind) {
		dataLossFields = usdeConfig.PipelineSpecDataLossFields
	} else if reflect.DeepEqual(newDef.GroupVersionKind(), numaflowv1.ISBGroupVersionKind) {
		dataLossFields = usdeConfig.ISBServiceSpecDataLossFields
	} else if reflect.DeepEqual(newDef.GroupVersionKind(), apiv1.NumaflowControllerGroupVersionKind) {
		// TODO: for NumaflowController updates do we need to figure out which strategy to use based on the type of changes similarly done for Pipeline and ISBSvc?
		// OR should we always return the user's preferred strategy?
		// For now, make the entire spec a data loss field and include all its subfields
		dataLossFields = []config.SpecDataLossField{
			{
				Path:             "spec",
				IncludeSubfields: true,
			},
		}
	}

	upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newDef.GetNamespace())
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"usdeConfig", usdeConfig,
		"dataLossFields", dataLossFields,
		"upgradeStrategy", upgradeStrategy,
		"newDefUnstr", newDef,
		"existingDefUnstr", existingDef,
	).Debug("started deriving upgrade strategy")

	// Loop through all the data loss fields from config to see if any changes based on those fields require a data loss prevention strategy
	for _, dataLossField := range dataLossFields {
		// newDefField is a map starting with the first field specified in the path
		// newIsMap describes the inner most element(s) described by the path
		newDefField, newIsMap, err := util.ExtractPath(newDef.Object, strings.Split(dataLossField.Path, "."))
		if err != nil {
			return false, apiv1.UpgradeStrategyError, err
		}

		// existingDefField is a map starting with the first field specified in the path
		// existingIsMap describes the inner most element(s) described by the path
		existingDefField, existingIsMap, err := util.ExtractPath(existingDef.Object, strings.Split(dataLossField.Path, "."))
		if err != nil {
			return false, apiv1.UpgradeStrategyError, err
		}

		numaLogger.WithValues(
			"dataLossField", dataLossField,
			"newDefField", newDefField,
			"existingDefField", existingDefField,
			"newIsMap", newIsMap,
			"existingIsMap", existingIsMap,
		).Debug("checking data loss field differences")

		if dataLossField.IncludeSubfields {
			// is the definition (fields + children) at all different?
			if !util.CompareStructNumTypeAgnostic(newDefField, existingDefField) {
				return true, upgradeStrategy, nil
			}
		} else {
			isMap := newIsMap || existingIsMap
			// if it's a map, since we don't care about subfields, we just need to know if it's present in one and not the other
			if isMap {
				if !newIsMap || !existingIsMap { // this means that one of them is nil
					return true, upgradeStrategy, nil
				}
			} else {
				if !util.CompareStructNumTypeAgnostic(newDefField, existingDefField) {
					return true, upgradeStrategy, nil
				}
			}
		}
	}

	numaLogger.Debug("no data loss field changes detected, comparing specs for any Apply-type of changes")

	// If there were no changes in the data loss fields, there could be changes in other fields of the specs.
	// Therefore, check if there are any differences in any field of the specs and return Apply strategy if any.
	if !util.CompareStructNumTypeAgnostic(newDef.Object["spec"], existingDef.Object["spec"]) {
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

func resourceMetadataNeedsUpdating(ctx context.Context, newDef, existingDef *unstructured.Unstructured) (bool, apiv1.UpgradeStrategy, error) {
	numaLogger := logger.FromContext(ctx)

	upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newDef.GetNamespace())
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"new annotations", newDef.GetAnnotations(),
		"existing annotations", existingDef.GetAnnotations(),
		"new labels", newDef.GetLabels(),
		"existing labels", existingDef.GetLabels(),
	).Debug("metadata comparison")

	// First look for Label or Annotation changes that require PPND or Progressive strategy
	// TODO: make this configurable to look for particular Labels and Annotations rather than this specific one
	instanceIDNew := newDef.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]
	instanceIDExisting := existingDef.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]
	if instanceIDNew != instanceIDExisting {
		return true, upgradeStrategy, nil
	}

	// now see if any Labels or Annotations changed at all
	if !checkMapsEqual(newDef.GetLabels(), existingDef.GetLabels()) || !checkMapsEqual(newDef.GetAnnotations(), existingDef.GetAnnotations()) {
		return true, apiv1.UpgradeStrategyApply, nil
	}
	return false, apiv1.UpgradeStrategyNoOp, nil
}

func checkMapsEqual(map1 map[string]string, map2 map[string]string) bool {
	tempMap1 := map1
	if tempMap1 == nil {
		tempMap1 = map[string]string{}
	}
	tempMap2 := map2
	if tempMap2 == nil {
		tempMap2 = map[string]string{}
	}
	return util.CompareStructNumTypeAgnostic(tempMap1, tempMap2)
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
	case config.NoStrategyID, "":
		return apiv1.UpgradeStrategyApply, nil
	default:
		return apiv1.UpgradeStrategyError, fmt.Errorf("invalid Upgrade Strategy: %v", userUpgradeStrategy)
	}
}

func GetUserStrategy(ctx context.Context, namespace string) (config.USDEUserStrategy, error) {
	numaLogger := logger.FromContext(ctx)

	namespaceConfig := config.GetConfigManagerInstance().GetNamespaceConfig(namespace)

	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return config.NoStrategyID, fmt.Errorf("error getting the global config: %v", err)
	}

	var userUpgradeStrategy config.USDEUserStrategy = globalConfig.DefaultUpgradeStrategy
	if userUpgradeStrategy == "" {
		userUpgradeStrategy = config.NoStrategyID
	}
	if namespaceConfig != nil {
		if !namespaceConfig.UpgradeStrategy.IsValid() {
			numaLogger.WithValues("upgrade strategy", namespaceConfig.UpgradeStrategy).Warnf("invalid Upgrade strategy for namespace %s", namespace)
		} else {
			userUpgradeStrategy = namespaceConfig.UpgradeStrategy
		}
	}
	return userUpgradeStrategy, nil
}
