package usde

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var (
	strategyRating map[apiv1.UpgradeStrategy]int = map[apiv1.UpgradeStrategy]int{
		apiv1.UpgradeStrategyNoOp:        0,
		apiv1.UpgradeStrategyApply:       1,
		apiv1.UpgradeStrategyPPND:        2,
		apiv1.UpgradeStrategyProgressive: 2,
	}
)

// ResourceNeedsUpdating calculates the upgrade strategy to use during the
// resource reconciliation process based on configuration and user preference (see design doc for details).
// It returns the following parameters:
// - bool: Indicates whether the resource needs an update.
// - apiv1.UpgradeStrategy: The most conservative upgrade strategy to be used for updating the resource.
// - bool: Indicates if the controller managed resources should be recreated (delete-recreate).
// - error: Any error encountered during the function execution.
func ResourceNeedsUpdating(ctx context.Context, newDef, existingDef *unstructured.Unstructured) (bool, apiv1.UpgradeStrategy, bool, error) {
	numaLogger := logger.FromContext(ctx)

	metadataNeedsUpdating, metadataUpgradeStrategy, err := resourceMetadataNeedsUpdating(ctx, newDef, existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, err
	}

	specNeedsUpdating, specUpgradeStrategy, recreate, err := resourceSpecNeedsUpdating(ctx, newDef, existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, err
	}

	numaLogger.WithValues(
		"metadataUpgradeStrategy", metadataUpgradeStrategy,
		"specUpgradeStrategy", specUpgradeStrategy,
	).Debug("upgrade strategies")

	if !metadataNeedsUpdating && !specNeedsUpdating {
		return false, apiv1.UpgradeStrategyNoOp, false, nil
	}

	return true, getMostConservativeStrategy([]apiv1.UpgradeStrategy{metadataUpgradeStrategy, specUpgradeStrategy}), recreate, nil

}

// resourceSpecNeedsUpdating determines if a resource specification needs updating.
// It returns the following parameters:
// - bool: Indicates whether the resource specification needs an update.
// - apiv1.UpgradeStrategy: The strategy to be used for upgrading the resource.
// - bool: Indicates if the controller managed resources should be recreated (delete-recreate).
// - error: Any error encountered during the function execution.
func resourceSpecNeedsUpdating(ctx context.Context, newDef, existingDef *unstructured.Unstructured) (bool, apiv1.UpgradeStrategy, bool, error) {

	numaLogger := logger.FromContext(ctx)

	// Get USDE Config
	usdeConfig := config.GetConfigManagerInstance().GetUSDEConfig()

	// Get data loss fields config based on the spec type (Pipeline, ISBS, etc.)
	usdeConfigMapKey := strings.ToLower(newDef.GetKind())
	recreateFields := usdeConfig[usdeConfigMapKey].Recreate
	dataLossFields := usdeConfig[usdeConfigMapKey].DataLoss
	progressiveFields := usdeConfig[usdeConfigMapKey].Progressive

	upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newDef.GetNamespace())
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, err
	}

	numaLogger.WithValues(
		"usdeConfig", usdeConfig,
		"usdeConfigMapKey", usdeConfigMapKey,
		"recreateFields", recreateFields,
		"dataLossFields", dataLossFields,
		"progressiveFields", progressiveFields,
		"upgradeStrategy", upgradeStrategy,
		"newDefUnstr", newDef,
		"existingDefUnstr", existingDef,
	).Debug("started deriving upgrade strategy")

	switch upgradeStrategy {
	case apiv1.UpgradeStrategyProgressive:
		mergedSpecFieldLists := []config.SpecField{}
		mergedSpecFieldLists = append(mergedSpecFieldLists, recreateFields...)
		mergedSpecFieldLists = append(mergedSpecFieldLists, dataLossFields...)
		mergedSpecFieldLists = append(mergedSpecFieldLists, progressiveFields...)

		specNeedsUpdating, err := checkFieldsList(ctx, mergedSpecFieldLists, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using full USDE Config (strategy '%s'): %w", upgradeStrategy, err)
		}
		if specNeedsUpdating {
			return specNeedsUpdating, upgradeStrategy, false, nil
		}
	case apiv1.UpgradeStrategyPPND:
		// Use the recreate fields list from config
		specNeedsUpdating, err := checkFieldsList(ctx, recreateFields, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using 'recreate' USDE Config (strategy '%s'): %w", upgradeStrategy, err)
		}
		if specNeedsUpdating {
			// Also return "recreate" true to communicate to the controller to recreate the appropriate resources
			return specNeedsUpdating, upgradeStrategy, true, nil
		}

		// Use the dataLoss fields list from config
		specNeedsUpdating, err = checkFieldsList(ctx, dataLossFields, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using 'dataLoss' USDE Config (strategy '%s'): %w", upgradeStrategy, err)
		}
		if specNeedsUpdating {
			return specNeedsUpdating, upgradeStrategy, false, nil
		}
	case apiv1.UpgradeStrategyApply:
		specNeedsUpdating, err := checkFieldsList(ctx, recreateFields, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using 'recreate' USDE Config (strategy '%s'): %w", upgradeStrategy, err)
		}
		if specNeedsUpdating {
			// Also return "recreate" true to communicate to the controller to recreate the appropriate resources
			return specNeedsUpdating, upgradeStrategy, true, nil
		}
	}

	numaLogger.Debug("no USDE Config field changes detected, comparing specs for any DirectApply-type of changes")

	// If there were no changes in the dataLoss, recreate, and progressive fields, there could be changes in other fields of the specs.
	// Therefore, check if there are any differences in any field of the specs and return Apply strategy if any.
	if !util.CompareStructNumTypeAgnostic(newDef.Object["spec"], existingDef.Object["spec"]) {
		return true, apiv1.UpgradeStrategyApply, false, nil
	}

	numaLogger.Debug("the specs are equal, no update needed")

	// Return NoOp if no differences were found between the new and existing specs
	return false, apiv1.UpgradeStrategyNoOp, false, nil
}

func checkFieldsList(ctx context.Context, specFields []config.SpecField, newDef, existingDef *unstructured.Unstructured) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// Loop through all the spec fields from config to see if any changes based on those fields require the specified upgrade strategy
	for _, specField := range specFields {
		// newDefField is a map starting with the first field specified in the path
		// newIsMap describes the inner most element(s) described by the path
		newDefField, newIsMap, err := util.ExtractPath(newDef.Object, strings.Split(specField.Path, "."))
		if err != nil {
			return false, err
		}

		// existingDefField is a map starting with the first field specified in the path
		// existingIsMap describes the inner most element(s) described by the path
		existingDefField, existingIsMap, err := util.ExtractPath(existingDef.Object, strings.Split(specField.Path, "."))
		if err != nil {
			return false, err
		}

		numaLogger.WithValues(
			"specField", specField,
			"newDefField", newDefField,
			"existingDefField", existingDefField,
			"newIsMap", newIsMap,
			"existingIsMap", existingIsMap,
		).Debug("checking spec field differences")

		if specField.IncludeSubfields {
			// is the definition (fields + children) at all different?
			if !util.CompareStructNumTypeAgnostic(newDefField, existingDefField) {
				return true, nil
			}
		} else {
			isMap := newIsMap || existingIsMap
			// if it's a map, since we don't care about subfields, we just need to know if it's present in one and not the other
			if isMap {
				if !newIsMap || !existingIsMap { // this means that one of them is nil
					return true, nil
				}
			} else {
				if !util.CompareStructNumTypeAgnostic(newDefField, existingDefField) {
					return true, nil
				}
			}
		}
	}

	return false, nil
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
