package usde

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// ResourceNeedsUpdatingUnstructured calculates the upgrade strategy to use during the resource reconciliation process based on configuration and user preference.
// TODO: This is a temporary function which will be removed once all the controller are migrated to use Unstructured Object
func ResourceNeedsUpdatingUnstructured(ctx context.Context, newDef, existingDef *unstructured.Unstructured) (bool, apiv1.UpgradeStrategy, error) {
	newDefObject, err := kubernetes.UnstructuredToObject(newDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}
	existingDefObject, err := kubernetes.UnstructuredToObject(existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}
	return ResourceNeedsUpdating(ctx, newDefObject, existingDefObject)
}

// ResourceNeedsUpdating calculates the upgrade strategy to use during the
// resource reconciliation process based on configuration and user preference (see design doc for details).
// It returns whether an update is needed and the strategy to use
func ResourceNeedsUpdating(ctx context.Context, newDef *kubernetes.GenericObject, existingDef *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {

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

func resourceSpecNeedsUpdating(ctx context.Context, newDef *kubernetes.GenericObject, existingDef *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {

	numaLogger := logger.FromContext(ctx)

	// Get USDE Config
	usdeConfig := config.GetConfigManagerInstance().GetUSDEConfig()

	// Get data loss fields config based on the spec type (Pipeline, ISBS)
	dataLossFields := []config.SpecDataLossField{}
	if reflect.DeepEqual(newDef.GroupVersionKind(), numaflowv1.PipelineGroupVersionKind) {
		dataLossFields = usdeConfig.PipelineSpecDataLossFields
	} else if reflect.DeepEqual(newDef.GroupVersionKind(), numaflowv1.ISBGroupVersionKind) {
		dataLossFields = usdeConfig.ISBServiceSpecDataLossFields
	}

	newDefUnstr, err := kubernetes.ObjectToUnstructured(newDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	existingDefUnstr, err := kubernetes.ObjectToUnstructured(existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newDef.Namespace)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"usdeConfig", usdeConfig,
		"dataLossFields", dataLossFields,
		"upgradeStrategy", upgradeStrategy,
		"newDefUnstr", newDefUnstr,
		"existingDefUnstr", existingDefUnstr,
	).Debug("started deriving upgrade strategy")

	// Loop through all the data loss fields from config to see if any changes based on those fields require a data loss prevention strategy
	for _, dataLossField := range dataLossFields {
		newDefField, newDefFieldFound, err := unstructured.NestedFieldNoCopy(newDefUnstr.Object, strings.Split(dataLossField.Path, ".")...)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, err
		}

		existingDefField, existingDefFieldFound, err := unstructured.NestedFieldNoCopy(existingDefUnstr.Object, strings.Split(dataLossField.Path, ".")...)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, err
		}

		numaLogger.WithValues(
			"dataLossField", dataLossField,
			"newDefField", newDefField,
			"newDefFieldFound", newDefFieldFound,
			"existingDefField", existingDefField,
			"existingDefFieldFound", existingDefFieldFound,
		).Debug("checking data loss field differences")

		// If both specs (new and existing) have the data loss field, compare them based on their config IncludeSubfields and based on if they are a map or a primitive
		if newDefFieldFound && existingDefFieldFound {
			// Only check the type of the existing spec field (no need to also check the new spec)
			_, isExistingDefFieldMap := existingDefField.(map[any]any)

			numaLogger.WithValues(
				"dataLossField", dataLossField,
				"isExistingDefFieldMap", isExistingDefFieldMap,
			).Debug("new and existing specs have data loss field")

			// If the current field is not a map or if it is (assumed from the config if IncludeSubfields is true) and the config
			// says to include comparing the subfields, then compare the fields/maps and, if the fields/maps are different,
			// a data loss prevention strategy is needed
			if (dataLossField.IncludeSubfields || !isExistingDefFieldMap) && !reflect.DeepEqual(newDefField, existingDefField) {
				return true, upgradeStrategy, nil
			}
		} else if !newDefFieldFound && !existingDefFieldFound {
			// Nothing to do since the field is missing from both new and existing specs
			continue
		} else {
			// The specs are different since one has the field while the other does not. Therefore, a data loss prevention strategy is needed
			return true, upgradeStrategy, nil
		}
	}

	numaLogger.Debug("no data loss field changes detected, comparing specs for any Apply-type of changes")

	// If there were no changes in the data loss fields, there could be changes in other fields of the specs.
	// Therefore, check if there are any differences in any field of the specs and return Apply strategy if any.
	if !reflect.DeepEqual(newDefUnstr, existingDefUnstr) {
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

func resourceMetadataNeedsUpdating(ctx context.Context, newDef *kubernetes.GenericObject, existingDef *kubernetes.GenericObject) (bool, apiv1.UpgradeStrategy, error) {
	numaLogger := logger.FromContext(ctx)

	upgradeStrategy, err := getDataLossUpggradeStrategy(ctx, newDef.Namespace)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, err
	}

	numaLogger.WithValues(
		"new annotations", newDef.Annotations,
		"existing annotations", existingDef.Annotations,
		"new labels", newDef.Labels,
		"existing labels", existingDef.Labels,
	).Debug("metadata comparison")

	// First look for Label or Annotation changes that require PPND or Progressive strategy
	// TODO: make this configurable to look for particular Labels and Annotations rather than this specific one
	instanceIDNew := newDef.Annotations[common.AnnotationKeyNumaflowInstanceID]
	instanceIDExisting := existingDef.Annotations[common.AnnotationKeyNumaflowInstanceID]
	if instanceIDNew != instanceIDExisting {
		return true, upgradeStrategy, nil
	}

	// now see if any Labels or Annotations changed at all
	if !checkMapsEqual(newDef.Labels, existingDef.Labels) || !checkMapsEqual(newDef.Annotations, existingDef.Annotations) {
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
	return reflect.DeepEqual(tempMap1, tempMap2)
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

	var userUpgradeStrategy config.USDEUserStrategy = config.GetConfigManagerInstance().GetUSDEConfig().DefaultUpgradeStrategy
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
