package usde

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
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
// It returns the following values:
// - bool: Indicates whether the resource needs an update.
// - apiv1.UpgradeStrategy: The most conservative upgrade strategy to be used for updating the resource.
// - bool: Indicates if the controller managed resources should be recreated (delete-recreate).
// - unstructured.UnstructuredList: Resources that require addition
// - unstructured.UnstructuredList: Resources that require modification
// - unstructured.UnstructuredList: Resources that require deletion
// - error: Any error encountered during the function execution.
func ResourceNeedsUpdating(
	ctx context.Context,
	newDef, existingDef *unstructured.Unstructured,
	newRiders []riders.Rider,
	existingRiders unstructured.UnstructuredList) (bool, apiv1.UpgradeStrategy, bool, unstructured.UnstructuredList, unstructured.UnstructuredList, unstructured.UnstructuredList, error) {
	numaLogger := logger.FromContext(ctx)

	metadataNeedsUpdating, metadataUpgradeStrategy, err := resourceMetadataNeedsUpdating(ctx, newDef, existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, err
	}

	specNeedsUpdating, specUpgradeStrategy, recreate, err := resourceSpecNeedsUpdating(ctx, newDef, existingDef)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, unstructured.UnstructuredList{}, err
	}

	ridersNeedUpdating, ridersUpgradeStrategy, additionsRequired, modificationsRequired, deletionsRequired, err := RidersNeedUpdating(ctx, existingDef.GetNamespace(), existingDef.GetKind(), existingDef.GetName(), newRiders, existingRiders)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, additionsRequired, modificationsRequired, deletionsRequired, err
	}

	numaLogger.WithValues(
		"metadataUpgradeStrategy", metadataUpgradeStrategy,
		"specUpgradeStrategy", specUpgradeStrategy,
		"ridersUpgradeStrategy", ridersUpgradeStrategy,
	).Debug("upgrade strategies")

	if !metadataNeedsUpdating && !specNeedsUpdating && !ridersNeedUpdating {
		return false, apiv1.UpgradeStrategyNoOp, false, additionsRequired, modificationsRequired, deletionsRequired, nil
	}

	return true, getMostConservativeStrategy([]apiv1.UpgradeStrategy{metadataUpgradeStrategy, specUpgradeStrategy, ridersUpgradeStrategy}), recreate, additionsRequired, modificationsRequired, deletionsRequired, nil

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

	dataLossUpgradeStrategy, err := getDataLossUpgradeStrategy(ctx, newDef.GetNamespace(), existingDef.GetKind())
	if err != nil {
		return false, apiv1.UpgradeStrategyError, false, err
	}

	numaLogger.WithValues(
		"dataLossUpgradeStrategy", dataLossUpgradeStrategy,
		"newDefUnstr", kubernetes.GetLoggableResource(newDef),
		"existingDefUnstr", kubernetes.GetLoggableResource(existingDef),
	).Debug("started deriving upgrade strategy")

	switch dataLossUpgradeStrategy {
	case apiv1.UpgradeStrategyProgressive:
		mergedSpecFieldLists := []config.SpecField{}
		mergedSpecFieldLists = append(mergedSpecFieldLists, recreateFields...)
		mergedSpecFieldLists = append(mergedSpecFieldLists, dataLossFields...)
		mergedSpecFieldLists = append(mergedSpecFieldLists, progressiveFields...)

		specNeedsUpdating, field, err := checkFieldsList(mergedSpecFieldLists, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using full USDE Config (strategy '%s'): %w", dataLossUpgradeStrategy, err)
		}
		if specNeedsUpdating {
			if field != nil {
				numaLogger.WithValues(
					"field", field.Path,
					"resource", existingDef.GetName()).Debug("field resulting in Progressive strategy")
			}
			return specNeedsUpdating, dataLossUpgradeStrategy, false, nil

		}
	case apiv1.UpgradeStrategyPPND:
		// Use the recreate fields list from config
		specNeedsUpdating, field, err := checkFieldsList(recreateFields, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using 'recreate' USDE Config (strategy '%s'): %w", dataLossUpgradeStrategy, err)
		}
		if specNeedsUpdating {

			if field != nil {
				numaLogger.WithValues(
					"field", field.Path,
					"resource", existingDef.GetName()).Debug("recreate field resulting in PPND strategy")
			}

			// Also return "recreate" true to communicate to the controller to recreate the appropriate resources
			return specNeedsUpdating, dataLossUpgradeStrategy, true, nil
		}

		// Use the dataLoss fields list from config
		specNeedsUpdating, field, err = checkFieldsList(dataLossFields, newDef, existingDef)
		if err != nil {
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using 'dataLoss' USDE Config (strategy '%s'): %w", dataLossUpgradeStrategy, err)
		}
		if specNeedsUpdating {
			if field != nil {
				numaLogger.WithValues(
					"field", field.Path,
					"resource", existingDef.GetName()).Debug("data loss field resulting in PPND strategy")
			}
			return specNeedsUpdating, dataLossUpgradeStrategy, false, nil
		}
	case apiv1.UpgradeStrategyApply:
		specNeedsUpdating, field, err := checkFieldsList(recreateFields, newDef, existingDef)
		if err != nil {
			if field != nil {
				numaLogger.WithValues(
					"field", field.Path,
					"resource", existingDef.GetName()).Debug("field resulting in Direct Apply strategy")
			}
			return false, apiv1.UpgradeStrategyError, false, fmt.Errorf("error while checking spec changes using 'recreate' USDE Config (strategy '%s'): %w", dataLossUpgradeStrategy, err)
		}
		if specNeedsUpdating {
			// Also return "recreate" true to communicate to the controller to recreate the appropriate resources
			return specNeedsUpdating, dataLossUpgradeStrategy, true, nil
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

// traverse the fields passed in to see if any are different
// if true, return the first one found
func checkFieldsList(specFields []config.SpecField, newDef, existingDef *unstructured.Unstructured) (bool, *config.SpecField, error) {

	// Loop through all the spec fields from config to see if any changes based on those fields require the specified upgrade strategy
	for _, specField := range specFields {
		// newDefField is a map starting with the first field specified in the path
		// newIsMap describes the inner most element(s) described by the path
		newDefField, newIsMap, err := util.ExtractPath(newDef.Object, strings.Split(specField.Path, "."))
		if err != nil {
			return false, nil, err
		}

		// existingDefField is a map starting with the first field specified in the path
		// existingIsMap describes the inner most element(s) described by the path
		existingDefField, existingIsMap, err := util.ExtractPath(existingDef.Object, strings.Split(specField.Path, "."))
		if err != nil {
			return false, nil, err
		}

		if specField.IncludeSubfields {
			// is the definition (fields + children) at all different?
			if !util.CompareStructNumTypeAgnostic(newDefField, existingDefField) {
				return true, &specField, nil
			}
		} else {
			isMap := newIsMap || existingIsMap
			// if it's a map, since we don't care about subfields, we just need to know if it's present in one and not the other
			if isMap {
				if !newIsMap || !existingIsMap { // this means that one of them is nil
					return true, &specField, nil
				}
			} else {
				if !util.CompareStructNumTypeAgnostic(newDefField, existingDefField) {
					return true, &specField, nil
				}
			}
		}
	}

	return false, nil, nil
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

	upgradeStrategy, err := getDataLossUpgradeStrategy(ctx, newDef.GetNamespace(), existingDef.GetKind())
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
	if ResourceMetadataHasDataLossRisk(ctx, newDef, existingDef) {
		return true, upgradeStrategy, nil
	}

	// We don't need to do PPND or Progressive strategy.
	// Now to determine if we need to do a Direct Apply type of change, we actually don't want to compare the newDef to the existingDef.
	// Because the existingDef may have other Labels and Annotations that have been applied outside of what was defined in the Rollout,
	// we want to ignore those.
	// Therefore, we just check to see if the existingDef contains the annotations and labels that are required (by the newDef)
	requiredAnnotationsPresent := util.IsMapSubset(newDef.GetAnnotations(), existingDef.GetAnnotations())
	requiredLabelsPresent := util.IsMapSubset(newDef.GetLabels(), existingDef.GetLabels())
	if !requiredAnnotationsPresent || !requiredLabelsPresent {
		return true, apiv1.UpgradeStrategyApply, nil
	}
	return false, apiv1.UpgradeStrategyNoOp, nil
}

func ResourceMetadataHasDataLossRisk(ctx context.Context, newDef, existingDef *unstructured.Unstructured) bool {
	instanceIDNew := newDef.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]
	instanceIDExisting := existingDef.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]
	return instanceIDNew != instanceIDExisting
}

// return required upgrade strategy, list of additions, modifications, deletions required
func RidersNeedUpdating(ctx context.Context, namespace string, childKind string, childName string, newRiders []riders.Rider, existingRiders unstructured.UnstructuredList) (bool, apiv1.UpgradeStrategy, unstructured.UnstructuredList, unstructured.UnstructuredList, unstructured.UnstructuredList, error) {

	numaLogger := logger.FromContext(ctx)

	additionsRequired := unstructured.UnstructuredList{}
	deletionsRequired := unstructured.UnstructuredList{}
	modificationsRequired := unstructured.UnstructuredList{}
	upgradeStrategy := apiv1.UpgradeStrategyNoOp

	existingRiderMap := make(map[string]unstructured.Unstructured)
	newRiderMap := make(map[string]unstructured.Unstructured)

	// which upgrade strategy does user prefer for this type of Child Kind? find out if it's Progressive
	// since some Riders, if changed, can invoke a Progressive strategy
	dataLossUpgradeStrategy, err := getDataLossUpgradeStrategy(ctx, namespace, childKind)
	if err != nil {
		return false, apiv1.UpgradeStrategyError, additionsRequired, modificationsRequired, deletionsRequired, err
	}
	userPrefersProgressive := (dataLossUpgradeStrategy == apiv1.UpgradeStrategyProgressive)

	// Create a map of newRiders
	for _, rider := range newRiders {
		gvk := rider.Definition.GroupVersionKind()
		key := gvk.String() + "/" + rider.Definition.GetName()
		newRiderMap[key] = rider.Definition
	}

	// Create a map of existingRiders
	for _, rider := range existingRiders.Items {
		gvk := rider.GroupVersionKind()
		key := gvk.String() + "/" + rider.GetName()
		existingRiderMap[key] = rider
	}

	// Get the additionsRequired
	// Find new riders not in existing
	for _, newRider := range newRiders {
		gvk := newRider.Definition.GroupVersionKind()
		key := gvk.String() + "/" + newRider.Definition.GetName()

		if _, found := existingRiderMap[key]; !found {
			additionsRequired.Items = append(additionsRequired.Items, newRider.Definition)
			upgradeStrategy = apiv1.UpgradeStrategyApply
		}
	}

	// Get the deletionsRequired
	// Find existing riders not in new
	for _, existingRider := range existingRiders.Items {
		gvk := existingRider.GroupVersionKind()
		key := gvk.String() + "/" + existingRider.GetName()

		if _, found := newRiderMap[key]; !found {
			// riders.Rider is in newRiders but not in existingRiders
			deletionsRequired.Items = append(deletionsRequired.Items, existingRider)
			upgradeStrategy = apiv1.UpgradeStrategyApply
		}
	}

	// Get the modificationsRequired
	// Find the resources that are in both newRiders and existingRiders and determine which ones have changed
	for _, newRider := range newRiders {
		gvk := newRider.Definition.GroupVersionKind()
		key := gvk.String() + "/" + newRider.Definition.GetName()

		if existingRider, found := existingRiderMap[key]; found {
			newHash, err := kubernetes.CalculateHash(ctx, newRider.Definition)
			if err != nil {
				return false, apiv1.UpgradeStrategyError, additionsRequired, modificationsRequired, deletionsRequired, fmt.Errorf("faied to calculate hash annotation for rider %s: %s", newRider.Definition.GetName(), err)
			}

			existingHash := riders.GetExistingHashAnnotation(existingRider)
			if newHash != existingHash {

				// return progressive if user prefers progressive and it's required for this resource change
				if newRider.RequiresProgressive && userPrefersProgressive {
					upgradeStrategy = apiv1.UpgradeStrategyProgressive
				} else {
					if upgradeStrategy == apiv1.UpgradeStrategyNoOp {
						upgradeStrategy = apiv1.UpgradeStrategyApply
					}
				}

				newRider.Definition.SetResourceVersion(existingRider.GetResourceVersion())
				modificationsRequired.Items = append(modificationsRequired.Items, newRider.Definition)
			}
		}
	}

	// log additionsRequired, modificationsRequired, deletionsRequired, as well as upgrade strategy
	numaLogger.WithValues(
		"child name", childName,
		"additionsRequired", getNamesAndKinds(additionsRequired),
		"modificationsRequired", getNamesAndKinds(modificationsRequired),
		"deletionsRequired", getNamesAndKinds(deletionsRequired),
	).Debug("rider changes")

	requiresUpdate := upgradeStrategy == apiv1.UpgradeStrategyApply || upgradeStrategy == apiv1.UpgradeStrategyProgressive
	return requiresUpdate, upgradeStrategy, additionsRequired, modificationsRequired, deletionsRequired, nil
}

func getNamesAndKinds(ulist unstructured.UnstructuredList) string {
	namesAndKinds := ""
	for _, u := range ulist.Items {
		namesAndKinds = namesAndKinds + u.GetKind() + "/" + u.GetName() + "; "
	}
	return namesAndKinds
}

// return the upgrade strategy that represents what the user prefers to do when there's a concern for data loss
func getDataLossUpgradeStrategy(ctx context.Context, namespace, resourceKind string) (apiv1.UpgradeStrategy, error) {
	userUpgradeStrategy, err := GetUserStrategy(ctx, namespace, resourceKind)
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

func GetUserStrategy(ctx context.Context, namespace, resourceKind string) (config.USDEUserStrategy, error) {
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

	// TODO: remove when FeatureFlagDisallowProgressiveForNonMonoVertex no longer needed
	if userUpgradeStrategy == config.ProgressiveStrategyID &&
		resourceKind != numaflowv1.MonoVertexGroupVersionKind.Kind &&
		globalConfig.FeatureFlagDisallowProgressiveForNonMonoVertex {

		// Use the next most conservative strategy: PPND
		userUpgradeStrategy = config.PPNDStrategyID
	}

	return userUpgradeStrategy, nil
}
