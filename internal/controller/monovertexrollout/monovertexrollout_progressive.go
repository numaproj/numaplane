package monovertexrollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" monovertex
// This implements a function of the progressiveController interface
func (r *MonoVertexRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	metadata, err := getBaseMonoVertexMetadata(monoVertexRollout)
	if err != nil {
		return nil, err
	}
	monoVertex, err := r.makeMonoVertexDefinition(monoVertexRollout, name, metadata)
	if err != nil {
		return nil, err
	}

	labels := monoVertex.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)
	monoVertex.SetLabels(labels)

	return monoVertex, nil
}

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// This implements a function of the progressiveController interface
func (r *MonoVertexRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	return progressive.AssessUpgradingPipelineType(ctx, existingUpgradingChildDef)
}

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted monovertex.
It performs the following pre-upgrade operations:
- it ensures that the promoted monovertex is scaled down before proceeding with a progressive upgrade.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPreUpgrade").WithName("MonoVertexRollout")

	numaLogger.Debug("started pre-upgrade processing of promoted monovertex")
	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted monovertex pre-upgrade", rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// scaleDownMonoVertex either updates the promoted monovertex to scale down the pods or
	// retrieves the currently running pods to update the PromotedMonoVertexStatus scaleValues.
	// This serves to make sure that the pods have been really scaled down before proceeding
	// with the progressive upgrade.
	requeue, err := scaleDownPromotedMonoVertex(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted monovertex")

	return requeue, nil
}

func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPostFailure").WithName("MonoVertexRollout")

	numaLogger.Debug("started post-failure processing of upgrading monovertex")

	min := int64(0)
	max := int64(0)
	err := scaleMonoVertex(ctx, upgradingChildDef, &min, &max, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-failure processing of upgrading monovertex")

	return false, nil
}

func (r *MonoVertexRolloutReconciler) ProcessUpgradingChildPreForcedPromotion(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) error {

	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process upgrading monovertex post-success", rolloutObject)
	}
	var monovertexSpec map[string]interface{}
	if err := util.StructToStruct(monoVertexRollout.Spec.MonoVertex.Spec, &monovertexSpec); err != nil {
		return err
	}

	minPtr, maxPtr, err := getScaleValuesFromMonoVertexSpec(monovertexSpec)
	if err != nil {
		return err
	}

	err = scaleMonoVertex(ctx, upgradingChildDef, minPtr, maxPtr, c)
	if err != nil {
		return err
	}
	return nil
}

func getScaleValuesFromMonoVertexSpec(monovertexSpec map[string]interface{}) (*int64, *int64, error) {
	min, foundMin, err := unstructured.NestedInt64(monovertexSpec, "scale", "min")
	if err != nil {
		return nil, nil, err
	}
	max, foundMax, err := unstructured.NestedInt64(monovertexSpec, "scale", "max")
	if err != nil {
		return nil, nil, err
	}
	var minPtr, maxPtr *int64
	if foundMin {
		minPtr = &min
	}
	if foundMax {
		maxPtr = &max
	}
	return minPtr, maxPtr, nil
}

/*
ProcessPromotedChildPostFailure andles the post-upgrade processing of the promoted monovertex after the "upgrading" child has failed.
It performs the following post-upgrade operations:
- it restores the promoted monovertex scale values to the original values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("MonoVertexRollout")

	numaLogger.Debug("started post-failure processing of promoted monovertex")

	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted monovertex post-upgrade", rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	requeue, err := scalePromotedMonoVertexToOriginalValues(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return requeue, nil
}

/*
scaleDownPromotedMonoVertex scales down the pods of a monovertex to half of the current count if not already scaled down.
It checks if the monovertex was already scaled down and skips the operation if true.
The function updates the scale values in the rollout status and adjusts the scale configuration
of the promoted child definition. It ensures that the scale.min does not exceed the new scale.max.
Returns a boolean indicating if scaling was performed and an error if any operation fails.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedMVStatus: the status of the promoted child in the rollout.
- promotedChildDef: the unstructured object representing the promoted child definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error, or if the pods count has changed, or if the monovertex has not been scaled down yet.
- error: an error if any operation fails during the scaling process.
*/
func scaleDownPromotedMonoVertex(
	ctx context.Context,
	promotedMVStatus *apiv1.PromotedMonoVertexStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scaleDownMonoVertex")

	// If the monovertex has been scaled down already, do not perform scaling down operations
	if promotedMVStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()) {
		return false, nil
	}

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if promotedMVStatus.ScaleValues != nil {
		scaleValuesMap = promotedMVStatus.ScaleValues
	}

	podsList, err := kubernetes.ListPodsMetadataOnly(ctx, c, promotedChildDef.GetNamespace(), fmt.Sprintf(
		"%s=%s, %s=%s",
		common.LabelKeyNumaflowPodMonoVertexName, promotedChildDef.GetName(),
		// the vertex name for a monovertex is the same as the monovertex name
		common.LabelKeyNumaflowPodMonoVertexVertexName, promotedChildDef.GetName(),
	))
	if err != nil {
		return true, err
	}

	actualPodsCount := int64(len(podsList.Items))

	// If for the vertex we already set a Scaled scale value, we only need to update the actual pods count
	// to later verify that the pods were actually scaled down.
	// We want to skip scaling down again.
	if vertexScaleValues, exist := scaleValuesMap[promotedChildDef.GetName()]; exist {
		vertexScaleValues.Actual = actualPodsCount
		scaleValuesMap[promotedChildDef.GetName()] = vertexScaleValues

		promotedMVStatus.ScaleValues = scaleValuesMap
		promotedMVStatus.MarkAllSourceVerticesScaledDown()

		numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debug("updated scaleValues map with running pods count, skipping scaling down since it has already been done")
		return true, nil
	}

	originalScaleDef, foundScale, err := unstructured.NestedMap(promotedChildDef.Object, "spec", "scale")
	if err != nil {
		return true, err
	}

	var originalScaleDefAsString *string
	if foundScale {
		jsonBytes, err := json.Marshal(originalScaleDef)
		if err != nil {
			return true, err
		}

		jsonString := string(jsonBytes)
		originalScaleDefAsString = &jsonString
	}

	newMin, newMax, err := progressive.CalculateScaleMinMaxValues(promotedChildDef.Object, int(actualPodsCount), []string{"spec", "scale", "min"})
	if err != nil {
		return true, fmt.Errorf("cannot calculate the scale min and max values: %+w", err)
	}

	numaLogger.WithValues(
		"promotedChildName", promotedChildDef.GetName(),
		"actualPodsCount", actualPodsCount,
		"newMin", newMin,
		"newMax", newMax,
		"originalScaleDefAsString", originalScaleDefAsString,
	).Debugf("found %d pod(s) for the monovertex, scaling down to %d", actualPodsCount, newMax)

	scaleValuesMap[promotedChildDef.GetName()] = apiv1.ScaleValues{
		OriginalScaleDefinition: originalScaleDefAsString,
		ScaleTo:                 newMax,
		Actual:                  actualPodsCount,
	}

	if err := scaleMonoVertex(ctx, promotedChildDef, &newMin, &newMax, c); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted monovertex to the original scale values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef, "scaleValuesMap", scaleValuesMap).Debug("patched the promoted monovertex with the new scale configuration")

	promotedMVStatus.ScaleValues = scaleValuesMap
	promotedMVStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedMVStatus.ScaleValuesRestoredToOriginal = false

	return !promotedMVStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()), nil
}

/*
scalePromotedMonoVertexToOriginalValues scales a monovertex to its original values based on the rollout status.
This function checks if the monovertex has already been scaled to the original values. If not, it restores the scale values
from the rollout's promoted child status and updates the Kubernetes resource accordingly.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedMVStatus: the status of the promoted child in the rollout, containing scale values.
- promotedChildDef: the unstructured definition of the promoted child resource.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if the monovertex has not been scaled back to original values.
- An error if any issues occur during the scaling process.
*/
func scalePromotedMonoVertexToOriginalValues(
	ctx context.Context,
	promotedMVStatus *apiv1.PromotedMonoVertexStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scaleMonoVertexToOriginalValues")

	// If the monovertex has been scaled back to the original values already, do not restore scaling values again
	if promotedMVStatus.AreScaleValuesRestoredToOriginal(promotedChildDef.GetName()) {
		return false, nil
	}

	if promotedMVStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted monovertex because the rollout does not have promotedChildStatus scaleValues set")
	}

	patchJson := `{"spec": {"scale": null}}`
	if promotedMVStatus.ScaleValues[promotedChildDef.GetName()].OriginalScaleDefinition != nil {
		patchJson = fmt.Sprintf(`{"spec": {"scale": %s}}`, *promotedMVStatus.ScaleValues[promotedChildDef.GetName()].OriginalScaleDefinition)
	}

	if err := kubernetes.PatchResource(ctx, c, promotedChildDef, patchJson, k8stypes.MergePatchType); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted monovertex to original values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("patched the promoted monovertex with the original scale configuration")

	promotedMVStatus.ScaleValuesRestoredToOriginal = true
	promotedMVStatus.AllSourceVerticesScaledDown = false
	promotedMVStatus.ScaleValues = nil

	return false, nil
}

func scaleMonoVertex(
	ctx context.Context,
	monovertex *unstructured.Unstructured,
	min *int64,
	max *int64,
	c client.Client) error {

	scaleValue := "null"
	if min != nil && max != nil {
		scaleValue = fmt.Sprintf(`{"min": %d, "max": %d}`, min, max)
	} else if min != nil {
		scaleValue = fmt.Sprintf(`{"min": %d}`, min)
	} else if max != nil {
		scaleValue = fmt.Sprintf(`{"max": %d}`, max)
	}
	patchJson := fmt.Sprintf(`{"spec": {"scale": %s}}`, scaleValue)
	return kubernetes.PatchResource(ctx, c, monovertex, patchJson, k8stypes.MergePatchType)
}
