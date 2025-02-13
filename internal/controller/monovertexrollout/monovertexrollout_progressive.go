package monovertexrollout

import (
	"context"
	"errors"
	"fmt"

	"github.com/numaproj/numaplane/internal/controller/progressive"
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
// Assessment:
// Success: phase must be "Running" and all conditions must be True
// Failure: phase is "Failed" or any condition is False
// Unknown: neither of the above if met
// This implements a function of the progressiveController interface
// TODO: fix this assessment not to return an immediate result as soon as things are healthy or unhealthy
func (r *MonoVertexRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	numaLogger := logger.FromContext(ctx)
	upgradingObjectStatus, err := kubernetes.ParseStatus(existingUpgradingChildDef)
	if err != nil {
		return apiv1.AssessmentResultUnknown, err
	}

	numaLogger.
		WithValues("namespace", existingUpgradingChildDef.GetNamespace(), "name", existingUpgradingChildDef.GetName()).
		Debugf("Upgrading child is in phase %s", upgradingObjectStatus.Phase)

	if upgradingObjectStatus.Phase == "Running" && progressive.IsNumaflowChildReady(&upgradingObjectStatus) {
		return apiv1.AssessmentResultSuccess, nil
	}

	if upgradingObjectStatus.Phase == "Failed" || !progressive.IsNumaflowChildReady(&upgradingObjectStatus) {
		return apiv1.AssessmentResultFailure, nil
	}

	return apiv1.AssessmentResultUnknown, nil
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
	requeue, err := scaleDownMonoVertex(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted monovertex")

	return requeue, nil
}

/*
ProcessPromotedChildPostUpgrade handles the post-upgrade processing of a promoted monovertex.
It performs the following post-upgrade operations:
- it restores the promoted monovertex scale values to the desired values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutObject: the MonoVertexRollout instance
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPostUpgrade(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("MonoVertexRollout")

	numaLogger.Debug("started post-upgrade processing of promoted monovertex")

	monoVertexRollout, ok := rolloutObject.(*apiv1.MonoVertexRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted monovertex post-upgrade", rolloutObject)
	}

	if monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	requeue, err := scaleMonoVertexToDesiredValues(ctx, monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return requeue, nil
}

/*
scaleDownMonoVertex scales down the pods of a monovertex to half of the current count if not already scaled down.
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
func scaleDownMonoVertex(
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
	if vertexScaleValues, exist := scaleValuesMap[promotedChildDef.GetName()]; exist && vertexScaleValues.ScaleTo != 0 {
		vertexScaleValues.Actual = actualPodsCount
		scaleValuesMap[promotedChildDef.GetName()] = vertexScaleValues

		promotedMVStatus.ScaleValues = scaleValuesMap
		promotedMVStatus.MarkAllSourceVerticesScaledDown()

		numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debug("updated scaleValues map with running pods count, skipping scaling down since it has already been done")
		return true, nil
	}

	_, foundDesiredScaleField, err := unstructured.NestedMap(promotedChildDef.Object, "spec", "scale")
	if err != nil {
		return true, err
	}

	newMin, newMax, originalMin, originalMax, err := progressive.CalculateScaleMinMaxValues(promotedChildDef.Object, int(actualPodsCount), []string{"spec", "scale", "min"}, []string{"spec", "scale", "max"})
	if err != nil {
		return true, fmt.Errorf("cannot calculate the scale min and max values: %+w", err)
	}

	numaLogger.WithValues(
		"promotedChildName", promotedChildDef.GetName(),
		"actualPodsCount", actualPodsCount,
		"newMin", newMin,
		"newMax", newMax,
		"originalMin", originalMin,
		"originalMax", originalMax,
	).Debugf("found %d pod(s) for the monovertex, scaling down to %d", actualPodsCount, newMax)

	scaleValuesMap[promotedChildDef.GetName()] = apiv1.ScaleValues{
		IsDesiredScaleSet: foundDesiredScaleField,
		DesiredMin:        originalMin,
		DesiredMax:        originalMax,
		ScaleTo:           newMax,
		Actual:            actualPodsCount,
	}

	patchJson := fmt.Sprintf(`{"spec": {"scale": {"min": %d, "max": %d}}}`, newMin, newMax)
	if err := kubernetes.PatchResource(ctx, c, promotedChildDef, patchJson, k8stypes.MergePatchType); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted monovertex to desired values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef, "scaleValuesMap", scaleValuesMap).Debug("patched the promoted monovertex with the new scale configuration")

	promotedMVStatus.ScaleValues = scaleValuesMap
	promotedMVStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToDesired to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedMVStatus.ScaleValuesRestoredToDesired = false

	return !promotedMVStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()), nil
}

/*
scaleMonoVertexToDesiredValues scales a monovertex to its desired values based on the rollout status.
This function checks if the monovertex has already been scaled to the desired values. If not, it restores the scale values
from the rollout's promoted child status and updates the Kubernetes resource accordingly.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedMVStatus: the status of the promoted child in the rollout, containing scale values.
- promotedChildDef: the unstructured definition of the promoted child resource.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if the monovertex has not been scaled back to desired values.
- An error if any issues occur during the scaling process.
*/
func scaleMonoVertexToDesiredValues(
	ctx context.Context,
	promotedMVStatus *apiv1.PromotedMonoVertexStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scaleMonoVertexToDesiredValues")

	// If the monovertex has been scaled back to desired values already, do not restore scaling values again
	if promotedMVStatus.AreScaleValuesRestoredToDesired(promotedChildDef.GetName()) {
		return false, nil
	}

	if promotedMVStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted monovertex because the rollout does not have promotedChildStatus scaleValues set")
	}

	vertexScaleValues, exists := promotedMVStatus.ScaleValues[promotedChildDef.GetName()]
	if !exists {
		return true, fmt.Errorf("the scale values for the monovertex '%s' are not present in the rollout promotedChildStatus", promotedChildDef.GetName())
	}

	patchJson := `{"spec": {"scale": null}}`
	if vertexScaleValues.IsDesiredScaleSet {
		var minVal any = "null"
		var maxVal any = "null"

		if promotedMVStatus.ScaleValues[promotedChildDef.GetName()].DesiredMin != nil {
			minVal = *promotedMVStatus.ScaleValues[promotedChildDef.GetName()].DesiredMin
		}

		if promotedMVStatus.ScaleValues[promotedChildDef.GetName()].DesiredMax != nil {
			maxVal = *promotedMVStatus.ScaleValues[promotedChildDef.GetName()].DesiredMax
		}

		patchJson = fmt.Sprintf(`{"spec": {"scale": {"min": %v, "max": %v}}}`, minVal, maxVal)
	}

	if err := kubernetes.PatchResource(ctx, c, promotedChildDef, patchJson, k8stypes.MergePatchType); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted monovertex to desired values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("patched the promoted monovertex with the desired scale configuration")

	promotedMVStatus.ScaleValuesRestoredToDesired = true
	promotedMVStatus.AllSourceVerticesScaledDown = false
	promotedMVStatus.ScaleValues = nil

	return false, nil
}
