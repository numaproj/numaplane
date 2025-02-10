package monovertexrollout

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" monovertex
// This implements a function of the progressiveController interface
func (r *MonoVertexRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error) {
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
  - rolloutPromotedChildStatus: the status of the promoted child, which may be updated during processing.
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPreUpgrade").WithName("MonoVertexRollout")

	numaLogger.Debug("started pre-upgrade processing of promoted monovertex")

	if rolloutPromotedChildStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// scaleDownMonoVertex either updates the promoted monovertex to scale down the pods or
	// retrieves the currently running pods to update the rolloutPromotedChildStatus scaleValues.
	// This serves to make sure that the pods have been really scaled down before proceeding
	// with the progressive upgrade.
	performedScaling, err := scaleDownMonoVertex(ctx, rolloutPromotedChildStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted monovertex")

	return performedScaling, nil
}

/*
ProcessPromotedChildPostUpgrade handles the post-upgrade processing of a promoted monovertex.
It performs the following post-upgrade operations:
- it restores the promoted monovertex scale values to the desired values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutPromotedChildStatus: the status of the promoted child, which may be updated during processing.
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *MonoVertexRolloutReconciler) ProcessPromotedChildPostUpgrade(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("MonoVertexRollout")

	numaLogger.Debug("started post-upgrade processing of promoted monovertex")

	if rolloutPromotedChildStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	performedScaling, err := scaleMonoVertexToDesiredValues(ctx, rolloutPromotedChildStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-upgrade processing of promoted monovertex")

	return performedScaling, nil
}

/*
scaleDownMonoVertex scales down the pods of a monovertex to half of the current count if not already scaled down.
It checks if the monovertex was already scaled down and skips the operation if true.
The function updates the scale values in the rollout status and adjusts the scale configuration
of the promoted child definition. It ensures that the scale.min does not exceed the new scale.max.
Returns a boolean indicating if scaling was performed and an error if any operation fails.

Parameters:
- ctx: the context for managing request-scoped values.
- rolloutPromotedChildStatus: the status of the promoted child in the rollout.
- promotedChildDef: the unstructured object representing the promoted child definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if scaling down was performed, false otherwise.
- error: an error if any operation fails during the scaling process.
*/
func scaleDownMonoVertex(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// If the monovertex has been scaled down already, do not perform scaling down operations
	if rolloutPromotedChildStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()) {
		// Return that scaling down was NOT performed
		return false, nil
	}

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if rolloutPromotedChildStatus.ScaleValues != nil {
		scaleValuesMap = rolloutPromotedChildStatus.ScaleValues
	}

	pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedChildDef.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s, %s=%s",
			common.LabelKeyNumaflowPodMonoVertexName, promotedChildDef.GetName(),
			// the vertex name for a monovertex is the same as the monovertex name
			common.LabelKeyNumaflowPodMonoVertexVertexName, promotedChildDef.GetName(),
		),
	})
	if err != nil {
		return false, err
	}

	actualPodsCount := int64(len(pods.Items))

	// If for the vertex we already set a Scaled scale value, we only need to update the actual pods count
	// to later verify that the pods were actually scaled down.
	// We want to skip scaling down again.
	if vertexScaleValues, exist := scaleValuesMap[promotedChildDef.GetName()]; exist && vertexScaleValues.ScaleTo != 0 {
		vertexScaleValues.Actual = actualPodsCount
		scaleValuesMap[promotedChildDef.GetName()] = vertexScaleValues

		rolloutPromotedChildStatus.ScaleValues = scaleValuesMap

		numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debug("updated scaleValues map with running pods count, skipping scaling down since it has already been done")
		return false, nil
	}

	scaleValue := int64(math.Floor(float64(actualPodsCount) / float64(2)))

	originalMax, _, err := unstructured.NestedInt64(promotedChildDef.Object, "spec", "scale", "max")
	if err != nil {
		return false, err
	}

	numaLogger.WithValues("promotedChildName", promotedChildDef.GetName()).Debugf("found %d pod(s) for the monovertex, scaling down to %d", len(pods.Items), scaleValue)

	// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
	originalMin, found, err := unstructured.NestedInt64(promotedChildDef.Object, "spec", "scale", "min")
	if err != nil {
		return false, err
	}
	newMin := originalMin
	if found && originalMin > scaleValue {
		newMin = scaleValue
	}

	scaleValuesMap[promotedChildDef.GetName()] = apiv1.ScaleValues{
		DesiredMin: originalMin,
		DesiredMax: originalMax,
		ScaleTo:    scaleValue,
		Actual:     actualPodsCount,
	}

	patchJson := fmt.Sprintf(`{"spec": {"scale": {"min": %d, "max": %d}}}`, newMin, scaleValue)
	if err := kubernetes.PatchResource(ctx, c, promotedChildDef, patchJson, k8stypes.MergePatchType); err != nil {
		return false, fmt.Errorf("error scaling the existing promoted monovertex to desired values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef, "scaleValuesMap", scaleValuesMap).Debug("patched the promoted monovertex with the new scale configuration")

	rolloutPromotedChildStatus.ScaleValues = scaleValuesMap
	rolloutPromotedChildStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToDesired to false in case previously set to true and now scaling back down to recover from a previous failure
	rolloutPromotedChildStatus.ScaleValuesRestoredToDesired = false

	return true, nil
}

/*
scaleMonoVertexToDesiredValues scales a monovertex to its desired values based on the rollout status.
This function checks if the monovertex has already been scaled to the desired values. If not, it restores the scale values
from the rollout's promoted child status and updates the Kubernetes resource accordingly.

Parameters:
- ctx: the context for managing request-scoped values.
- rolloutPromotedChildStatus: the status of the promoted child in the rollout, containing scale values.
- promotedChildDef: the unstructured definition of the promoted child resource.
- c: the Kubernetes client for resource operations.

Returns:
- A boolean indicating whether scaling to desired values was performed.
- An error if any issues occur during the scaling process.
*/
func scaleMonoVertexToDesiredValues(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// If the monovertex has been scaled back to desired values already, do not restore scaling values again
	if rolloutPromotedChildStatus.AreScaleValuesRestoredToDesired(promotedChildDef.GetName()) {
		// Return that scaling to desired values was NOT performed
		return false, nil
	}

	if rolloutPromotedChildStatus.ScaleValues == nil {
		return false, errors.New("unable to restore scale values for the promoted monovertex because the rollout does not have promotedChildStatus scaleValues set")
	}

	if _, exists := rolloutPromotedChildStatus.ScaleValues[promotedChildDef.GetName()]; !exists {
		return false, fmt.Errorf("the scale values for the monovertex '%s' are not present in the rollout promotedChildStatus", promotedChildDef.GetName())
	}

	patchJson := fmt.Sprintf(
		`{"spec": {"scale": {"min": %d, "max": %d}}}`,
		rolloutPromotedChildStatus.ScaleValues[promotedChildDef.GetName()].DesiredMin,
		rolloutPromotedChildStatus.ScaleValues[promotedChildDef.GetName()].DesiredMax,
	)

	if err := kubernetes.PatchResource(ctx, c, promotedChildDef, patchJson, k8stypes.MergePatchType); err != nil {
		return false, fmt.Errorf("error scaling the existing promoted monovertex to desired values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("patched the promoted monovertex with the desired scale configuration")

	rolloutPromotedChildStatus.ScaleValuesRestoredToDesired = true
	rolloutPromotedChildStatus.AllSourceVerticesScaledDown = false
	rolloutPromotedChildStatus.ScaleValues = nil

	return true, nil
}
