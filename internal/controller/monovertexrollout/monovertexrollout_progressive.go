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

// TTODO
func (r *MonoVertexRolloutReconciler) PreUpgradePromotedChildProcessing(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	// TTODO: put the scaling down logic in a function called `scaleDownPromotedChild` that this function calls

	// TTODO: keep?
	// PreUpgradePromotedChildProcessing either updates the existingPromotedChild to scale down the source vertices pods or
	// retrieves the currently running pods to update the scaleValuesMap used on the rollout status.
	// This serves to make sure that the pods for each vertex have been really scaled down before proceeding with the progressive update.

	numaLogger := logger.FromContext(ctx).WithName("ScaleDownPromotedChildSourceVertices").WithName("MonoVertexRollout")

	numaLogger.Debug("started promoted child source vertex scaling down process")

	// If all source vertices have been scaled down already, there is no need to perform scaling down operations again.
	// Also, no need to requeue since this confirms the running pods are scaled down has needed.
	if liveRolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()) {
		numaLogger.Debug("monovertex already scaled down, exiting scaling down process")
		return false, nil
	}

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus != nil && promotedChildStatus.ScaleValues != nil {
		scaleValuesMap = promotedChildStatus.ScaleValues
	}

	pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedChildDef.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s, %s=%s",
			common.LabelKeyNumaflowPodMonoVertexName, promotedChildDef.GetName(),
			// the vertex name for a monovertex is the same as the monovertex name
			common.LabelKeyNumaflowPodMonoVertexVertexName, promotedChildDef.GetName(),
		),
	})
	if err != nil {
		return true, err
	}

	actualPodsCount := int64(len(pods.Items))

	// If for the vertex we already set a Scaled scale value, we only need to update the actual pods count
	// to later verify that the pods were actually scaled down.
	// We want to skip scaling down again.
	if vertexScaleValues, exist := scaleValuesMap[promotedChildDef.GetName()]; exist && vertexScaleValues.ScaleTo != 0 {
		vertexScaleValues.Actual = actualPodsCount
		scaleValuesMap[promotedChildDef.GetName()] = vertexScaleValues

		numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debug("updated scaleValues map with running pods count, skipping scaling down since it has already been done")
		return true, nil
	}

	scaleValue := int64(math.Floor(float64(actualPodsCount) / float64(2)))

	originalMax, _, err := unstructured.NestedInt64(promotedChildDef.Object, "spec", "scale", "max")
	if err != nil {
		return true, err
	}

	numaLogger.WithValues("promotedChildName", promotedChildDef.GetName()).Debugf("found %d pod(s) for the source vertex, scaling down to %d", len(pods.Items), scaleValue)

	if err := unstructured.SetNestedField(promotedChildDef.Object, scaleValue, "spec", "scale", "max"); err != nil {
		return true, err
	}

	// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
	originalMin, found, err := unstructured.NestedInt64(promotedChildDef.Object, "spec", "scale", "min")
	if err != nil {
		return true, err
	}
	if found && originalMin > scaleValue {
		if err := unstructured.SetNestedField(promotedChildDef.Object, scaleValue, "spec", "scale", "min"); err != nil {
			return true, err
		}
	}

	scaleValuesMap[promotedChildDef.GetName()] = apiv1.ScaleValues{
		DesiredMin: originalMin,
		DesiredMax: originalMax,
		ScaleTo:    scaleValue,
		Actual:     actualPodsCount,
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef, "scaleValuesMap", scaleValuesMap).Debug("applied scale changes to promoted child definition")

	if err = kubernetes.UpdateResource(ctx, c, promotedChildDef); err != nil {
		return false, fmt.Errorf("error scaling down the existing promoted child: %w", err)
	}

	if rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus == nil {
		rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus = &apiv1.PromotedChildStatus{}
	}
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.Name = promotedChildDef.GetName()
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValues = scaleValuesMap
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToDesired to false in case previously set to true and now scaling back down to recover from a previous failure
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValuesRestoredToDesired = false

	return true, nil
}

// TTODO
func (r *MonoVertexRolloutReconciler) PostUpgradePromotedChildProcessing(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	// TTODO: put the scaling up logic in a function called `scaleUpPromotedChild` that this function calls

	numaLogger := logger.FromContext(ctx).WithName("PostUpgradePromotedChildProcessing").WithName("MonoVertexRollout")

	numaLogger.Debug("restoring scale values back to desired values for the promoted child source vertex")

	if liveRolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AreScaleValuesRestoredToDesired(promotedChildDef.GetName()) {
		return false, nil
	}

	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus == nil || promotedChildStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted child source vertex because the rollout does not have progressiveStatus and/or promotedChildStatus set")
	}

	if _, exists := promotedChildStatus.ScaleValues[promotedChildDef.GetName()]; !exists {
		return true, fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", promotedChildDef.GetName())
	}

	if err := unstructured.SetNestedField(promotedChildDef.Object, promotedChildStatus.ScaleValues[promotedChildDef.GetName()].DesiredMax, "spec", "scale", "max"); err != nil {
		return true, err
	}

	if err := unstructured.SetNestedField(promotedChildDef.Object, promotedChildStatus.ScaleValues[promotedChildDef.GetName()].DesiredMin, "spec", "scale", "min"); err != nil {
		return true, err
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("applied scale changes to promoted child definition")

	if err := kubernetes.UpdateResource(ctx, c, promotedChildDef); err != nil {
		return true, fmt.Errorf("error scaling back to desired min and max values the existing promoted child: %w", err)
	}

	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValuesRestoredToDesired = true

	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AllSourceVerticesScaledDown = false
	rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.ScaleValues = nil

	return true, nil
}
