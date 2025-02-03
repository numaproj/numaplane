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

// ScaleDownPromotedChildSourceVertices scales down the source vertex of a promoted child
// monovertex. It retrieves the most current promoted child and scales down the source vertex
// by adjusting its max and min scale values. The function returns
// a map of vertex names to their scale values, including desired, scaled, and actual pod counts.
// The returned map will only have one vertex for the MonoVertex case.
//
// Parameters:
//
//	ctx - The context for the operation.
//	rolloutObject - The rollout object containing the status and configuration.
//	promotedChildDef - The unstructured definition of the promoted child.
//	c - The Kubernetes client for interacting with the cluster.
//
// Returns:
//
//	A map of vertex names to their scale values and an error if any occurs during the process.
func (r *MonoVertexRolloutReconciler) ScaleDownPromotedChildSourceVertices(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (map[string]apiv1.ScaleValues, bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ScaleDownPromotedChildSourceVertices").WithName("MonoVertexRollout")

	numaLogger.Debug("started promoted child source vertex scaling down process")

	promotedChild, err := progressive.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradePromoted, true, c)
	if err != nil {
		return nil, false, fmt.Errorf("error while looking for most current promoted child: %w", err)
	}

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus != nil && promotedChildStatus.ScaleValues != nil {
		scaleValuesMap = promotedChildStatus.ScaleValues
	}

	pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedChild.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s, %s=%s",
			common.LabelKeyNumaflowPodMonoVertexName, promotedChild.GetName(),
			// the vertex name for a monovertex is the same as the monovertex name
			common.LabelKeyNumaflowPodMonoVertexVertexName, promotedChild.GetName(),
		),
	})
	if err != nil {
		return nil, false, err
	}

	actualPodsCount := int64(len(pods.Items))

	// If for the vertex we already set a Scaled scale value, we only need to update the actual pods count
	// to later verify that the pods were actually scaled down.
	// We want to skip scaling down again.
	if vertexScaleValues, exist := scaleValuesMap[promotedChild.GetName()]; exist && vertexScaleValues.Scaled != 0 {
		vertexScaleValues.Actual = actualPodsCount
		scaleValuesMap[promotedChild.GetName()] = vertexScaleValues

		numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debug("updated scaleValues map with running pods count, skipping scaling down since it has already been done")
		return scaleValuesMap, false, nil
	}

	scaleValue := int64(math.Floor(float64(actualPodsCount) / float64(2)))

	originalMax, _, err := unstructured.NestedInt64(promotedChildDef.Object, "spec", "scale", "max")
	if err != nil {
		return nil, false, err
	}

	numaLogger.WithValues("promotedChildName", promotedChild.GetName()).Debugf("found %d pod(s) for the source vertex, scaling down to %d", len(pods.Items), scaleValue)

	if err := unstructured.SetNestedField(promotedChildDef.Object, scaleValue, "spec", "scale", "max"); err != nil {
		return nil, false, err
	}

	// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
	originalMin, found, err := unstructured.NestedInt64(promotedChildDef.Object, "spec", "scale", "min")
	if err != nil {
		return nil, false, err
	}
	if found && originalMin > scaleValue {
		if err := unstructured.SetNestedField(promotedChildDef.Object, scaleValue, "spec", "scale", "min"); err != nil {
			return nil, false, err
		}
	}

	scaleValuesMap[promotedChild.GetName()] = apiv1.ScaleValues{
		DesiredMin: originalMin,
		DesiredMax: originalMax,
		Scaled:     scaleValue,
		Actual:     actualPodsCount,
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef, "scaleValuesMap", scaleValuesMap).Debug("applied scale changes to promoted child definition")

	return scaleValuesMap, true, nil
}

/*
ScalePromotedChildSourceVerticesToDesiredValues restores the scale values of a promoted child source vertex
to its desired values as specified in the rollout object's progressive status.

Parameters:
  - ctx: The context for logging and tracing.
  - rolloutObject: The rollout object containing the progressive status and promoted child status.
  - promotedChildDef: The unstructured object representing the promoted child source vertex.
  - c: The Kubernetes client for interacting with the cluster.

Returns:
  - error: An error if the scale values cannot be restored due to missing status information or if setting
    the scale values fails.
*/
func (r *MonoVertexRolloutReconciler) ScalePromotedChildSourceVerticesToDesiredValues(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithName("ScalePromotedChildSourceVerticesToDesiredValues").WithName("MonoVertexRollout")

	numaLogger.Debug("restoring scale values back to desired values for the promoted child source vertex")

	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus == nil || promotedChildStatus.ScaleValues == nil {
		return errors.New("unable to restore scale values for the promoted child source vertex because the rollout does not have progressiveStatus and/or promotedChildStatus set")
	}

	if err := unstructured.SetNestedField(promotedChildDef.Object, promotedChildStatus.ScaleValues[promotedChildDef.GetName()].DesiredMax, "spec", "scale", "max"); err != nil {
		return err
	}

	if err := unstructured.SetNestedField(promotedChildDef.Object, promotedChildStatus.ScaleValues[promotedChildDef.GetName()].DesiredMin, "spec", "scale", "min"); err != nil {
		return err
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("applied scale changes to promoted child definition")

	return nil
}
