package pipelinerollout

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" pipeline
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	metadata, err := getBasePipelineMetadata(pipelineRollout)
	if err != nil {
		return nil, err
	}

	// which InterstepBufferServiceName should we use?
	// If there is an upgrading isbsvc, use that
	// Otherwise, use the promoted one
	isbsvc, err := r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradeInProgress)
	if err != nil {
		return nil, err
	}
	if isbsvc == nil {
		numaLogger.Debugf("no Upgrading isbsvc found for Pipeline, will find promoted one")
		isbsvc, err = r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradePromoted)
		if err != nil || isbsvc == nil {
			return nil, fmt.Errorf("failed to find isbsvc that's 'promoted': won't be able to reconcile PipelineRollout, err=%v", err)
		}
	}

	pipeline, err := r.makePipelineDefinition(pipelineRollout, name, isbsvc.GetName(), metadata)
	if err != nil {
		return nil, err
	}

	labels := pipeline.GetLabels()
	labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradeInProgress)
	labels[common.LabelKeyISBServiceChildNameForPipeline] = isbsvc.GetName()
	pipeline.SetLabels(labels)

	return pipeline, nil
}

// AssessUpgradingChild makes an assessment of the upgrading child to determine if it was successful, failed, or still not known
// Assessment:
// Success: phase must be "Running" and all conditions must be True
// Failure: phase is "Failed" or any condition is False
// Unknown: neither of the above if met
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {

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

// ScaleDownPromotedChildSourceVertices scales down the source vertices of a promoted child
// pipeline. It retrieves the most current promoted child, identifies source vertices,
// and scales them down by adjusting their max and min scale values. The function returns
// a map of vertex names to their scale values, including desired, scaled, and actual pod counts.
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
func (r *PipelineRolloutReconciler) ScaleDownPromotedChildSourceVertices(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (map[string]apiv1.ScaleValues, bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ScaleDownPromotedChildSourceVertices").WithName("PipelineRollout")

	numaLogger.Debug("started promoted child source vertices scaling down process")

	promotedChild, err := progressive.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, common.LabelValueUpgradePromoted, true, c)
	if err != nil {
		return nil, false, fmt.Errorf("error while looking for most current promoted child: %w", err)
	}

	vertices, _, err := unstructured.NestedSlice(promotedChildDef.Object, "spec", "vertices")
	if err != nil {
		return nil, false, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedChildName", promotedChild.GetName(), "vertices", vertices).Debugf("found vertices for the promoted child: %d", len(vertices))

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus != nil && promotedChildStatus.ScaleValues != nil {
		scaleValuesMap = promotedChildStatus.ScaleValues
	}

	promotedChildNeedsUpdate := false
	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, found, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return nil, false, err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return nil, false, err
			}
			if !found {
				return nil, false, errors.New("a vertex must have a name")
			}

			pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedChild.GetNamespace()).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("%s=%s, %s=%s",
					common.LabelKeyNumaflowPodPipelineName, promotedChild.GetName(),
					common.LabelKeyNumaflowPodPipelineVertexName, vertexName,
				),
			})
			if err != nil {
				return nil, false, err
			}

			actualPodsCount := int64(len(pods.Items))

			// If for the vertex we already set a Scaled scale value, we only need to update the actual pods count
			// to later verify that the pods were actually scaled down.
			// We want to skip scaling down again.
			if vertexScaleValues, exist := scaleValuesMap[vertexName]; exist && vertexScaleValues.ScaleTo != 0 {
				vertexScaleValues.Actual = actualPodsCount
				scaleValuesMap[vertexName] = vertexScaleValues

				numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debugf("updated scaleValues map for vertex '%s' with running pods count, skipping scaling down for this vertex since it has already been done", vertexName)
				continue
			}

			promotedChildNeedsUpdate = true

			scaleValue := int64(math.Floor(float64(actualPodsCount) / float64(2)))

			originalMax, _, err := unstructured.NestedInt64(vertexAsMap, "scale", "max")
			if err != nil {
				return nil, false, err
			}

			numaLogger.WithValues("promotedChildName", promotedChild.GetName(), "vertexName", vertexName).Debugf("found %d pod(s) for the source vertex, scaling down to %d", len(pods.Items), scaleValue)

			if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "max"); err != nil {
				return nil, false, err
			}

			// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
			originalMin, found, err := unstructured.NestedInt64(vertexAsMap, "scale", "min")
			if err != nil {
				return nil, false, err
			}
			if found && originalMin > scaleValue {
				if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "min"); err != nil {
					return nil, false, err
				}
			}

			scaleValuesMap[vertexName] = apiv1.ScaleValues{
				DesiredMin: originalMin,
				DesiredMax: originalMax,
				ScaleTo:    scaleValue,
				Actual:     actualPodsCount,
			}
		}
	}

	if promotedChildNeedsUpdate {
		err = unstructured.SetNestedSlice(promotedChildDef.Object, vertices, "spec", "vertices")
		if err != nil {
			return nil, false, err
		}

		numaLogger.WithValues("vertices", vertices, "scaleValuesMap", scaleValuesMap).Debug("applied updated vertices to promoted child definition")
	}

	return scaleValuesMap, promotedChildNeedsUpdate, nil
}

/*
ScalePromotedChildSourceVerticesToDesiredValues restores the scale values of a promoted child source vertices
to its desired values as specified in the rollout object's progressive status.

Parameters:
  - ctx: The context for logging and tracing.
  - rolloutObject: The rollout object containing the progressive status and promoted child status.
  - promotedChildDef: The unstructured object representing the promoted child source vertices.
  - c: The Kubernetes client for interacting with the cluster.

Returns:
  - error: An error if the scale values cannot be restored due to missing status information or if setting
    the scale values fails.
*/
func (r *PipelineRolloutReconciler) ScalePromotedChildSourceVerticesToDesiredValues(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) error {

	numaLogger := logger.FromContext(ctx).WithName("ScalePromotedChildSourceVerticesToDesiredValues").WithName("PipelineRollout")

	numaLogger.Debug("restoring scale values back to desired values for the promoted child source vertices")

	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus == nil || promotedChildStatus.ScaleValues == nil {
		return errors.New("unable to restore scale values for the promoted child source vertices because the rollout does not have progressiveStatus and/or promotedChildStatus set")
	}

	vertices, _, err := unstructured.NestedSlice(promotedChildDef.Object, "spec", "vertices")
	if err != nil {
		return fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, found, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return err
			}
			if !found {
				return errors.New("a vertex must have a name")
			}

			if _, exists := promotedChildStatus.ScaleValues[vertexName]; !exists {
				return fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", vertexName)
			}

			if err := unstructured.SetNestedField(vertexAsMap, promotedChildStatus.ScaleValues[vertexName].DesiredMax, "scale", "max"); err != nil {
				return err
			}

			if err := unstructured.SetNestedField(vertexAsMap, promotedChildStatus.ScaleValues[vertexName].DesiredMin, "scale", "min"); err != nil {
				return err
			}
		}
	}

	err = unstructured.SetNestedSlice(promotedChildDef.Object, vertices, "spec", "vertices")
	if err != nil {
		return err
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("applied scale changes to promoted child definition")

	return nil
}
