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
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// TTODO
func (r *PipelineRolloutReconciler) PreUpgradePromotedChildProcessing(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ScaleDownPromotedChildSourceVertices").WithName("PipelineRollout")

	numaLogger.Debug("started promoted child source vertices scaling down process")

	// If all source vertices have been scaled down already, there is no need to perform scaling down operations again.
	// Also, no need to requeue since this confirms the running pods are scaled down has needed.
	if liveRolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()) {
		numaLogger.Debug("monovertex already scaled down, exiting scaling down process")
		return false, nil
	}

	vertices, _, err := unstructured.NestedSlice(promotedChildDef.Object, "spec", "vertices")
	if err != nil {
		return true, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedChildName", promotedChildDef.GetName(), "vertices", vertices).Debugf("found vertices for the promoted child: %d", len(vertices))

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
				return true, err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return true, err
			}
			if !found {
				return true, errors.New("a vertex must have a name")
			}

			pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedChildDef.GetNamespace()).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("%s=%s, %s=%s",
					common.LabelKeyNumaflowPodPipelineName, promotedChildDef.GetName(),
					common.LabelKeyNumaflowPodPipelineVertexName, vertexName,
				),
			})
			if err != nil {
				return true, err
			}

			actualPodsCount := int64(len(pods.Items))

			numaLogger.WithValues("vertexName", vertexName, "actualPodsCount", actualPodsCount).Debugf("found pods for the source vertex")

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
				return true, err
			}

			numaLogger.WithValues("promotedChildName", promotedChildDef.GetName(), "vertexName", vertexName).Debugf("found %d pod(s) for the source vertex, scaling down to %d", len(pods.Items), scaleValue)

			if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "max"); err != nil {
				return true, err
			}

			// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
			originalMin, found, err := unstructured.NestedInt64(vertexAsMap, "scale", "min")
			if err != nil {
				return true, err
			}
			if found && originalMin > scaleValue {
				if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "min"); err != nil {
					return true, err
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
			return true, err
		}

		numaLogger.WithValues("vertices", vertices, "scaleValuesMap", scaleValuesMap).Debug("applied updated vertices to promoted child definition")

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
	}

	return promotedChildNeedsUpdate, nil
}

// TTODO
func (r *PipelineRolloutReconciler) PostUpgradePromotedChildProcessing(
	ctx context.Context,
	rolloutObject ctlrcommon.RolloutObject,
	liveRolloutObject ctlrcommon.RolloutObject,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ScalePromotedChildSourceVerticesToDesiredValues").WithName("PipelineRollout")

	numaLogger.Debug("restoring scale values back to desired values for the promoted child source vertices")

	if liveRolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus.AreScaleValuesRestoredToDesired(promotedChildDef.GetName()) {
		return false, nil
	}

	promotedChildStatus := rolloutObject.GetRolloutStatus().ProgressiveStatus.PromotedChildStatus
	if promotedChildStatus == nil || promotedChildStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted child source vertices because the rollout does not have progressiveStatus and/or promotedChildStatus set")
	}

	vertices, _, err := unstructured.NestedSlice(promotedChildDef.Object, "spec", "vertices")
	if err != nil {
		return true, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, found, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return true, err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return true, err
			}
			if !found {
				return true, errors.New("a vertex must have a name")
			}

			if _, exists := promotedChildStatus.ScaleValues[vertexName]; !exists {
				return true, fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", vertexName)
			}

			if err := unstructured.SetNestedField(vertexAsMap, promotedChildStatus.ScaleValues[vertexName].DesiredMax, "scale", "max"); err != nil {
				return true, err
			}

			if err := unstructured.SetNestedField(vertexAsMap, promotedChildStatus.ScaleValues[vertexName].DesiredMin, "scale", "min"); err != nil {
				return true, err
			}
		}
	}

	err = unstructured.SetNestedSlice(promotedChildDef.Object, vertices, "spec", "vertices")
	if err != nil {
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
