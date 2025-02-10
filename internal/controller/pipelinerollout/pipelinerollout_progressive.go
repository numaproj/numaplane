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
	k8stypes "k8s.io/apimachinery/pkg/types"
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

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted pipeline.
It performs the following pre-upgrade operations:
- it ensures that the promoted pipeline source vertices are scaled down before proceeding with a progressive upgrade.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutPromotedChildStatus: the status of the promoted child, which may be updated during processing.
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPreUpgrade").WithName("PipelineRollout")

	numaLogger.Debug("started pre-upgrade processing of promoted pipeline")

	if rolloutPromotedChildStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// scaleDownPipelineSourceVertices either updates the promoted pipeline to scale down the source vertices
	// pods or retrieves the currently running pods to update the rolloutPromotedChildStatus scaleValues.
	// This serves to make sure that the source vertices pods have been really scaled down before proceeding
	// with the progressive upgrade.
	performedScaling, err := scaleDownPipelineSourceVertices(ctx, rolloutPromotedChildStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted pipeline")

	return performedScaling, nil
}

/*
ProcessPromotedChildPostUpgrade handles the post-upgrade processing of a promoted pipeline.
It performs the following post-upgrade operations:
- it restores the promoted pipeline source vertices scale values to the desired values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - rolloutPromotedChildStatus: the status of the promoted child, which may be updated during processing.
  - promotedChildDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessPromotedChildPostUpgrade(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("PipelineRollout")

	numaLogger.Debug("started post-upgrade processing of promoted pipeline")

	if rolloutPromotedChildStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	performedScaling, err := scalePipelineSourceVerticesToDesiredValues(ctx, rolloutPromotedChildStatus, promotedChildDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-upgrade processing of promoted pipeline")

	return performedScaling, nil
}

/*
scaleDownPipelineSourceVertices scales down the source vertices pods of a pipeline to half of the current count if not already scaled down.
It checks if all source vertices are already scaled down and skips the operation if true.
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
func scaleDownPipelineSourceVertices(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// If the pipeline source vertices have been scaled down already, do not perform scaling down operations
	if rolloutPromotedChildStatus.AreAllSourceVerticesScaledDown(promotedChildDef.GetName()) {
		// Return that scaling down was NOT performed
		return false, nil
	}

	vertices, _, err := unstructured.NestedSlice(promotedChildDef.Object, "spec", "vertices")
	if err != nil {
		return false, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedChildName", promotedChildDef.GetName(), "vertices", vertices).Debugf("found vertices for the promoted pipeline: %d", len(vertices))

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if rolloutPromotedChildStatus.ScaleValues != nil {
		scaleValuesMap = rolloutPromotedChildStatus.ScaleValues
	}

	promotedChildNeedsUpdate := false
	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, found, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return false, err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return false, err
			}
			if !found {
				return false, errors.New("a vertex must have a name")
			}

			pods, err := kubernetes.KubernetesClient.CoreV1().Pods(promotedChildDef.GetNamespace()).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("%s=%s, %s=%s",
					common.LabelKeyNumaflowPodPipelineName, promotedChildDef.GetName(),
					common.LabelKeyNumaflowPodPipelineVertexName, vertexName,
				),
			})
			if err != nil {
				return false, err
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
				return false, err
			}

			numaLogger.WithValues("promotedChildName", promotedChildDef.GetName(), "vertexName", vertexName).Debugf("found %d pod(s) for the source vertex, scaling down to %d", len(pods.Items), scaleValue)

			if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "max"); err != nil {
				return false, err
			}

			// If scale.min exceeds the new scale.max (scaleValue), reduce also scale.min to scaleValue
			originalMin, found, err := unstructured.NestedInt64(vertexAsMap, "scale", "min")
			if err != nil {
				return false, err
			}
			if found && originalMin > scaleValue {
				if err := unstructured.SetNestedField(vertexAsMap, scaleValue, "scale", "min"); err != nil {
					return false, err
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
		if err := patchPipelineVertices(ctx, promotedChildDef, vertices, c); err != nil {
			return false, fmt.Errorf("error scaling down the existing promoted pipeline: %w", err)
		}
	}

	numaLogger.WithValues("vertices", vertices, "scaleValuesMap", scaleValuesMap).Debug("updated the promoted pipeline with the new scale configuration")

	rolloutPromotedChildStatus.ScaleValues = scaleValuesMap
	rolloutPromotedChildStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToDesired to false in case previously set to true and now scaling back down to recover from a previous failure
	rolloutPromotedChildStatus.ScaleValuesRestoredToDesired = false

	return promotedChildNeedsUpdate, nil
}

/*
scalePipelineSourceVerticesToDesiredValues scales the source vertices of a pipeline to their desired values based on the rollout status.
This function checks if the pipeline source vertices have already been scaled to the desired values. If not, it restores the scale values
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
func scalePipelineSourceVerticesToDesiredValues(
	ctx context.Context,
	rolloutPromotedChildStatus *apiv1.PromotedChildStatus,
	promotedChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// If all the pipeline source vertices have been scaled back to desired values already, do not restore scaling values again
	if rolloutPromotedChildStatus.AreScaleValuesRestoredToDesired(promotedChildDef.GetName()) {
		// Return that scaling to desired values was NOT performed
		return false, nil
	}

	if rolloutPromotedChildStatus.ScaleValues == nil {
		return false, errors.New("unable to restore scale values for the promoted pipeline source vertices because the rollout does not have promotedChildStatus set")
	}

	vertices, _, err := unstructured.NestedSlice(promotedChildDef.Object, "spec", "vertices")
	if err != nil {
		return false, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, found, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return false, err
			}
			if !found {
				continue
			}

			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return false, err
			}
			if !found {
				return false, errors.New("a vertex must have a name")
			}

			if _, exists := rolloutPromotedChildStatus.ScaleValues[vertexName]; !exists {
				return false, fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", vertexName)
			}

			if err := unstructured.SetNestedField(vertexAsMap, rolloutPromotedChildStatus.ScaleValues[vertexName].DesiredMax, "scale", "max"); err != nil {
				return false, err
			}

			if err := unstructured.SetNestedField(vertexAsMap, rolloutPromotedChildStatus.ScaleValues[vertexName].DesiredMin, "scale", "min"); err != nil {
				return false, err
			}
		}
	}

	if err := patchPipelineVertices(ctx, promotedChildDef, vertices, c); err != nil {
		return false, fmt.Errorf("error scaling the existing promoted pipeline source vertices to desired values: %w", err)
	}

	numaLogger.WithValues("promotedChildDef", promotedChildDef).Debug("patched the promoted pipeline source vertices with the desired scale configuration")

	rolloutPromotedChildStatus.ScaleValuesRestoredToDesired = true
	rolloutPromotedChildStatus.AllSourceVerticesScaledDown = false
	rolloutPromotedChildStatus.ScaleValues = nil

	return true, nil
}

func patchPipelineVertices(ctx context.Context, promotedChildDef *unstructured.Unstructured, vertices []any, c client.Client) error {
	patch := &unstructured.Unstructured{Object: make(map[string]any)}
	err := unstructured.SetNestedSlice(patch.Object, vertices, "spec", "vertices")
	if err != nil {
		return err
	}

	patchAsBytes, err := patch.MarshalJSON()
	if err != nil {
		return err
	}

	if err := kubernetes.PatchResource(ctx, c, promotedChildDef, string(patchAsBytes), k8stypes.MergePatchType); err != nil {
		return err
	}

	return nil
}
