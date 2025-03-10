package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateUpgradingChildDefinition creates a definition for an "upgrading" pipeline
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject progressive.ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
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
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	return progressive.AssessUpgradingPipelineType(ctx, existingUpgradingChildDef)
}

/*
ProcessPromotedChildPreUpgrade handles the pre-upgrade processing of a promoted pipeline.
It performs the following pre-upgrade operations:
- it ensures that the promoted pipeline source vertices are scaled down before proceeding with a progressive upgrade.

Parameters:
  - ctx: the context for managing request-scoped values.
  - pipelineRollout: the pipelineRollout
  - promotedPipelineDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessPromotedChildPreUpgrade(
	ctx context.Context,
	pipelineRollout progressive.ProgressiveRolloutObject,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPreUpgrade").WithName("PipelineRollout")

	numaLogger.Debug("started pre-upgrade processing of promoted pipeline")
	pipelineRO, ok := pipelineRollout.(*apiv1.PipelineRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted pipeline pre-upgrade", pipelineRollout)
	}

	if pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform pre-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	// scaleDownPipelineSourceVertices either updates the promoted pipeline to scale down the source vertices
	// pods or retrieves the currently running pods to update the PromotedPipelineStatus scaleValues.
	// This serves to make sure that the source vertices pods have been really scaled down before proceeding
	// with the progressive upgrade.
	requeue, err := scaleDownPipelineSourceVertices(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed pre-upgrade processing of promoted pipeline")

	return requeue, nil
}

/*
ProcessPromotedChildPostFailure handles the post-upgrade processing of the promoted pipeline after the "upgrading" child has failed.
It performs the following post-upgrade operations:
- it restores the promoted pipeline source vertices scale values to the original values retrieved from the rollout status.

Parameters:
  - ctx: the context for managing request-scoped values.
  - pipelineRollout: the PipelineRollout instance
  - promotedPipelineDef: the definition of the promoted child as an unstructured object.
  - c: the client used for interacting with the Kubernetes API.

Returns:
  - A boolean indicating whether we should requeue.
  - An error if any issues occur during processing.
*/
func (r *PipelineRolloutReconciler) ProcessPromotedChildPostFailure(
	ctx context.Context,
	pipelineRollout progressive.ProgressiveRolloutObject,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessPromotedChildPostUpgrade").WithName("PipelineRollout")

	numaLogger.Debug("started post-failure processing of promoted pipeline")

	pipelineRO, ok := pipelineRollout.(*apiv1.PipelineRollout)
	if !ok {
		return true, fmt.Errorf("unexpected type for ProgressiveRolloutObject: %+v; can't process promoted pipeline post-upgrade", pipelineRollout)
	}

	if pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus == nil {
		return true, errors.New("unable to perform post-upgrade operations because the rollout does not have promotedChildStatus set")
	}

	requeue, err := scalePipelineSourceVerticesToOriginalValues(ctx, pipelineRO.Status.ProgressiveStatus.PromotedPipelineStatus, promotedPipelineDef, c)
	if err != nil {
		return true, err
	}

	numaLogger.Debug("completed post-failure processing of promoted pipeline")

	return requeue, nil
}

func (r *PipelineRolloutReconciler) ProcessUpgradingChildPostFailure(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("ProcessUpgradingChildPostFailure").WithName("PipelineRollout")

	numaLogger.Debug("started post-failure processing of upgrading pipeline")

	// TODO

	numaLogger.Debug("completed post-failure processing of upgrading pipeline")

	return false, nil
}

func (r *PipelineRolloutReconciler) ProcessUpgradingChildPreForcedPromotion(
	ctx context.Context,
	rolloutObject progressive.ProgressiveRolloutObject,
	upgradingChildDef *unstructured.Unstructured,
	c client.Client,
) error {

	// TODO
	return nil
}

/*
scaleDownPipelineSourceVertices scales down the source vertices pods of a pipeline to half of the current count if not already scaled down.
It checks if all source vertices are already scaled down and skips the operation if true.
The function updates the scale values in the rollout status and adjusts the scale configuration
of the promoted child definition. It ensures that the scale.min does not exceed the new scale.max.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedPipelineStatus: the status of the promoted child in the rollout.
- promotedPipelineDef: the unstructured object representing the promoted child definition.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if not all source vertices have been scaled down.
- error: an error if any operation fails during the scaling process.
*/
func scaleDownPipelineSourceVertices(
	ctx context.Context,
	promotedPipelineStatus *apiv1.PromotedPipelineStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scaleDownPipelineSourceVertices")

	// If the pipeline source vertices have been scaled down already, do not perform scaling down operations
	if promotedPipelineStatus.AreAllSourceVerticesScaledDown(promotedPipelineDef.GetName()) {
		return false, nil
	}

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
	if err != nil {
		return true, fmt.Errorf("error while getting vertices of promoted pipeline: %w", err)
	}

	numaLogger.WithValues("promotedChildName", promotedPipelineDef.GetName(), "vertices", vertices).Debugf("found vertices for the promoted pipeline: %d", len(vertices))

	scaleValuesMap := map[string]apiv1.ScaleValues{}
	if promotedPipelineStatus.ScaleValues != nil {
		scaleValuesMap = promotedPipelineStatus.ScaleValues
	}

	promotedChildNeedsUpdate := false
	for _, vertex := range vertices {
		if vertexAsMap, ok := vertex.(map[string]any); ok {
			_, foundVertexSource, err := unstructured.NestedMap(vertexAsMap, "source")
			if err != nil {
				return true, err
			}
			if !foundVertexSource {
				continue
			}

			vertexName, foundVertexName, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return true, err
			}
			if !foundVertexName {
				return true, errors.New("a vertex must have a name")
			}

			podsList, err := kubernetes.ListPodsMetadataOnly(ctx, c, promotedPipelineDef.GetNamespace(), fmt.Sprintf(
				"%s=%s, %s=%s",
				common.LabelKeyNumaflowPodPipelineName, promotedPipelineDef.GetName(),
				common.LabelKeyNumaflowPodPipelineVertexName, vertexName,
			))
			if err != nil {
				return true, err
			}

			actualPodsCount := int64(len(podsList.Items))

			numaLogger.WithValues("vertexName", vertexName, "actualPodsCount", actualPodsCount).Debugf("found pods for the source vertex")

			// If for the vertex we already set a Scaled scale value, we only need to update the actual pods count
			// to later verify that the pods were actually scaled down.
			// We want to skip scaling down again.
			if vertexScaleValues, exist := scaleValuesMap[vertexName]; exist {
				vertexScaleValues.Actual = actualPodsCount
				scaleValuesMap[vertexName] = vertexScaleValues

				numaLogger.WithValues("scaleValuesMap", scaleValuesMap).Debugf("updated scaleValues map for vertex '%s' with running pods count, skipping scaling down for this vertex since it has already been done", vertexName)
				continue
			}

			promotedChildNeedsUpdate = true

			originalScaleDef, foundVertexScale, err := unstructured.NestedMap(vertexAsMap, "scale")
			if err != nil {
				return true, err
			}

			var originalScaleDefAsString *string
			if foundVertexScale {
				jsonBytes, err := json.Marshal(originalScaleDef)
				if err != nil {
					return true, err
				}

				jsonString := string(jsonBytes)
				originalScaleDefAsString = &jsonString
			}

			newMin, newMax, err := progressive.CalculateScaleMinMaxValues(vertexAsMap, int(actualPodsCount), []string{"scale", "min"})
			if err != nil {
				return true, fmt.Errorf("cannot calculate the scale min and max values: %+w", err)
			}

			numaLogger.WithValues(
				"promotedChildName", promotedPipelineDef.GetName(),
				"vertexName", vertexName,
				"actualPodsCount", actualPodsCount,
				"newMin", newMin,
				"newMax", newMax,
				"originalScaleDefAsString", originalScaleDefAsString,
			).Debugf("found %d pod(s) for the source vertex, scaling down to %d", actualPodsCount, newMax)

			if err := unstructured.SetNestedField(vertexAsMap, newMin, "scale", "min"); err != nil {
				return true, err
			}

			if err := unstructured.SetNestedField(vertexAsMap, newMax, "scale", "max"); err != nil {
				return true, err
			}

			scaleValuesMap[vertexName] = apiv1.ScaleValues{
				OriginalScaleDefinition: originalScaleDefAsString,
				ScaleTo:                 newMax,
				Actual:                  actualPodsCount,
			}
		}
	}

	if promotedChildNeedsUpdate {
		if err := patchPipelineVertices(ctx, promotedPipelineDef, vertices, c); err != nil {
			return true, fmt.Errorf("error scaling down the existing promoted pipeline: %w", err)
		}

		numaLogger.WithValues("vertices", vertices, "scaleValuesMap", scaleValuesMap).Debug("updated the promoted pipeline with the new scale configuration")
	}

	promotedPipelineStatus.ScaleValues = scaleValuesMap
	promotedPipelineStatus.MarkAllSourceVerticesScaledDown()

	// Set ScaleValuesRestoredToOriginal to false in case previously set to true and now scaling back down to recover from a previous failure
	promotedPipelineStatus.ScaleValuesRestoredToOriginal = false

	return !promotedPipelineStatus.AreAllSourceVerticesScaledDown(promotedPipelineDef.GetName()), nil
}

/*
scalePipelineSourceVerticesToOriginalValues scales the source vertices of a pipeline to their original values based on the rollout status.
This function checks if the pipeline source vertices have already been scaled to the original values. If not, it restores the scale values
from the rollout's promoted child status and updates the Kubernetes resource accordingly.

Parameters:
- ctx: the context for managing request-scoped values.
- promotedPipelineStatus: the status of the promoted child in the rollout, containing scale values.
- promotedPipelineDef: the unstructured definition of the promoted child resource.
- c: the Kubernetes client for resource operations.

Returns:
- bool: true if should requeue, false otherwise. Should requeue in case of error or if not all source vertices have been scaled back to the original values.
- An error if any issues occur during the scaling process.
*/
func scalePipelineSourceVerticesToOriginalValues(
	ctx context.Context,
	promotedPipelineStatus *apiv1.PromotedPipelineStatus,
	promotedPipelineDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {

	numaLogger := logger.FromContext(ctx).WithName("scalePipelineSourceVerticesToOriginalValues")

	// If all the pipeline source vertices have been scaled back to the original values already, do not restore scaling values again
	if promotedPipelineStatus.AreScaleValuesRestoredToOriginal(promotedPipelineDef.GetName()) {
		return false, nil
	}

	if promotedPipelineStatus.ScaleValues == nil {
		return true, errors.New("unable to restore scale values for the promoted pipeline source vertices because the rollout does not have promotedChildStatus set")
	}

	vertices, _, err := unstructured.NestedSlice(promotedPipelineDef.Object, "spec", "vertices")
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

			vertexScaleValues, exists := promotedPipelineStatus.ScaleValues[vertexName]
			if !exists {
				return true, fmt.Errorf("the scale values for vertex '%s' are not present in the rollout promotedChildStatus", vertexName)
			}

			if vertexScaleValues.OriginalScaleDefinition == nil {
				unstructured.RemoveNestedField(vertexAsMap, "scale")
			} else {
				scaleAsMap := map[string]any{}
				err := json.Unmarshal([]byte(*vertexScaleValues.OriginalScaleDefinition), &scaleAsMap)
				if err != nil {
					return true, fmt.Errorf("failed to unmarshal OriginalScaleDefinition: %w", err)
				}

				if err := unstructured.SetNestedField(vertexAsMap, scaleAsMap, "scale"); err != nil {
					return true, err
				}
			}
		}
	}

	if err := patchPipelineVertices(ctx, promotedPipelineDef, vertices, c); err != nil {
		return true, fmt.Errorf("error scaling the existing promoted pipeline source vertices to original values: %w", err)
	}

	numaLogger.WithValues("promotedPipelineDef", promotedPipelineDef).Debug("patched the promoted pipeline source vertices with the original scale configuration")

	promotedPipelineStatus.ScaleValuesRestoredToOriginal = true
	promotedPipelineStatus.AllSourceVerticesScaledDown = false
	promotedPipelineStatus.ScaleValues = nil

	return false, nil
}

func patchPipelineVertices(ctx context.Context, promotedPipelineDef *unstructured.Unstructured, vertices []any, c client.Client) error {
	patch := &unstructured.Unstructured{Object: make(map[string]any)}
	err := unstructured.SetNestedSlice(patch.Object, vertices, "spec", "vertices")
	if err != nil {
		return err
	}

	patchAsBytes, err := patch.MarshalJSON()
	if err != nil {
		return err
	}

	if err := kubernetes.PatchResource(ctx, c, promotedPipelineDef, string(patchAsBytes), k8stypes.MergePatchType); err != nil {
		return err
	}

	return nil
}
