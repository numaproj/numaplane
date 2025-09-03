package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TODO: need to figure out how to handle users who have explicitly paused their pipelines or taken those pipelines down to 0 Pods
// If the previous upgrading pipeline explicitly said 0 pods or pause, then it implies we shouldn't run that one
// If the new promoted pipeline explicitly says 0 pods or pause, then it implies we shouldn't run that one

// TODO: consider relocating Recycle-specific functionality into its own file
// Recycle deletes child; returns true if it was in fact deleted
// This implements a function of the RolloutController interface
func (r *PipelineRolloutReconciler) Recycle(
	ctx context.Context,
	pipeline *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", fmt.Sprintf("%s/%s", pipeline.GetNamespace(), pipeline.GetName()))
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	pipelineRollout, err := numaflowtypes.GetRolloutForPipeline(ctx, c, pipeline)
	if err != nil {
		return false, fmt.Errorf("failed to get rollout for pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
	}

	// Need to determine how to delete the pipeline
	// Use the "upgrade-strategy-reason" Label to determine how
	// if upgrade-strategy-reason="delete/recreate", then don't pause at all (it will have already paused if we're in PPND)
	// if upgrade-strategy-reason="progressive success", then either just pause, or pause and drain,
	//    depending on PipelineRollout specification (TODO: https://github.com/numaproj/numaplane/issues/512)
	// if upgrade-strategy-reason="progressive failure", then don't pause at all (in this case we are replacing a failed pipeline)
	upgradeState, upgradeStateReason := ctlrcommon.GetUpgradeState(ctx, c, pipeline)
	if upgradeState == nil || *upgradeState != common.LabelValueUpgradeRecyclable {
		numaLogger.Error(errors.New("should not call Recycle() on a Pipeline which is not in recyclable Upgrade State"), "Recycle() called on pipeline",
			"namespace", pipeline.GetNamespace(), "name", pipeline.GetName(), "labels", pipeline.GetLabels())
	}
	requiresPause := false
	requiresPauseOriginalSpec := false
	if upgradeStateReason != nil {
		switch *upgradeStateReason {
		case common.LabelValueDeleteRecreateChild:
			// this is the case of the pipeline being deleted and recreated, either due to a change on the pipeline or on the isbsvc
			// which required that.
			// no need to pause here (for the case of PPND, it will have already been done before getting here)
		case common.LabelValueProgressiveSuccess, common.LabelValueDiscontinueProgressive, common.LabelValueProgressiveReplaced:
			// LabelValueProgressiveSuccess is the case of the previous "promoted" pipeline being deleted because the Progressive upgrade succeeded
			// LabelValueProgressiveReplaced is the case of the previous "upgrading" pipeline being deleted because it was replaced with a new pipeline during the upgrade process
			// in this case, we pause the pipeline because we want to push all of the remaining data in there through
			requiresPause = true
			requiresPauseOriginalSpec = true
		case common.LabelValueProgressiveReplacedFailed:
			requiresPause = true
		}
	}

	if !requiresPause {
		numaLogger.Info("Pipeline will be deleted now")
		err = kubernetes.DeleteResource(ctx, c, pipeline)
		return true, err
	}

	// Is the pipeline still defined with its original spec or have we overridden it with that of the "promoted" pipeline?
	originalSpec := true
	_, found := pipeline.GetAnnotations()[common.AnnotationKeyOverriddenSpec]
	if found {
		originalSpec = false
	}

	// if the recycling strategy requires pausing with the original spec and we still have the original spec, then
	// make sure we pause it and check on it
	if requiresPauseOriginalSpec && originalSpec {
		paused, drained, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, c)
		if err != nil {
			return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		numaLogger.WithValues("paused", paused, "drained", drained).Debug("checking drain of Pipeline using original spec")
		if paused {
			if drained {
				numaLogger.Info("Pipeline has been drained and will be deleted now")
				err = kubernetes.DeleteResource(ctx, c, pipeline)
				return true, err
			} else {
				// implicitly fall through
			}
		} else {
			return false, nil
		}
	}

	// if pausing original spec first:
	//   desiredPhase:Paused + min scale -> phase:Paused + min scale -> desiredPhase:Running + 0 scale -> phase:Running + 0 scale -> desiredPhase:Pausing + min scale -> phase:Paused + min scale
	// else:
	//.  desiredPhase:Running + 0 scale -> phase:Running + 0 scale -> desiredPhase:Pausing + min scale -> phase:Paused + min scale

	// force drain:
	// if no new promoted, ensure scaled to 0 and return
	currentPromotedPipeline, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradePromoted, nil, true, c)
	if err != nil {
		return false, fmt.Errorf("failed to find current promoted pipeline for rollout %s/%s: %w", pipelineRollout.Namespace, pipelineRollout.Name, err)
	}
	isNewPromotedPipeline, err := ctlrcommon.IsChildNewer(pipelineRollout.Name, currentPromotedPipeline.GetName(), pipeline.GetName())
	if err != nil {
		return false, err
	}
	if !isNewPromotedPipeline {
		numaLogger.Debug("No new promoted pipeline found, scaling current pipeline to zero")
		err = ensurePipelineScaledToZero(ctx, pipeline, c)
		if err != nil {
			return false, fmt.Errorf("failed to scale pipeline %s/%s to zero: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}

		return false, nil
	} else {

		// if we still have the original spec, we need to update with the promoted pipeline's spec
		if originalSpec {
			numaLogger.WithValues("promotedPipeline", currentPromotedPipeline.GetName()).Info("Found newer promoted pipeline, will force apply it")
			// update spec with 0 scale, overridden-spec=true, desiredPhase=Running
			forceApplySpecOnUndrainablePipeline(ctx, pipeline, currentPromotedPipeline, c)
			return false, nil
		}
		// we need to make sure we get out of the previous Paused state
		// TODO: what if user intended that their pipeline be paused, though?
		// if desiredPhase==Running and phase==Paused, return
		desiredPhase, err := numaflowtypes.GetPipelineDesiredPhase(pipeline)
		if err != nil {
			return false, err
		}
		isPaused := numaflowtypes.CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhasePaused)
		if desiredPhase == string(numaflowv1.PipelinePhaseRunning) && isPaused {
			numaLogger.WithValues("desiredPhase", desiredPhase, "currentPhase", "Paused").Debug("Pipeline transitioning from paused to running, waiting for completion")
			return false, nil
		}

		paused, drained, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, c)
		if err != nil {
			return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		numaLogger.WithValues("paused", paused, "drained", drained).Debug("checking drain of Pipeline using latest promoted pipeline's spec")
		if paused {
			numaLogger.WithValues("drained", drained).Infof("Pipeline has the promoted pipeline's spec and has paused, now ready to delete")
			err = kubernetes.DeleteResource(ctx, c, pipeline)
			return true, err
		}
	}

	return false, nil

}

func forceApplySpecOnUndrainablePipeline(ctx context.Context, currentPipeline *unstructured.Unstructured, newPipeline *unstructured.Unstructured, c client.Client) error {

	numaLogger := logger.FromContext(ctx)

	// take the newPipeline Spec, make a copy, and set its scale.min and max to 0
	currentVertexSpecs, err := numaflowtypes.GetPipelineVertexDefinitions(currentPipeline)
	if err != nil {
		return fmt.Errorf("failed to get vertices from pipeline %s/%s: %w", currentPipeline.GetNamespace(), currentPipeline.GetName(), err)
	}
	zero := int64(0)
	vertexScaleDefinitions := make([]apiv1.VertexScaleDefinition, len(currentVertexSpecs))
	for vertexIndex, currentVertexSpec := range currentVertexSpecs {
		if vertexAsMap, ok := currentVertexSpec.(map[string]any); ok {
			vertexScaleDefinitions[vertexIndex] = apiv1.VertexScaleDefinition{
				VertexName: vertexAsMap["name"].(string),
				ScaleDefinition: &apiv1.ScaleDefinition{
					Min: &zero,
					Max: &zero,
				},
			}
		}
	}

	newPipelineCopy := newPipeline.DeepCopy()
	applyScaleValuesToPipelineDefinition(ctx, newPipelineCopy, vertexScaleDefinitions)

	// Set the desiredPhase to Running just in case it isn't (we need to make to take it out of Paused state if it's in it to give it a chance to pause again)
	// and set the "overridden-spec" annotation to indicate that we've applied over top the original
	err = unstructured.SetNestedField(newPipelineCopy.Object, numaflowv1.PipelinePhaseRunning, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}
	annotations := newPipelineCopy.GetAnnotations()
	annotations[common.AnnotationKeyOverriddenSpec] = "true"
	newPipelineCopy.SetAnnotations(annotations)

	// Take the difference between this newPipelineCopy spec and the original currentPipeline spec to derive the patch we need and then apply it

	// Create a strategic merge patch by comparing the current pipeline with the new pipeline copy
	// We need to extract just the fields we want to update: spec, metadata.annotations
	patchData := map[string]interface{}{
		"spec": newPipelineCopy.Object["spec"],
		"metadata": map[string]interface{}{
			"annotations": newPipelineCopy.GetAnnotations(),
		},
	}

	// Convert patch data to JSON
	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w", err)
	}

	numaLogger.WithValues("currentPipeline", currentPipeline.GetName(), "patch", string(patchBytes)).Debug("applying strategic merge patch to pipeline")

	// Apply the strategic merge patch to the current pipeline
	err = kubernetes.PatchResource(ctx, c, currentPipeline, string(patchBytes), k8stypes.StrategicMergePatchType)
	if err != nil {
		return fmt.Errorf("failed to apply patch to pipeline %s: %w", currentPipeline.GetName(), err)
	}

	numaLogger.WithValues("currentPipeline", currentPipeline.GetName()).Debug("successfully applied patch to pipeline")
	return nil
}

// make sure Pipeline's desiredPhase==Paused and if not set it
// make sure its scale and pauseGracePeriodSeconds is adjusted if necessary
// return:
// - whether phase==Paused
// - whether fully drained
// - error if any
func drainRecyclablePipeline(
	ctx context.Context,
	pipeline *unstructured.Unstructured,
	pipelineRollout *apiv1.PipelineRollout,
	c client.Client,
) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	desiredPhase, err := numaflowtypes.GetPipelineDesiredPhase(pipeline)
	if err != nil {
		return false, false, err
	}
	if desiredPhase != string(numaflowv1.PipelinePhasePaused) {
		recycleScaleFactor := getRecycleScaleFactor(pipelineRollout)
		numaLogger.WithValues("scaleFactor", recycleScaleFactor).Debug("scale factor to scale down by during pausing")

		newVertexScaleDefinitions, err := calculateScaleForRecycle(ctx, pipeline, pipelineRollout, recycleScaleFactor)
		if err != nil {
			return false, false, err
		}

		newPauseGracePeriodSeconds, err := calculatePauseTimeForRecycle(ctx, pipeline, pipelineRollout, 100.0/float64(recycleScaleFactor))
		if err != nil {
			return false, false, err
		}

		// patch the pipeline to update the scale values
		err = applyScaleValuesToLivePipeline(ctx, pipeline, newVertexScaleDefinitions, c)
		if err != nil {
			return false, false, err
		}

		// patch the pipeline to set desiredPhase=Paused and set the new pause time
		patchJson := fmt.Sprintf(`{"spec": {"lifecycle": {"desiredPhase": "Paused", "pauseGracePeriodSeconds": %d}}}`, newPauseGracePeriodSeconds)
		numaLogger.WithValues("pipeline", pipeline.GetName(), "patchJson", patchJson).Debug("patching pipeline lifecycle")

		err = kubernetes.PatchResource(ctx, c, pipeline, patchJson, k8stypes.MergePatchType)
		if err != nil {
			return false, false, err
		}

		return false, false, nil

	} else {
		// return if Pipeline is Paused and if so if it's Drained
		isPaused := numaflowtypes.CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhasePaused)
		isDrained := false
		if isPaused {
			isDrained, _ = numaflowtypes.CheckPipelineDrained(ctx, pipeline)
		}
		return isPaused, isDrained, nil
	}

}

// return the new pauseGracePeriodSeconds to use for the Pipeline, based off of the original pauseGracePeriodSeconds, divided by the recycleScaleFactor
func calculatePauseTimeForRecycle(
	ctx context.Context,
	pipeline *unstructured.Unstructured,
	pipelineRollout *apiv1.PipelineRollout,
	multiplier float64,
) (int64, error) {
	numaLogger := logger.FromContext(ctx)

	// get pipeline spec
	pipelineSpec, err := numaflowtypes.GetPipelineSpecFromRollout(pipeline.GetName(), pipelineRollout)
	if err != nil {
		return 0, err
	}
	origPauseGracePeriodSeconds, found, err := unstructured.NestedInt64(pipelineSpec, "lifecycle", "pauseGracePeriodSeconds")
	if err != nil {

	}
	if !found {
		numaLogger.Debugf("pauseGracePeriodSeconds field not found, will use default %d seconds", defaultPauseGracePeriodSeconds)
		origPauseGracePeriodSeconds = defaultPauseGracePeriodSeconds
	}
	newPauseGracePeriodSeconds := float64(origPauseGracePeriodSeconds) * multiplier
	return int64(math.Ceil(newPauseGracePeriodSeconds)), nil

}

// multiply the number of Pods that were running previously by a promoted Pipeline of this PipelineRollout by some factor
// return the new Vertex Scale Definitions
// if the Vertex is new, just use the min value defined in the PipelineRollout
func calculateScaleForRecycle(
	ctx context.Context,
	pipeline *unstructured.Unstructured,
	pipelineRollout *apiv1.PipelineRollout,
	percent int32,
) ([]apiv1.VertexScaleDefinition, error) {
	numaLogger := logger.FromContext(ctx)

	// get the spec for the Pipeline that we need to scale down: this tells us what all the vertices are that we need to account for
	currentVertexSpecs, err := numaflowtypes.GetPipelineVertexDefinitions(pipeline)
	if err != nil {
		return nil, err
	}

	// get the definition of the pipeline spec in the PipelineRollout: if we don't have the historical pod count for a given vertex because it's new
	// then we will need to refer here for the scale.min value
	pipelineRolloutDefinedSpec, err := numaflowtypes.GetPipelineSpecFromRollout(pipeline.GetName(), pipelineRollout)
	if err != nil {
		return nil, err
	}

	// get the number of Pods that were historically running in the last "promoted" Pipeline before it was scaled down
	// so we can get an idea of how many need to run normally
	historicalPodCount := pipelineRollout.Status.ProgressiveStatus.HistoricalPodCount
	if historicalPodCount == nil {
		return nil, fmt.Errorf("HistoricalPodCount is nil for PipelineRollout %s/%s", pipelineRollout.Namespace, pipelineRollout.Name)
	}

	// Create the VertexScaleDefinitions that we'll return
	vertexScaleDefinitions := make([]apiv1.VertexScaleDefinition, len(currentVertexSpecs))

	for vertexIndex, currentVertexSpec := range currentVertexSpecs {
		if vertexAsMap, ok := currentVertexSpec.(map[string]any); ok {

			// Get the vertex's name
			vertexName, found, err := unstructured.NestedString(vertexAsMap, "name")
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, errors.New("a vertex must have a name")
			}

			var newScaleValue int64

			// If the Vertex is a source type, scale it to 0 (we don't want to be ingesting any new data)
			_, isSource, _ := unstructured.NestedFieldNoCopy(vertexAsMap, "source")
			if isSource {
				newScaleValue = 0
				numaLogger.WithValues("vertex", vertexName).Debug("Vertex is source, setting its scale to 0")
			}

			// If the Vertex was running previously in the "promoted" Pipeline, then multiply by the number that was running then
			originalPodsRunning, found := historicalPodCount[vertexName]
			if found {
				newScaleValue = int64(math.Ceil(float64(originalPodsRunning) * float64(percent) / 100.0))
				numaLogger.WithValues("vertex", vertexName, "newScaleValue", newScaleValue, "originalPodsRunning", originalPodsRunning, "percent", percent).Debug("Setting Vertex Scale value to percent of previous running count")
			} else {
				// This Vertex was not running in the "promoted" Pipeline: it must be new
				// We can set the newScaleValue from the PipelineRollout min

				pipelineRolloutVertexDef, found, err := numaflowtypes.GetVertexFromPipelineSpecMap(pipelineRolloutDefinedSpec, vertexName)
				if err != nil {
					return nil, fmt.Errorf("can't calculate scale for vertex %q, error getting vertex from PipelineRollout: %+v", vertexName, pipelineRolloutDefinedSpec)
				}
				if !found {
					// Vertex not found in the PipelineRollout or in the Historical Pod Count
					vertexScaleDef, err := progressive.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
					if err != nil {
						return nil, err
					}
					if vertexScaleDef.Min == nil {
						newScaleValue = 1
					} else {
						newScaleValue = *vertexScaleDef.Min
					}
					numaLogger.WithValues("vertex", vertexName).Debugf("Vertex not found in PipelineRollout %+v nor in Historical Pod Count %v, setting newScaleValue to %d", pipelineRolloutDefinedSpec, historicalPodCount, newScaleValue)
				} else {
					// set the newScaleValue from the PipelineRollout min
					floatVal, found, err := unstructured.NestedFloat64(pipelineRolloutVertexDef, "scale", "min")
					newScaleValue = int64(floatVal)
					numaLogger.WithValues("vertex", vertexName, "newScaleValue", newScaleValue).Debug("Vertex not found in Historical Pod Count, using PipelineRollout vertex min value")
					if err != nil {
						return nil, fmt.Errorf("can't calculate scale for vertex %q, error getting scale.min from PipelineRollout: %+v, err=%v", vertexName, pipelineRolloutDefinedSpec, err)
					}
					if !found {
						// If the scale.min wasn't set in PipelineRollout, it is equivalent to 1
						numaLogger.WithValues("vertex", vertexName, "pipelineRolloutDefinedSpec", pipelineRolloutDefinedSpec, "historicalPodCount", historicalPodCount).Debug("Vertex not found in Historical Pod Count, and scale.min not defined in PipelineRollout, so setting newScaleValue to 1")
						newScaleValue = 1
					}
				}
			}

			vertexScaleDefinitions[vertexIndex] = apiv1.VertexScaleDefinition{
				VertexName: vertexName,
				ScaleDefinition: &apiv1.ScaleDefinition{
					Min: &newScaleValue,
					Max: &newScaleValue,
				},
			}

		}
	}

	return vertexScaleDefinitions, nil
}

// return the percent by which the PipelineRollout should scale down if defined; otherwise, return the one defined by config file
func getRecycleScaleFactor(pipelineRollout *apiv1.PipelineRollout) int32 {
	if pipelineRollout.Spec.Strategy.RecycleStrategy.ScaleFactor != nil {
		return *pipelineRollout.Spec.Strategy.RecycleStrategy.ScaleFactor
	}

	// get the globally configured strategy
	globalConfig, _ := config.GetConfigManagerInstance().GetConfig()
	if globalConfig.Pipeline.RecycleScaleFactor != nil {
		return *globalConfig.Pipeline.RecycleScaleFactor
	}
	// not defined, return default
	return 50
}
