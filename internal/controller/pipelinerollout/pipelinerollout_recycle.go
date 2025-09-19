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
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Recycle deletes child; returns true if it was in fact deleted
// This implements a function of the RolloutController interface
// For Pipelines, for the Progressive case, we first drain the Pipeline here before we delete it. (For PPND case, it will have been drained prior to this function.)
// And if the Pipeline can't drain by itself (in the case it's unhealthy), we "force drain" it by applying a new spec over top it.
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
	// for the progressive upgrade-strategy-reasons, we drain first
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
		case common.LabelValueProgressiveSuccess, common.LabelValueProgressiveReplaced, common.LabelValueDiscontinueProgressive:
			// LabelValueProgressiveSuccess is the case of the previous "promoted" pipeline being deleted because the Progressive upgrade succeeded
			// LabelValueProgressiveReplaced is the case of the previous "upgrading" pipeline being deleted because it was replaced with a new pipeline during the upgrade process
			// LabelValueDiscontinueProgressive is the case of an upgrade being discontinued
			// this generally happens if a user goes from spec A->B->A quickly before B has had a chance to be assessed
			// in this case, we pause the pipeline because we want to push all of the remaining data in there through
			requiresPause = true
			// first attempt the pause with the original spec because it may be able to pause on its own
			requiresPauseOriginalSpec = true
		case common.LabelValueProgressiveReplacedFailed:
			// LabelValueProgressiveReplacedFailed is the case of the "upgrading" pipeline failing and then being replaced with a newer Pipeline
			requiresPause = true
			// We don't attempt to pause it with the original spec first since it's failed and is very unlikely to be able to pause on its own.
			requiresPauseOriginalSpec = false
		}
	}

	if !requiresPause {
		numaLogger.Info("Pipeline will be deleted now")
		err = kubernetes.DeleteResource(ctx, c, pipeline)
		return true, err
	}

	// First check if the PipelineRollout is configured to run
	// If it's configured to be paused or has Vertex.scale.max==0, then we must respect the user's preference not to run
	// TODO: reduce the number of Pipelines which are in this state by deleting any pipeline which has always been paused or scaled to 0 its entire life, in which case there's
	// no risk of there being data in it to drain
	pauseDesired, err := checkUserDesiresPause(ctx, pipelineRollout, pipeline, c)
	if err != nil {
		return false, err
	}
	if pauseDesired {
		numaLogger.Debug("Pipeline is not supposed to run, per definition: will not drain it yet")
		return false, nil
	}

	// Is the pipeline still defined with its original spec or have we overridden it with that of the "promoted" pipeline?
	originalSpec := !isPipelineSpecOverridden(pipeline)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// if requiresPauseOriginalSpec==true, it means we first attempt to pause with our original spec, and only if that is not able to drain fully, we do force draining
	// if requiresPauseOriginalSpec==false, we are pretty sure the first attempt will fail, so we go straight to force draining
	// In either case, before Force Draining, we need to wait until there's a new promoted Pipeline we can use
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// if the recycling strategy requires pausing with the original spec and we still have the original spec, then
	// make sure we pause it and check on it
	if requiresPauseOriginalSpec && originalSpec {
		paused, drained, failed, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, c)
		if err != nil {
			return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		numaLogger.WithValues("paused", paused, "drained", drained).Debug("checking drain of Pipeline using original spec")
		if paused {
			if drained {
				numaLogger.Info("Pipeline has been drained and will be deleted now")
				err = kubernetes.DeleteResource(ctx, c, pipeline)
				return true, err
			} // else implicitly fall through to force draining

		} else if failed {
			numaLogger.Debug("Pipeline is in Failed phase; will force drain") // fall through to force draining
		} else {
			return false, nil
		}
	}

	// force drain:
	// if no new promoted pipeline, we need to wait: ensure we scale to 0 and return
	promotedPipeline, err := r.checkForPromotedPipelineForForceDrain(ctx, pipelineRollout)
	if err != nil {
		return false, fmt.Errorf("error checking for promoted pipeline for force drain for PipelineRollout %s/%s: %v", pipelineRollout.Namespace, pipelineRollout.Name, err)
	}
	if promotedPipeline == nil {
		numaLogger.Debug("No viable promoted pipeline found for force draining, scaling current pipeline to zero")
		err = ensurePipelineScaledToZero(ctx, pipeline, c)
		if err != nil {
			return false, fmt.Errorf("failed to scale pipeline %s/%s to zero: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}

		return false, nil
	} else {
		return forceDrain(ctx, pipeline, promotedPipeline, pipelineRollout, originalSpec, c)
	}

}

// apply a spec that's considered valid (from a promoted pipeline) over top a spec that's not working.
// The new spec will enable it to drain.
// Then pause it.
func forceDrain(ctx context.Context,
	// the pipeline whose spec will be updated
	pipeline *unstructured.Unstructured,
	// the definition of the pipeline whose spec will be used
	promotedPipeline *unstructured.Unstructured,
	// the PipelineRollout parent
	pipelineRollout *apiv1.PipelineRollout,
	// this function may be called multiple times
	// if originalSpec is true, we still need to update the spec
	// if false, just continue with the remaining process
	originalSpec bool,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// if we still have the original spec, we need to update with the promoted pipeline's spec
	if originalSpec {
		numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Info("Found promoted pipeline, will force apply it")
		// update spec with desiredPhase=Running and scaled to 0 initially, plus update the annotation to indicate that we've overridden the spec
		err := forceApplySpecOnUndrainablePipeline(ctx, pipeline, promotedPipeline, c)
		return false, err
	}

	// we need to make sure we get out of the previous Paused state before we Pause again, just to make sure that Numaflow will restart the pause
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

	// just to be sure, we also verify that observedGeneration==generation in order to confirm that numaflow has reconciled our previous changes first before we set desiredPhase=Running
	pipelineReconciled, generation, observedGeneration, err := numaflowtypes.CheckPipelineObservedGeneration(ctx, pipeline)
	if err != nil {
		return false, fmt.Errorf("error checking pipeline %s/%s observed generation: %v", pipeline.GetNamespace(), pipeline.GetName(), err)
	}
	if !pipelineReconciled {
		numaLogger.WithValues("generation", generation, "observedGeneration", observedGeneration).Debug("waiting for pipeline observedGeneration to match generation")
		return false, nil
	}

	// perform the drain
	paused, drained, failed, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, c)
	if err != nil {
		return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
	}
	numaLogger.WithValues("paused", paused, "drained", drained, "failed", failed).Debug("checking drain of Pipeline using latest promoted pipeline's spec")
	// if it's either paused or failed, delete it
	if paused || failed { // TODO: are we okay to delete on failure? could it be an intermittent failure? Ideally maybe we'd wait until pauseGracePeriodSeconds regardless?
		numaLogger.WithValues("paused", paused, "drained", drained, "failed", failed).Infof("Pipeline has the promoted pipeline's spec and has either paused or failed, now deleting it")
		err = kubernetes.DeleteResource(ctx, c, pipeline)
		return true, err
	}

	return false, nil
}

// if there's a Promoted Pipeline we can use for force drain, return it; otherwise return nil
// generally we try to only use "promoted" Pipelines which are "current", meaning they match the PipelineRollout spec
func (r *PipelineRolloutReconciler) checkForPromotedPipelineForForceDrain(ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) (*unstructured.Unstructured, error) {
	currentPromotedPipeline, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradePromoted, nil, true, r.client)
	if err != nil {
		return nil, fmt.Errorf("failed to find current promoted pipeline for Rollout %s/%s: %w", pipelineRollout.Namespace, pipelineRollout.Name, err)
	}

	// Compare the rollout definition to the "promoted" pipeline
	// In order to compare, we need to update the rollout definition to use the identical isbsvc name as the "promoted" pipeline so we can ignore that
	different, err := r.CheckForDifferencesWithRolloutDef(ctx, currentPromotedPipeline, pipelineRollout)
	if err != nil {
		return nil, err
	}
	if different {
		return nil, nil
	} else {
		return currentPromotedPipeline, nil
	}
}

// Update the pipeline to the new spec with min=max=0 initially and set to desiredPhase=Running
// (it will be set to Paused later)
func forceApplySpecOnUndrainablePipeline(
	ctx context.Context,
	// the pipeline that will be updated
	currentPipeline *unstructured.Unstructured,
	// spec from the new pipeline which will be applied
	newPipeline *unstructured.Unstructured,
	c client.Client) error {

	numaLogger := logger.FromContext(ctx)

	// take the newPipeline Spec, make a copy, and set any sources to min=max=0
	newPipelineCopy := newPipeline.DeepCopy()
	err := scalePipelineDefSourceVerticesToZero(ctx, newPipelineCopy)
	if err != nil {
		return err
	}

	// Set the desiredPhase to Running just in case it isn't (we need to make to take it out of Paused state if it's in it to give it a chance to pause again)
	// and set the "overridden-spec" annotation to indicate that we've applied over top the original
	err = unstructured.SetNestedField(newPipelineCopy.Object, string(numaflowv1.PipelinePhaseRunning), "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}
	markPipelineSpecOverridden(newPipelineCopy)

	// Take the difference between this newPipelineCopy spec and the original currentPipeline spec to derive the patch we need and then apply it

	// Create a strategic merge patch by comparing the current pipeline with the new pipeline copy
	// We need to extract just the fields we want to update: spec, metadata.annotations
	patchData := map[string]interface{}{
		"spec": newPipelineCopy.Object["spec"], // we assume we're the only ones who write to the Spec; therefore it's okay to copy the entire thing and use it knowing that nobody else has changed it
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

	// Apply the merge patch to the current pipeline
	err = kubernetes.PatchResource(ctx, c, currentPipeline, string(patchBytes), k8stypes.MergePatchType)
	if err != nil {
		return fmt.Errorf("failed to apply patch to pipeline %s: %w", currentPipeline.GetName(), err)
	}

	numaLogger.WithValues("currentPipeline", currentPipeline.GetName()).Debug("successfully applied patch to pipeline")
	return nil
}

// make sure Pipeline's desiredPhase==Paused and if not set it
// make sure its scale and pauseGracePeriodSeconds is adjusted if necessary
// The scale is reduced to avoid too many pods running for the user.
// The pauseGracePeriodSeconds is increased to allow more time for pausing given the reduction in Pods.
// return:
// - whether phase==Paused
// - whether fully drained
// - whether failed
// - error if any
func drainRecyclablePipeline(
	ctx context.Context,
	pipeline *unstructured.Unstructured,
	pipelineRollout *apiv1.PipelineRollout,
	c client.Client,
) (bool, bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	desiredPhase, err := numaflowtypes.GetPipelineDesiredPhase(pipeline)
	if err != nil {
		return false, false, false, err
	}
	if desiredPhase != string(numaflowv1.PipelinePhasePaused) {
		recycleScaleFactor := getRecycleScaleFactor(pipelineRollout)
		numaLogger.WithValues("scaleFactor", recycleScaleFactor).Debug("scale factor to scale down by during pausing")

		newVertexScaleDefinitions, err := calculateScaleForRecycle(ctx, pipeline, pipelineRollout, recycleScaleFactor)
		if err != nil {
			return false, false, false, err
		}

		newPauseGracePeriodSeconds, err := calculatePauseTimeForRecycle(ctx, pipeline, pipelineRollout, 100.0/float64(recycleScaleFactor))
		if err != nil {
			return false, false, false, err
		}

		// patch the pipeline to update the scale values
		err = applyScaleValuesToLivePipeline(ctx, pipeline, newVertexScaleDefinitions, c)
		if err != nil {
			return false, false, false, err
		}

		// Make sure Numaflow reconciles the scale values before we set desiredPhase=Paused
		// (this is essential to make sure Numaflow will scale vertices > 0 if they were previously at 0)
		pipelineReconciled, generation, observedGeneration, err := numaflowtypes.CheckPipelineLiveObservedGeneration(ctx, pipeline)
		if err != nil {
			return false, false, false, fmt.Errorf("error checking pipeline %s/%s live observed generation: %v", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		if !pipelineReconciled {
			numaLogger.WithValues("generation", generation, "observedGeneration", observedGeneration).Debug("waiting for pipeline observedGeneration to match generation")
			return false, false, false, nil
		}

		// patch the pipeline to set desiredPhase=Paused and set the new pause time
		patchJson := fmt.Sprintf(`{"spec": {"lifecycle": {"desiredPhase": "Paused", "pauseGracePeriodSeconds": %d}}}`, newPauseGracePeriodSeconds)
		numaLogger.WithValues("pipeline", pipeline.GetName(), "patchJson", patchJson).Debug("patching pipeline lifecycle")

		err = kubernetes.PatchResource(ctx, c, pipeline, patchJson, k8stypes.MergePatchType)
		if err != nil {
			return false, false, false, err
		}

		return false, false, false, nil

	} else {
		// return if Pipeline is Paused and if so if it's Drained
		isPaused := numaflowtypes.CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhasePaused)
		isFailed := numaflowtypes.CheckPipelinePhase(ctx, pipeline, numaflowv1.PipelinePhaseFailed)
		isDrained := false
		if isPaused {
			isDrained, _ = numaflowtypes.CheckPipelineDrained(ctx, pipeline)
		}
		return isPaused, isDrained, isFailed, nil
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
	origPauseGracePeriodSeconds, found, err := unstructured.NestedFloat64(pipelineSpec, "lifecycle", "pauseGracePeriodSeconds")
	if err != nil {
		return 0, err
	}
	if !found {
		numaLogger.Debugf("pauseGracePeriodSeconds field not found, will use default %d seconds", defaultPauseGracePeriodSeconds)
		origPauseGracePeriodSeconds = float64(defaultPauseGracePeriodSeconds)
	}
	newPauseGracePeriodSeconds := origPauseGracePeriodSeconds * multiplier
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
		numaLogger.Warnf("HistoricalPodCount is nil for PipelineRollout %s/%s", pipelineRollout.Namespace, pipelineRollout.Name) // note this will happen if progressive upgrade hasn't happened since the storage of this value was introduced
		historicalPodCount = map[string]int{}
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
			} else {

				// If the Vertex was running previously in the "promoted" Pipeline, then multiply by the number that was running then
				originalPodsRunning, found := historicalPodCount[vertexName]
				if found {
					newScaleValue = int64(math.Ceil(float64(originalPodsRunning) * float64(percent) / 100.0))
					numaLogger.WithValues("vertex", vertexName, "newScaleValue", newScaleValue, "originalPodsRunning", originalPodsRunning, "percent", percent).Debug("Setting Vertex Scale value to percent of previous running count")
				} else {
					// This Vertex was not running in the "promoted" Pipeline: the Vertex may be new, or HistoricalPodCount hasn't been stored yet on this Pipeline due to not having done a progressive upgrade since it was introduced
					// We can set the newScaleValue from the PipelineRollout min

					pipelineRolloutVertexDef, found, err := numaflowtypes.GetVertexFromPipelineSpecMap(pipelineRolloutDefinedSpec, vertexName)
					if err != nil {
						return nil, fmt.Errorf("can't calculate scale for vertex %q, error getting vertex from PipelineRollout: %+v", vertexName, pipelineRolloutDefinedSpec)
					}
					if !found {
						// Vertex not found in the PipelineRollout or in the Historical Pod Count
						vertexScaleDef, err := numaflowtypes.ExtractScaleMinMax(vertexAsMap, []string{"scale"})
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
						if err != nil {
							return nil, fmt.Errorf("can't calculate scale for vertex %q, error getting scale.min from PipelineRollout: %+v, err=%v", vertexName, pipelineRolloutDefinedSpec, err)
						}
						if found {
							newScaleValue = int64(floatVal)
							numaLogger.WithValues("vertex", vertexName, "newScaleValue", newScaleValue).Debug("Vertex not found in Historical Pod Count, using PipelineRollout vertex min value")
						} else {
							// If the scale.min wasn't set in PipelineRollout, it is equivalent to 1
							numaLogger.WithValues("vertex", vertexName, "pipelineRolloutDefinedSpec", pipelineRolloutDefinedSpec, "historicalPodCount", historicalPodCount).Debug("Vertex not found in Historical Pod Count, and scale.min not defined in PipelineRollout, so setting newScaleValue to 1")
							newScaleValue = 1
						}
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
	strategy := pipelineRollout.Spec.Strategy
	if strategy != nil && strategy.RecycleStrategy.ScaleFactor != nil {
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

// if the user has set desiredPhase=Paused or any Vertex to scale.max=0, it means the user prefers not to run their Pipeline
// so we should hold off on draining it until they set the PipelineRollout back for running again
// Scale it to zero in the meantime (if it's not)
func checkUserDesiresPause(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, pipeline *unstructured.Unstructured, c client.Client) (bool, error) {
	pipelineSpec, err := numaflowtypes.GetPipelineSpecFromRollout(pipeline.GetName(), pipelineRollout)
	if err != nil {
		return false, err
	}
	pipelineRolloutDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	// use the incoming spec from the PipelineRollout after templating, except replace the InterstepBufferServiceName with the one that's dynamically derived
	pipelineRolloutDef.Object["spec"] = pipelineSpec
	setToRun, err := numaflowtypes.CheckPipelineSetToRun(ctx, pipelineRolloutDef)
	if err != nil {
		return false, err
	}
	if !setToRun {
		err = ensurePipelineScaledToZero(ctx, pipeline, c)
		if err != nil {
			return false, fmt.Errorf("failed to scale pipeline %s/%s to zero: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		return true, nil
	}
	return false, nil
}

func markPipelineSpecOverridden(pipeline *unstructured.Unstructured) {
	annotations := pipeline.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[common.AnnotationKeyOverriddenSpec] = "true"
	pipeline.SetAnnotations(annotations)
}

func isPipelineSpecOverridden(pipeline *unstructured.Unstructured) bool {
	_, found := pipeline.GetAnnotations()[common.AnnotationKeyOverriddenSpec]
	return found
}
