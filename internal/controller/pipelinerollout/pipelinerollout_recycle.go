package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// Recycle deletes child; returns true if it was in fact deleted
// This implements a function of the RolloutController interface
// For Pipelines, for the Progressive case, we first drain the Pipeline here before we delete it. (For PPND case, it will have been drained prior to this function.)
// And if the Pipeline can't drain by itself (in the case it's unhealthy), we "force drain" it by applying a new spec over top it.
func (r *PipelineRolloutReconciler) Recycle(
	ctx context.Context,
	pipeline *unstructured.Unstructured,
) (bool, error) {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", fmt.Sprintf("%s/%s", pipeline.GetNamespace(), pipeline.GetName()))
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	pipelineRollout, err := numaflowtypes.GetRolloutForPipeline(ctx, r.client, pipeline)
	if err != nil {
		return false, fmt.Errorf("failed to get rollout for pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
	}

	// Need to determine how to delete the pipeline
	// Use the "upgrade-strategy-reason" Label to determine how
	// if upgrade-strategy-reason="delete/recreate", then don't pause at all (it will have already paused if we're in PPND)
	// for the progressive upgrade-strategy-reasons, we drain first
	upgradeState, upgradeStateReason := ctlrcommon.GetUpgradeState(ctx, r.client, pipeline)
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

	numaLogger.WithValues("reason", upgradeStateReason).Debug("Recycling Pipeline")

	if requiresPause {
		if pipelineRollout.Spec.Strategy != nil && pipelineRollout.Spec.Strategy.Progressive.ForcePromote {
			// this is a case in which user likely doesn't care about data loss (and also we have no idea if the "promoted" pipeline is any good), so we can just delete the pipeline without draining
			numaLogger.Debug("PipelineRollout strategy implies no concern for data loss so Pipeline will be deleted without draining")
			requiresPause = false
		}
	}

	if !requiresPause {
		numaLogger.Info("Pipeline will be deleted now")
		err = kubernetes.DeleteResource(ctx, r.client, pipeline)
		return true, err
	}

	// if pipeline doesn't require drain then just delete it
	if pipeline.GetAnnotations() == nil || pipeline.GetAnnotations()[common.AnnotationKeyRequiresDrain] != "true" {
		numaLogger.Info("Pipeline does not require drain and will be deleted now")
		err = kubernetes.DeleteResource(ctx, r.client, pipeline)
		return true, err
	}

	// Get live Pipeline here

	// First check if the PipelineRollout is configured to run
	// If it's configured to be paused or has Vertex.scale.max==0, then we must respect the user's preference not to run
	pauseDesired, err := checkUserDesiresPause(ctx, pipelineRollout, pipeline, r.client)
	if err != nil {
		return false, err
	}
	if pauseDesired {
		numaLogger.Debug("Pipeline is not supposed to run, per definition: will not drain it yet")
		return false, nil
	}

	// Is the pipeline still defined with its original spec or have we overridden it with that of the "promoted" pipeline?
	originalSpec := isPipelineSpecOriginal(pipeline)

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// if requiresPauseOriginalSpec==true, it means we first attempt to pause with our original spec, and only if that is not able to drain fully, we do force draining
	// if requiresPauseOriginalSpec==false, we are pretty sure the first attempt will fail, so we go straight to force draining
	// In either case, before Force Draining, we need to wait until there's a new promoted Pipeline we can use
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// if the recycling strategy requires pausing with the original spec and we still have the original spec, then
	// make sure we pause it and check on it
	if requiresPauseOriginalSpec && originalSpec {
		paused, drained, failed, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, r.client)
		if err != nil {
			return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		numaLogger.WithValues("paused", paused, "drained", drained).Debug("checking drain of Pipeline using original spec")
		if paused {
			if drained {
				numaLogger.Info("Pipeline has been drained and will be deleted now")
				err = kubernetes.DeleteResource(ctx, r.client, pipeline)
				r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, true, metrics.LabelValueDrainResult_StandardDrain)

				return true, err
			} // else implicitly fall through to force draining

		} else if failed {
			numaLogger.Debug("Pipeline is in Failed phase; will force drain") // fall through to force draining
		} else {
			return false, nil
		}
	}

	// force drain:
	return r.checkAndPerformForceDrain(ctx, pipeline, pipelineRollout)

}

// registerFinalDrainStatus issues an Event and a metric for the final drain status of a pipeline
func (r *PipelineRolloutReconciler) registerFinalDrainStatus(namespace, pipelineRolloutName string, pipeline *unstructured.Unstructured, drainComplete bool, drainResult metrics.LabelValueDrainResult) {
	r.customMetrics.IncProgressivePipelineDrains(namespace, pipelineRolloutName, pipeline.GetName(), drainComplete, drainResult)
	eventType := "Normal"
	if !drainComplete {
		eventType = "Warning"
	}
	r.recorder.Eventf(pipeline, eventType, string(drainResult), string(drainResult))
}

// checkAndPerformForceDrain checks if we should be force draining right now, and if so, does.
// Otherwise, it may be that time has expired and we need to delete the pipeline instead; otherwise, we should just scale the pipeline to 0 and wait..
func (r *PipelineRolloutReconciler) checkAndPerformForceDrain(ctx context.Context,
	// the pipeline whose spec will be updated
	pipeline *unstructured.Unstructured,
	// the PipelineRollout parent
	pipelineRollout *apiv1.PipelineRollout,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// Check if the max recyclable duration has been exceeded: if so, delete the pipeline
	recyclableStartTimeStr, err := kubernetes.GetAnnotation(pipeline, common.AnnotationKeyRecyclableStartTime)
	if err != nil {
		return false, fmt.Errorf("failed to get recyclable start time annotation: %w", err)
	}

	if recyclableStartTimeStr != "" {
		recyclableStartTime, err := time.Parse(time.RFC3339, recyclableStartTimeStr)
		if err != nil {
			return false, fmt.Errorf("failed to parse recyclable start time annotation %q on pipeline %s/%s: %w", recyclableStartTimeStr, pipeline.GetNamespace(), pipeline.GetName(), err)
		}

		maxDurationMinutes := config.GetMaxRecyclableDurationMinutes()
		elapsedMinutes := int32(time.Since(recyclableStartTime).Minutes())

		if elapsedMinutes >= maxDurationMinutes {
			numaLogger.WithValues("recyclableStartTime", recyclableStartTime, "elapsedMinutes", elapsedMinutes, "maxDurationMinutes", maxDurationMinutes).
				Info("Time expired on force draining: deleting pipeline now")

			err = kubernetes.DeleteResource(ctx, r.client, pipeline)
			r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, false, metrics.LabelValueDrainResult_NeverDrained)
			return true, err
		}
	}

	// if no new promoted pipeline, we need to wait: ensure we scale to 0 and return
	promotedPipelineAvailableForDraining := false
	promotedPipeline, err := r.checkForPromotedPipelineForForceDrain(ctx, pipelineRollout)
	if err != nil {
		return false, fmt.Errorf("error checking for promoted pipeline for force drain for PipelineRollout %s/%s: %v", pipelineRollout.Namespace, pipelineRollout.Name, err)
	}

	if promotedPipeline != nil {
		// did we already try and fail to force drain with this promoted pipeline spec?
		if checkForValueInCommaDelimitedAnnotation(pipeline, promotedPipeline.GetName(), common.AnnotationKeyForceDrainSpecsCompleted) {
			numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Debug("Promoted pipeline has already been force drained, skipping")
		} else {
			// we either haven't started or at least haven't finished trying to drain with this spec
			promotedPipelineAvailableForDraining = true
		}
	}
	if !promotedPipelineAvailableForDraining {
		numaLogger.Debug("No viable promoted pipeline found for force draining, scaling current pipeline to zero")
		err = numaflowtypes.EnsurePipelineScaledToZero(ctx, pipeline, r.client)
		if err != nil {
			return false, fmt.Errorf("failed to scale pipeline %s/%s to zero: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}

		return false, nil
	} else {
		return r.forceDrain(ctx, pipeline, promotedPipeline, pipelineRollout)
	}
}

// apply a spec that's considered valid (from a promoted pipeline) over top a spec that's not working.
// The new spec will enable it to drain.
// Then pause it.
// Return true if the Pipeline was deleted
func (r *PipelineRolloutReconciler) forceDrain(ctx context.Context,
	// the pipeline whose spec will be updated
	pipeline *unstructured.Unstructured,
	// the definition of the pipeline whose spec will be used
	promotedPipeline *unstructured.Unstructured,
	// the PipelineRollout parent
	pipelineRollout *apiv1.PipelineRollout,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// if we haven't yet, we need to update with the promoted pipeline's spec (we use the annotation to check)
	if !checkForValueInCommaDelimitedAnnotation(pipeline, promotedPipeline.GetName(), common.AnnotationKeyForceDrainSpecsStarted) {
		numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Info("Found promoted pipeline, will force apply it")
		// update spec with desiredPhase=Running and scaled to 0 initially, plus update the annotation to indicate that we've overridden the spec
		err := forceApplySpecOnUndrainablePipeline(ctx, pipeline, promotedPipeline, r.client)
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
	paused, drained, failed, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, r.client)
	if err != nil {
		return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
	}
	numaLogger.WithValues("paused", paused, "drained", drained, "failed", failed).Debug("checking drain of Pipeline using latest promoted pipeline's spec")

	// check if it fully drained or if it went to the maximum pause time
	if paused {
		// patch the annotation to mark that we've completed the drain with this promoted pipeline spec
		err := markPipelineForceDrainCompleted(ctx, r.client, pipeline, promotedPipeline.GetName())
		if err != nil {
			return false, err
		}
		if drained {
			numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Infof("Pipeline has the promoted pipeline's spec and has fully drained, now deleting it")
			err = kubernetes.DeleteResource(ctx, r.client, pipeline)
			r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, true, metrics.LabelValueDrainResult_ForceDrain)
			return true, err
		} else {
			numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Infof("Pipeline has the promoted pipeline's spec but was not able to drain")
		}
	}
	// If the Pipeline failed during force drain, we need to wait some time before deleting it, as there may be transient failures.
	if failed {
		nonTransientFailure, err := r.checkForFailedPipeline(ctx, pipeline)
		if err != nil {
			return false, fmt.Errorf("failed to check for failed pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		if nonTransientFailure {
			// patch the annotation to mark that we've completed the drain with this promoted pipeline spec
			currentVal, _ := kubernetes.GetAnnotation(pipeline, common.AnnotationKeyForceDrainSpecsCompleted)
			err := kubernetes.SetAndPatchAnnotations(ctx, r.client, pipeline, map[string]string{
				common.AnnotationKeyForceDrainSpecsCompleted: currentVal + promotedPipeline.GetName() + ",",
				// reset this so we won't try to look at it on the next force drain attempt
				common.AnnotationKeyForceDrainFailureStartTime: "",
			})
			return false, err
		}
	}

	return false, nil
}

// check if the Pipeline has been in Failed state for long enough to consider it a permanent failure
// if so, delete it; otherwise, return false to indicate we haven't deleted it yet.
// decision: just use original failure start time in the case of Pipeline switching Failed->Running->Failed
func (r *PipelineRolloutReconciler) checkForFailedPipeline(ctx context.Context, pipeline *unstructured.Unstructured) (bool, error) {

	numaLogger := logger.FromContext(ctx)
	currentTime := time.Now()

	// the first time we detect failure, mark the time
	if pipeline.GetAnnotations()[common.AnnotationKeyForceDrainFailureStartTime] == "" {
		if err := kubernetes.SetAndPatchAnnotations(ctx, r.client, pipeline, map[string]string{
			common.AnnotationKeyForceDrainFailureStartTime: currentTime.Format(time.RFC3339),
		}); err != nil {
			return false, fmt.Errorf("failed to set force drain failure start time annotation on pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		return false, nil
	}

	// check if we've waited long enough
	startTime, err := time.Parse(time.RFC3339, pipeline.GetAnnotations()[common.AnnotationKeyForceDrainFailureStartTime])
	if err != nil {
		return false, fmt.Errorf("failed to parse force drain failure start time annotation %q on pipeline %s/%s: %w", startTime, pipeline.GetNamespace(), pipeline.GetName(), err)
	}
	waitDurationSeconds := config.GetForceDrainFailureWaitDuration()
	if int32(currentTime.Sub(startTime).Seconds()) < waitDurationSeconds {
		numaLogger.WithValues("startTime", startTime, "currentTime", currentTime, "waitDuration", waitDurationSeconds).Debug("waiting longer before deleting failed pipeline during force drain")
		return false, nil
	} else {
		numaLogger.Infof("Pipeline has the promoted pipeline's spec and has failed, drain attempt is complete, pipeline definition: %v", kubernetes.GetLoggableResource(pipeline))
		return true, err
	}
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
// currentPipeline: the pipeline that will be updated
// newPipeline: spec from the new pipeline which will be applied
func forceApplySpecOnUndrainablePipeline(ctx context.Context, currentPipeline, newPipeline *unstructured.Unstructured, c client.Client) error {
	numaLogger := logger.FromContext(ctx)
	// take the newPipeline Spec, make a copy, and set any sources to min=max=0
	newPipelineCopy := newPipeline.DeepCopy()
	err := numaflowtypes.ScalePipelineDefSourceVerticesToZero(ctx, newPipelineCopy)
	if err != nil {
		return err
	}

	// Set the desiredPhase to Running just in case it isn't (we need to make to take it out of Paused state if it's in it to give it a chance to pause again)
	// and set the "overridden-spec" annotation to indicate that we've applied over top the original
	err = unstructured.SetNestedField(newPipelineCopy.Object, string(numaflowv1.PipelinePhaseRunning), "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return err
	}

	// Take the difference between this newPipelineCopy spec and the original currentPipeline spec to derive the patch we need and then apply it

	// Create a strategic merge patch by comparing the current pipeline with the new pipeline copy
	// We need to extract just the fields we want to update: spec, metadata.annotations
	patchData := map[string]interface{}{
		"spec": newPipelineCopy.Object["spec"], // we assume we're the only ones who write to the Spec; therefore it's okay to copy the entire thing and use it knowing that nobody else has changed it
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

	// patch the annotation to mark that we've started the force drain with this promoted pipeline spec
	err = markPipelineForceDrainStarted(ctx, c, currentPipeline, newPipeline.GetName())
	if err != nil {
		return err
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
		err = numaflowtypes.ApplyScaleValuesToLivePipeline(ctx, pipeline, newVertexScaleDefinitions, c)
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
					if newScaleValue < 1 {
						newScaleValue = 1
					}
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
					Min:      &newScaleValue,
					Max:      &newScaleValue,
					Disabled: false,
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
	setToRun, err := numaflowtypes.CanPipelineIngestData(ctx, pipelineRolloutDef)
	if err != nil {
		return false, err
	}
	if !setToRun {
		err = numaflowtypes.EnsurePipelineScaledToZero(ctx, pipeline, c)
		if err != nil {
			return false, fmt.Errorf("failed to scale pipeline %s/%s to zero: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		return true, nil
	}
	return false, nil
}

// update the Pipeline's annotation to mark that a new pipeline's spec has been used to update the pipeline
func markPipelineForceDrainStarted(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured, forceDrainSpecPipeline string) error {
	// first check if it's already marked in which case we'll skip this
	if checkForValueInCommaDelimitedAnnotation(pipeline, forceDrainSpecPipeline, common.AnnotationKeyForceDrainSpecsStarted) {
		return nil
	}
	currentVal, _ := kubernetes.GetAnnotation(pipeline, common.AnnotationKeyForceDrainSpecsStarted)

	// Patch in Kubernetes
	return kubernetes.SetAndPatchAnnotations(ctx, c, pipeline, map[string]string{
		common.AnnotationKeyForceDrainSpecsStarted: currentVal + forceDrainSpecPipeline + ",",
	})
}

// update the Pipeline's annotation to mark that a new pipeline's spec has been used to update the pipeline
func markPipelineForceDrainCompleted(ctx context.Context, c client.Client, pipeline *unstructured.Unstructured, forceDrainSpecPipeline string) error {
	// first check if it's already marked in which case we'll skip this
	if checkForValueInCommaDelimitedAnnotation(pipeline, forceDrainSpecPipeline, common.AnnotationKeyForceDrainSpecsCompleted) {
		return nil
	}
	currentVal, _ := kubernetes.GetAnnotation(pipeline, common.AnnotationKeyForceDrainSpecsCompleted)

	// Patch in Kubernetes
	return kubernetes.SetAndPatchAnnotations(ctx, c, pipeline, map[string]string{
		common.AnnotationKeyForceDrainSpecsCompleted: currentVal + forceDrainSpecPipeline + ",",
	})
}

// annotation is comma delimited: check if the value is present
func checkForValueInCommaDelimitedAnnotation(pipeline *unstructured.Unstructured, value string, annotationKey string) bool {
	annotations := pipeline.GetAnnotations()
	if annotations == nil {
		return false
	}

	forceDrainSpecs, exists := annotations[annotationKey]
	if !exists {
		return false
	}

	for _, p := range strings.Split(forceDrainSpecs, ",") {
		if strings.TrimSpace(p) == value {
			return true
		}
	}
	return false
}

// we use an annotation to indicate when we've started force draining with a promoted pipeline's spec
// if the annotation is unset, then the pipeline spec is still the original
func isPipelineSpecOriginal(pipeline *unstructured.Unstructured) bool {
	forceDrainSpecs, found := pipeline.GetAnnotations()[common.AnnotationKeyForceDrainSpecsStarted]
	return !found || forceDrainSpecs == ""
}
