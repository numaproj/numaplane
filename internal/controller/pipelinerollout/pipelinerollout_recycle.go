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
	rolloutObject ctlrcommon.RolloutObject,
	pipeline *unstructured.Unstructured,
) (bool, error) {
	numaLogger := logger.FromContext(ctx).WithValues("pipeline", fmt.Sprintf("%s/%s", pipeline.GetNamespace(), pipeline.GetName()))
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	recyclablePipelineStatus := pipelineRollout.Status.ProgressiveStatus.GetOrCreateRecyclablePipelineStatus(pipeline.GetName())

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

	numaLogger.WithValues("reason", upgradeStateReason, "drainAttemptCount", len(recyclablePipelineStatus.DrainAttempts)).Debug("Recycling Pipeline")

	if !requiresPause {
		r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, false, metrics.LabelValueDrainResult_DrainNotRequired)
		err := kubernetes.DeleteResource(ctx, r.client, pipeline)
		return true, err
	}

	shouldDelete, expired, err := r.shouldDeleteRecyclablePipeline(ctx, pipelineRollout, pipeline)
	if err != nil {
		return false, err
	}
	if shouldDelete {
		numaLogger.Info("Pipeline will be deleted now")
		err = kubernetes.DeleteResource(ctx, r.client, pipeline)
		return true, err
	}
	if expired {
		return false, nil
	}

	// TODO: remove later
	// This is temporary backward compatible code to handle pipelines with older annotations
	if err := migrateForceDrainAnnotationsToDrainAttempts(recyclablePipelineStatus, pipeline, requiresPauseOriginalSpec); err != nil {
		return false, fmt.Errorf("failed to migrate force drain annotations to drain attempts status: %w", err)
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
	originalSpec := !recyclablePipelineStatus.HasForceDrainStarted(pipeline.GetName())

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// if requiresPauseOriginalSpec==true, it means we first attempt to pause with our original spec, and only if that is not able to drain fully, we do force draining
	// if requiresPauseOriginalSpec==false, we are pretty sure the first attempt will fail, so we go straight to force draining
	// In either case, before Force Draining, we need to wait until there's a new promoted Pipeline we can use
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// if the recycling strategy requires pausing with the original spec and we still have the original spec, then
	// make sure we pause it and check on it (skip once that drain attempt has already completed)
	if requiresPauseOriginalSpec && originalSpec && !recyclablePipelineStatus.IsDrainAttemptComplete(pipeline.GetName()) {
		// update the PipelineRollout Status for this recyclable pipeline to add in this DrainAttempt
		if !recyclablePipelineStatus.HasDrainAttempt(pipeline.GetName()) {
			recyclablePipelineStatus.StartDrainAttempt(pipeline.GetName())
		}

		paused, drained, failed, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, r.client, recyclablePipelineStatus, pipeline.GetName())
		if err != nil {
			return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		numaLogger.WithValues("paused", paused, "drained", drained).Debug("checking drain of Pipeline using original spec")
		if failed {
			// look for persistent "Failed" phase: if it's persistent over a period of time, we stop trying to drain
			// if it did fail, we keep track of the time to determine if it's persistent
			recyclablePipelineStatus.SetDrainAttemptFailure(pipeline.GetName())
			nonTransientFailure, err := r.checkForFailedPipeline(ctx, recyclablePipelineStatus, pipeline.GetName())
			if err != nil {
				return false, fmt.Errorf("failed to check for failed pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
			}
			if !nonTransientFailure {
				numaLogger.Debug("Pipeline is in Failed phase; waiting before force drain")
				return false, nil
			}
			recyclablePipelineStatus.CompleteDrainAttempt(pipeline.GetName(), apiv1.DrainCompletionReasonPipelineFailed)
			numaLogger.Debug("Pipeline has been in Failed phase for a period of time; will force drain") // fall through to force draining
		} else {
			// in case there was an intermittent "Failed" phase, unset that failed state now
			// (we require persistent failure in order to decide to stop the drain)
			recyclablePipelineStatus.UnsetDrainAttemptFailure(pipeline.GetName())

			if paused {
				if drained {
					recyclablePipelineStatus.CompleteDrainAttempt(pipeline.GetName(), apiv1.DrainCompletionReasonDrainComplete)
					numaLogger.Info("Pipeline has been drained and will be deleted now")
					err = kubernetes.DeleteResource(ctx, r.client, pipeline)
					r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, true, metrics.LabelValueDrainResult_StandardDrain)

					return true, err
				} else {
					recyclablePipelineStatus.CompleteDrainAttempt(pipeline.GetName(), apiv1.DrainCompletionReasonMaxPauseTime)
				}
			} else {
				return false, nil
			}
		}
	}

	// force drain:
	return r.checkAndPerformForceDrain(ctx, pipeline, pipelineRollout, recyclablePipelineStatus)

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
	recyclablePipelineStatus *apiv1.RecyclablePipelineStatus,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// if no new promoted pipeline, we need to wait: ensure we scale to 0 and return
	promotedPipelineAvailableForDraining := false
	promotedPipeline, err := r.checkForPromotedPipelineForForceDrain(ctx, pipelineRollout)
	if err != nil {
		return false, fmt.Errorf("error checking for promoted pipeline for force drain for PipelineRollout %s/%s: %v", pipelineRollout.Namespace, pipelineRollout.Name, err)
	}

	if promotedPipeline != nil {
		// did we already try and fail to force drain with this promoted pipeline spec?
		if recyclablePipelineStatus.IsDrainAttemptComplete(promotedPipeline.GetName()) {
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
	}

	return r.forceDrain(ctx, pipeline, promotedPipeline, pipelineRollout, recyclablePipelineStatus)
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
	recyclablePipelineStatus *apiv1.RecyclablePipelineStatus,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// if we haven't yet, we need to update with the promoted pipeline's spec
	if !recyclablePipelineStatus.HasDrainAttempt(promotedPipeline.GetName()) {
		numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Info("Found promoted pipeline, will force apply it")
		// update spec with desiredPhase=Running and scaled to 0 initially
		err := forceApplySpecOnUndrainablePipeline(ctx, pipeline, promotedPipeline, recyclablePipelineStatus, r.client)
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
	paused, drained, failed, err := drainRecyclablePipeline(ctx, pipeline, pipelineRollout, r.client, recyclablePipelineStatus, promotedPipeline.GetName())
	if err != nil {
		return false, fmt.Errorf("failed to drain recyclable pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
	}
	numaLogger.WithValues("paused", paused, "drained", drained, "failed", failed).Debug("checking drain of Pipeline using latest promoted pipeline's spec")

	if failed {
		recyclablePipelineStatus.SetDrainAttemptFailure(promotedPipeline.GetName())
		// If the Pipeline failed during force drain, we need to wait some time before deleting it, as there may be transient failures.
		nonTransientFailure, err := r.checkForFailedPipeline(ctx, recyclablePipelineStatus, promotedPipeline.GetName())
		if err != nil {
			return false, fmt.Errorf("failed to check for failed pipeline %s/%s: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
		}
		if nonTransientFailure {
			recyclablePipelineStatus.CompleteDrainAttempt(promotedPipeline.GetName(), apiv1.DrainCompletionReasonPipelineFailed)
		}
	} else {
		// in case there was an intermittent "Failed" phase, unset that failed state now
		// (we require persistent failure in order to decide to stop the drain)
		recyclablePipelineStatus.UnsetDrainAttemptFailure(promotedPipeline.GetName())

		if paused {
			if drained {
				recyclablePipelineStatus.CompleteDrainAttempt(promotedPipeline.GetName(), apiv1.DrainCompletionReasonDrainComplete)
				numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Infof("Pipeline has the promoted pipeline's spec and has fully drained, now deleting it")
				err = kubernetes.DeleteResource(ctx, r.client, pipeline)
				r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, true, metrics.LabelValueDrainResult_ForceDrain)
				return true, err
			}
			recyclablePipelineStatus.CompleteDrainAttempt(promotedPipeline.GetName(), apiv1.DrainCompletionReasonMaxPauseTime)
			numaLogger.WithValues("promotedPipeline", promotedPipeline.GetName()).Infof("Pipeline has the promoted pipeline's spec but was not able to drain")
		}
	}

	return false, nil
}

// check if the Pipeline has been in Failed state for long enough to consider it a permanent failure
// if so, return true; otherwise, return false to indicate we should keep waiting.
func (r *PipelineRolloutReconciler) checkForFailedPipeline(
	ctx context.Context,
	recyclablePipelineStatus *apiv1.RecyclablePipelineStatus,
	drainAttemptSourcePipelineName string,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	drainAttempt := recyclablePipelineStatus.GetDrainAttempt(drainAttemptSourcePipelineName)
	if drainAttempt == nil || drainAttempt.DrainAttemptComplete {
		return false, nil
	}

	waitDurationSeconds := config.GetForceDrainFailureWaitDuration()
	if drainAttempt != nil && drainAttempt.FailedPhaseStartTime != nil &&
		int32(time.Since(drainAttempt.FailedPhaseStartTime.Time).Seconds()) >= waitDurationSeconds {
		numaLogger.Infof("Pipeline has failed for long enough, giving up on current drain attempt")
		return true, nil
	}

	numaLogger.Debug("Pipeline is in Failed phase; waiting before giving up on current drain attempt")
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
	different, err := r.CheckForDifferencesWithRolloutDef(ctx, currentPromotedPipeline, pipelineRollout, common.LabelValueUpgradePromoted)
	if err != nil {
		return nil, err
	}
	if different {
		return nil, nil
	} else {
		return currentPromotedPipeline, nil
	}
}

// getForceAppliedSpec returns a copy of newPipeline with the transformations needed for force draining:
// source vertices scaled to zero, ISBService name preserved from currentPipeline, and desiredPhase set to Running.
func getForceAppliedSpec(ctx context.Context, currentPipeline, newPipelineSpec *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	forceAppliedPipelineSpec := newPipelineSpec.DeepCopy()

	err := numaflowtypes.ScalePipelineDefSourceVerticesToZero(ctx, forceAppliedPipelineSpec)
	if err != nil {
		return nil, err
	}

	// Preserve the current pipeline's ISBService name so we don't change it during force drain (a Pipeline cannot have its ISBService changed or it won't work)
	currentISBSvcName, err := numaflowtypes.GetPipelineISBSVCName(currentPipeline)
	if err != nil {
		return nil, err
	}
	if err := numaflowtypes.PipelineWithISBServiceName(forceAppliedPipelineSpec, currentISBSvcName); err != nil {
		return nil, err
	}

	// Set the desiredPhase to Running just in case it isn't (we need to take it out of Paused state if it's in it to give it a chance to pause again)
	err = unstructured.SetNestedField(forceAppliedPipelineSpec.Object, string(numaflowv1.PipelinePhaseRunning), "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return nil, err
	}

	return forceAppliedPipelineSpec, nil
}

// Update the pipeline to the new spec with min=max=0 initially and set to desiredPhase=Running
// (it will be set to Paused later)
// currentPipeline: the pipeline that will be updated
// newPipeline: spec from the new pipeline which will be applied
func forceApplySpecOnUndrainablePipeline(ctx context.Context, currentPipeline, newPipeline *unstructured.Unstructured, recyclablePipelineStatus *apiv1.RecyclablePipelineStatus, c client.Client) error {
	numaLogger := logger.FromContext(ctx)
	forceAppliedPipeline, err := getForceAppliedSpec(ctx, currentPipeline, newPipeline)
	if err != nil {
		return err
	}

	// Take the difference between this newPipelineCopy spec and the original currentPipeline spec to derive the patch we need and then apply it

	// Create a strategic merge patch by comparing the current pipeline with the new pipeline copy
	// We need to extract just the fields we want to update: spec, metadata.annotations
	patchData := map[string]interface{}{
		"spec": forceAppliedPipeline.Object["spec"], // we assume we're the only ones who write to the Spec; therefore it's okay to copy the entire thing and use it knowing that nobody else has changed it
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

	startForceDrainAttempt(recyclablePipelineStatus, newPipeline.GetName())

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
	recyclablePipelineStatus *apiv1.RecyclablePipelineStatus,
	drainAttemptSourcePipelineName string,
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
		recyclablePipelineStatus.SetDrainAttemptVertexReplicaCount(drainAttemptSourcePipelineName, newVertexScaleDefinitions)

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

// return whether undrained recyclable pipelines should be kept after max recyclable duration;
// uses the PipelineRollout recycle strategy if set, otherwise the controller config value.
func getKeepUndrainedPipelines(pipelineRollout *apiv1.PipelineRollout) bool {
	strategy := pipelineRollout.Spec.Strategy
	if strategy != nil && strategy.RecycleStrategy.KeepUndrainedPipelines != nil {
		return *strategy.RecycleStrategy.KeepUndrainedPipelines
	}

	return config.GetKeepUndrainedPipelines()
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

// startForceDrainAttempt records that a force drain attempt has started.
func startForceDrainAttempt(recyclablePipelineStatus *apiv1.RecyclablePipelineStatus, forceDrainSpecPipeline string) {
	if recyclablePipelineStatus.HasDrainAttempt(forceDrainSpecPipeline) {
		return
	}
	recyclablePipelineStatus.StartDrainAttempt(forceDrainSpecPipeline)
}

// TODO: this is Temporary code to remove later.
// migrateForceDrainAnnotationsToDrainAttempts copies force-drain-specs-started and
// force-drain-specs-completed annotations into RecyclablePipelineStatus.DrainAttempts
// for backward compatibility.
// Idempotent: existing DrainAttempts are not duplicated; DrainCompleted is only set to true from annotations.
func migrateForceDrainAnnotationsToDrainAttempts(
	recyclablePipelineStatus *apiv1.RecyclablePipelineStatus,
	pipeline *unstructured.Unstructured,
	requiresPauseOriginalSpec bool,
) error {
	if recyclablePipelineStatus == nil {
		return nil
	}

	// only migrate if the annotations are set and DrainAttempts is not set
	annotations := pipeline.GetAnnotations()
	if annotations[common.AnnotationKeyForceDrainSpecsStarted] == "" {
		return nil
	}
	if len(recyclablePipelineStatus.DrainAttempts) > 0 {
		return nil
	}

	forceDrainStarted := parseCommaDelimitedAnnotationValues(annotations, common.AnnotationKeyForceDrainSpecsStarted)
	forceDrainCompleted := parseCommaDelimitedAnnotationValues(annotations, common.AnnotationKeyForceDrainSpecsCompleted)
	if len(forceDrainStarted) == 0 && len(forceDrainCompleted) == 0 {
		return nil
	}

	existingBySource := make(map[string]apiv1.DrainAttempt, len(recyclablePipelineStatus.DrainAttempts))
	for _, drainAttempt := range recyclablePipelineStatus.DrainAttempts {
		existingBySource[drainAttempt.SourcePipelineSpec] = drainAttempt
	}

	completedSet := make(map[string]struct{}, len(forceDrainCompleted))
	for _, sourcePipelineName := range forceDrainCompleted {
		completedSet[sourcePipelineName] = struct{}{}
	}

	// Create DrainAttempts array in the same order as they're listed in the AnnotationKeyForceDrainSpecsStarted
	drainAttempts := make([]apiv1.DrainAttempt, 0, len(forceDrainStarted))
	seen := make(map[string]struct{}, len(forceDrainStarted))
	for _, sourcePipelineName := range forceDrainStarted {
		if _, alreadyAdded := seen[sourcePipelineName]; alreadyAdded {
			continue
		}
		seen[sourcePipelineName] = struct{}{}

		drainAttempt, exists := existingBySource[sourcePipelineName]
		if !exists {
			drainAttempt = apiv1.DrainAttempt{SourcePipelineSpec: sourcePipelineName}
		}
		drainAttempts = append(drainAttempts, drainAttempt)
	}

	// if they're in the AnnotationKeyForceDrainSpecsCompleted annotation, we mark them as complete
	for i := range drainAttempts {
		if _, completed := completedSet[drainAttempts[i].SourcePipelineSpec]; completed {
			drainAttempts[i].DrainAttemptComplete = true
		}
	}

	// as long as we have at least one DrainAttempt and assuming that we should've tried to drain with the original, then we assume that we should also add a DrainAttempt for the original drain
	if len(drainAttempts) > 0 && requiresPauseOriginalSpec {
		originalAttempt, exists := existingBySource[pipeline.GetName()]
		if !exists {
			originalAttempt = apiv1.DrainAttempt{SourcePipelineSpec: pipeline.GetName()}
		}
		originalAttempt.DrainAttemptComplete = true
		drainAttempts = append([]apiv1.DrainAttempt{originalAttempt}, drainAttempts...)
	}

	recyclablePipelineStatus.DrainAttempts = drainAttempts
	return nil
}

func parseCommaDelimitedAnnotationValues(annotations map[string]string, annotationKey string) []string {
	value, exists := annotations[annotationKey]
	if !exists || value == "" {
		return nil
	}

	var values []string
	for _, part := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

// shouldDeleteRecyclablePipeline determines if a recyclable pipeline should be deleted immediately
// without going through the drain process.
// Returns shouldDelete (true if the pipeline should be deleted) and expired (true if max recyclable duration was exceeded).
func (r *PipelineRolloutReconciler) shouldDeleteRecyclablePipeline(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
	pipeline *unstructured.Unstructured) (shouldDelete bool, expired bool, err error) {
	numaLogger := logger.FromContext(ctx)

	if pipelineRollout.Spec.Strategy != nil && pipelineRollout.Spec.Strategy.Progressive.ForcePromote {
		// this is a case in which user likely doesn't care about data loss (and also we have no idea if the "promoted" pipeline is any good), so we can just delete the pipeline without draining
		numaLogger.Debug("PipelineRollout strategy implies no concern for data loss so Pipeline will be deleted without draining")
		r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, false, metrics.LabelValueDrainResult_DrainNotRequired)
		return true, false, nil
	}

	// if pipeline doesn't require drain then just delete it
	if pipeline.GetAnnotations() == nil || pipeline.GetAnnotations()[common.AnnotationKeyRequiresDrain] != "true" {
		numaLogger.Debug("Pipeline does not require drain and will be deleted now")
		r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, false, metrics.LabelValueDrainResult_DrainNotRequired)
		return true, false, nil
	}

	expired, err = r.isRecycledPipelineExpired(ctx, pipeline)
	if err != nil {
		return false, false, err
	}
	if expired {
		r.registerFinalDrainStatus(pipelineRollout.Namespace, pipelineRollout.Name, pipeline, false, metrics.LabelValueDrainResult_NeverDrained)
		if getKeepUndrainedPipelines(pipelineRollout) {
			// mark it as recyclable-expired
			upgradeState, upgradeStateReason := ctlrcommon.GetUpgradeState(ctx, r.client, pipeline)
			if upgradeState == nil || *upgradeState != common.LabelValueUpgradeRecyclableExpired {
				if err := ctlrcommon.UpdateUpgradeState(ctx, r.client, common.LabelValueUpgradeRecyclableExpired, upgradeStateReason, pipeline); err != nil {
					return false, true, fmt.Errorf("failed to update pipeline %s/%s upgrade state to recyclable-expired: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
				}
			}
			numaLogger.Warn("Pipeline has reached the max recyclable duration but keepUndrainedPipelines is enabled - will not delete pipeline - user needs to delete manually")
			// make sure it's scaled down completely so it's not consuming resources at least
			err = numaflowtypes.EnsurePipelineScaledToZero(ctx, pipeline, r.client)
			if err != nil {
				return false, true, fmt.Errorf("failed to scale pipeline %s/%s to zero: %w", pipeline.GetNamespace(), pipeline.GetName(), err)
			}
			return false, true, nil
		}
		numaLogger.Warn("Pipeline has reached the max recyclable duration and will be deleted now")
		return true, true, nil
	}

	return false, false, nil
}

// isRecycledPipelineExpired checks if the max recyclable duration has been exceeded.
// Returns true if the pipeline should be deleted due to expiration.
func (r *PipelineRolloutReconciler) isRecycledPipelineExpired(
	ctx context.Context,
	pipeline *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)

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
				Warn("Time expired on force draining")

			return true, nil
		}
	} else {
		numaLogger.Error(errors.New("missing recyclable-start-time annotation"), "Pipeline somehow doesn't have recyclable-start-time annotation??")
		return true, nil
	}

	return false, nil
}
