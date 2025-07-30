/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pipelinerollout

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// normal sequence of events when we need to pause:
// - set Pipeline's desiredPhase=Paused
// - wait for Pipeline to become Paused
// - then if we need to update the Pipeline spec, update it
//
// - as long as there's no other requirement to pause, set desiredPhase=Running
// return boolean for whether we can stop the PPND process
func (r *PipelineRolloutReconciler) processExistingPipelineWithPPND(ctx context.Context, pipelineRollout *apiv1.PipelineRollout,
	existingPipelineDef, newPipelineDef *unstructured.Unstructured) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	var newPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(newPipelineDef.Object["spec"], &newPipelineSpec); err != nil {
		return false, fmt.Errorf("failed to convert new Pipeline spec %q into PipelineSpec type, err=%v", newPipelineDef.Object, err)
	}

	pipelineNeedsToUpdate, err := r.pipelineNeedsUpdatingForPPND(ctx, existingPipelineDef, newPipelineDef)
	if err != nil {
		return false, err
	}

	// Should Pipeline be paused? If it should be resumed, should that be forced with no additional checking?
	needsPaused, forceResume, err := r.shouldBePaused(ctx, pipelineRollout, existingPipelineDef, newPipelineDef, pipelineNeedsToUpdate)
	if err != nil {
		return false, err
	}
	if needsPaused == nil { // not enough information to know
		return false, errors.New("not enough information available to know if we need to pause")
	}

	resumed := false
	shouldBePaused := *needsPaused
	if shouldBePaused {
		if err := r.setPipelineLifecyclePaused(ctx, existingPipelineDef); err != nil {
			return false, err
		}
	} else {
		// make sure this is set to running
		if err := r.setPipelineLifecycleRunning(ctx, pipelineRollout, existingPipelineDef, forceResume); err != nil {
			return false, err
		}

		// TODO: I think we should change this so it really checks if it's not paused - if it's failed, this should be okay - unless handled below...
		// now check if it's running
		// this is used below to make sure it's running before we exit PPND strategy
		// (note: this is necessary to prevent overriding of the numaflow.numaproj.io/resume-strategy annotation)
		//resumed = numaflowtypes.CheckPipelinePhase(ctx, existingPipelineDef, numaflowv1.PipelinePhaseRunning)
		resumed = !numaflowtypes.CheckPipelinePhase(ctx, existingPipelineDef, numaflowv1.PipelinePhasePausing) ||
			!numaflowtypes.CheckPipelinePhase(ctx, existingPipelineDef, numaflowv1.PipelinePhasePaused)
	}

	// update the ResourceVersion in the newPipelineDef in case it got updated
	newPipelineDef.SetResourceVersion(existingPipelineDef.GetResourceVersion())

	// if it's safe to Update and we need to, do it now
	if pipelineNeedsToUpdate {
		pipelineRollout.Status.MarkPending()
		if !shouldBePaused || (shouldBePaused && numaflowtypes.CheckPipelinePhase(ctx, existingPipelineDef, numaflowv1.PipelinePhasePaused)) {
			numaLogger.Infof("it's safe to update Pipeline so updating now")
			r.recorder.Eventf(pipelineRollout, "Normal", "PipelineUpdate", "it's safe to update Pipeline so updating now")

			if shouldBePaused {
				err = numaflowtypes.PipelineWithDesiredPhase(newPipelineDef, "Paused")
				if err != nil {
					return false, err
				}
			}
			err = kubernetes.UpdateResource(ctx, r.client, newPipelineDef)
			if err != nil {
				return false, err
			}
			pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
		}
	} else {
		pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
	}

	// are we done with PPND?
	doneWithPPND := resumed

	// but if the PipelineRollout says to pause and we're Paused (or won't pause), stop doing PPND in that case too
	specBasedPause := r.isSpecBasedPause(newPipelineSpec)
	pausedOrWontPause, err := numaflowtypes.IsPipelinePausedOrWontPause(ctx, existingPipelineDef, pipelineRollout, false)
	if err != nil {
		return false, err
	}
	if specBasedPause && pausedOrWontPause {
		doneWithPPND = true
	}

	return doneWithPPND, nil
}

// Does the Pipeline need to be paused?
// This is based on:
//
//	any difference in spec between PipelineRollout and Pipeline, with the exception of lifecycle.desiredPhase field
//	any pause request coming from isbsvc or Numaflow Controller
//	spec says to pause
//
// return:
//   - whether to pause, not to pause, or otherwise unknown (nil)
//   - whether we can't pause due to some requirement
//   - error if any
func (r *PipelineRolloutReconciler) shouldBePaused(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, existingPipelineDef, newPipelineDef *unstructured.Unstructured, pipelineNeedsToUpdate bool) (*bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	var newPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(newPipelineDef.Object["spec"], &newPipelineSpec); err != nil {
		return nil, false, fmt.Errorf("failed to convert new Pipeline spec %v into PipelineSpec type, err=%v", newPipelineDef.Object["spec"], err)
	}

	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil {
		return nil, false, err
	}

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	externalPauseRequest, pauseRequestsKnown, err := r.checkForPauseRequest(ctx, pipelineRollout, isbsvcRollout.Name)
	if err != nil {
		return nil, false, err
	}

	// check to see if the PipelineRollout spec itself says to Pause
	specBasedPause := r.isSpecBasedPause(newPipelineSpec)

	var wontPause bool
	if existingPipelineDef != nil {
		wontPause = numaflowtypes.CheckIfPipelineWontPause(ctx, existingPipelineDef, pipelineRollout)
	}

	ppndPause := (pipelineNeedsToUpdate || externalPauseRequest) && !wontPause
	shouldBePaused := ppndPause || specBasedPause
	numaLogger.Debugf("shouldBePaused=%t, pipelineNeedsToUpdate=%t, externalPauseRequest=%t, specBasedPause=%t, wontPause=%t",
		shouldBePaused, pipelineNeedsToUpdate, externalPauseRequest, specBasedPause, wontPause)

	// if we have incomplete pause request information (i.e. numaflowcontrollerrollout or isbservicerollout not yet reconciled), don't return
	// that it's okay to run
	if !shouldBePaused && !pauseRequestsKnown {
		numaLogger.Debugf("incomplete pause request information")
		return nil, wontPause, nil
	}

	return &shouldBePaused, wontPause, nil
}

// do we need to start the PPND process, if we haven't already?
// this is based on if:
//
//	there's any difference in spec between PipelineRollout and Pipeline
//	any pause request coming from isbsvc or Numaflow Controller
func (r *PipelineRolloutReconciler) needPPND(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, pipelineUpdateRequiringPPND bool) (*bool, error) {
	numaLogger := logger.FromContext(ctx)

	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil {
		return nil, err
	}

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	externalPauseRequest, pauseRequestsKnown, err := r.checkForPauseRequest(ctx, pipelineRollout, isbsvcRollout.Name)
	if err != nil {
		return nil, err
	}

	needPPND := externalPauseRequest || pipelineUpdateRequiringPPND
	numaLogger.Debugf("needPPND=%t, externalPauseRequest=%t, pipelineUpdateRequiringPPND=%t", needPPND, externalPauseRequest, pipelineUpdateRequiringPPND)

	// if we have incomplete pause request information (i.e. numaflowcontrollerrollout or isbservicerollout not yet reconciled), don't return
	// that it's okay not to pause because we don't know for sure
	if !needPPND && !pauseRequestsKnown {
		numaLogger.Debugf("incomplete pause request information")
		return nil, nil
	}

	return &needPPND, nil
}

// Determine if the Pipeline has changed and needs updating
// We need to ignore any field that could be set by Numaplane in the pause-and-drain process as well as any labels or annotations that might be set directly on the Pipeline
// by some Controller
func (r *PipelineRolloutReconciler) pipelineNeedsUpdatingForPPND(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	fromCopy := from.DeepCopy()
	toCopy := to.DeepCopy()
	// remove lifecycle.desiredPhase field from comparison to test for equality
	numaflowtypes.PipelineWithoutDesiredPhase(fromCopy)
	numaflowtypes.PipelineWithoutDesiredPhase(toCopy)

	specsEqual := util.CompareStructNumTypeAgnostic(fromCopy.Object["spec"], toCopy.Object["spec"])
	numaLogger.Debugf("specsEqual: %t, from=%v, to=%v\n",
		specsEqual, fromCopy.Object["spec"], toCopy.Object["spec"])
	// We need to restrict to just looking specifically at the labels and annotations we care about for data loss; otherwise, some platform (such as Numaflow) may set an annotation
	// that we don't want to accidentally concern ourselves with
	metadataRisk := usde.ResourceMetadataHasDataLossRisk(ctx, from, to)
	numaLogger.Debugf("metadataRisk: %t, from=%v, to=%v\n",
		metadataRisk, from.Object["metadata"], to.Object["metadata"])

	return !specsEqual || metadataRisk, nil
}

func (r *PipelineRolloutReconciler) isSpecBasedPause(pipelineSpec numaflowtypes.PipelineSpec) bool {
	return (pipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused) || pipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePausing))
}

// check for all pause requests for this Pipeline (i.e. both from Numaflow Controller and ISBService)
// return:
// - whether there's a pause request
// - whether all pause requests are known
// - error if any
func (r *PipelineRolloutReconciler) checkForPauseRequest(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, isbsvcName string) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	pm := ppnd.GetPauseModule()

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	controllerPauseRequest, found := pm.GetPauseRequest(pm.GetNumaflowControllerKey(pipelineRollout.Namespace))
	if !found {
		numaLogger.Debugf("No pause request found for numaflow controller on namespace %q", pipelineRollout.Namespace)
		return false, false, nil

	}
	controllerRequestsPause := controllerPauseRequest != nil && *controllerPauseRequest

	isbsvcPauseRequest, found := pm.GetPauseRequest(pm.GetISBServiceKey(pipelineRollout.Namespace, isbsvcName))
	if !found {
		numaLogger.Debugf("No pause request found for isbsvc %q on namespace %q", isbsvcName, pipelineRollout.Namespace)
		return false, false, nil
	}
	isbsvcRequestsPause := (isbsvcPauseRequest != nil && *isbsvcPauseRequest)

	return controllerRequestsPause || isbsvcRequestsPause, true, nil
}

// set Pipeline Lifecycle to be paused
func (r *PipelineRolloutReconciler) setPipelineLifecyclePaused(ctx context.Context, existingPipelineDef *unstructured.Unstructured) error {
	numaLogger := logger.FromContext(ctx)
	var existingPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(existingPipelineDef.Object["spec"], &existingPipelineSpec); err != nil {
		return err
	}
	lifeCycleIsPaused := existingPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused)

	if !lifeCycleIsPaused {
		numaLogger.Info("pausing pipeline")
		r.recorder.Eventf(existingPipelineDef, "Normal", "PipelinePause", "pausing pipeline")
		if err := ppnd.GetPauseModule().PausePipeline(ctx, r.client, existingPipelineDef); err != nil {
			return err
		}
	}
	return nil
}

// set Pipeline Lifecycle to be running
func (r *PipelineRolloutReconciler) setPipelineLifecycleRunning(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, existingPipelineDef *unstructured.Unstructured, force bool) error {
	numaLogger := logger.FromContext(ctx)
	var existingPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(existingPipelineDef.Object["spec"], &existingPipelineSpec); err != nil {
		return err
	}
	lifeCycleIsPaused := existingPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused)

	if lifeCycleIsPaused {
		numaLogger.Info("resuming pipeline")
		r.recorder.Eventf(existingPipelineDef, "Normal", "PipelineResume", "resuming pipeline")

		isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
		if err != nil {
			return err
		}

		return ppnd.GetPauseModule().RunPipeline(ctx, r.client, existingPipelineDef, isbsvcRollout.Name, force)
	}
	return nil
}
