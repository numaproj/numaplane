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
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

// TODO: move PPND logic out to its own separate file
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

	pipelineNeedsToUpdate, err := r.ChildNeedsUpdating(ctx, existingPipelineDef, newPipelineDef)
	if err != nil {
		return false, err
	}

	needsPaused, err := r.shouldBePaused(ctx, pipelineRollout, existingPipelineDef, newPipelineDef, pipelineNeedsToUpdate)
	if err != nil {
		return false, err
	}
	if needsPaused == nil { // not enough information to know
		return false, errors.New("not enough information available to know if we need to pause")
	}
	shouldBePaused := *needsPaused
	if err := r.setPipelineLifecycle(ctx, shouldBePaused, existingPipelineDef); err != nil {
		return false, err
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
				err = numaflowtypes.WithDesiredPhase(newPipelineDef, "Paused")
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
	doneWithPPND := !shouldBePaused

	// but if the PipelineRollout says to pause and we're Paused (or won't pause), stop doing PPND in that case too
	specBasedPause := r.isSpecBasedPause(newPipelineSpec)
	if specBasedPause && numaflowtypes.IsPipelinePausedOrWontPause(ctx, existingPipelineDef, pipelineRollout) {
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
// return whether to pause, not to pause, or otherwise unknown
func (r *PipelineRolloutReconciler) shouldBePaused(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, existingPipelineDef, newPipelineDef *unstructured.Unstructured, pipelineNeedsToUpdate bool) (*bool, error) {
	numaLogger := logger.FromContext(ctx)

	var newPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(newPipelineDef.Object["spec"], &newPipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to convert new Pipeline spec %v into PipelineSpec type, err=%v", newPipelineDef.Object["spec"], err)
	}
	var existingPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(existingPipelineDef.Object["spec"], &existingPipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to convert existing Pipeline spec %v into PipelineSpec type, err=%v", existingPipelineDef.Object["spec"], err)
	}

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	externalPauseRequest, pauseRequestsKnown, err := r.checkForPauseRequest(ctx, pipelineRollout, newPipelineSpec.GetISBSvcName())
	if err != nil {
		return nil, err
	}

	// check to see if the PipelineRollout spec itself says to Pause
	specBasedPause := r.isSpecBasedPause(newPipelineSpec)

	wontPause := numaflowtypes.CheckIfPipelineWontPause(ctx, existingPipelineDef, pipelineRollout)

	ppndPause := (pipelineNeedsToUpdate || externalPauseRequest) && !wontPause
	shouldBePaused := ppndPause || specBasedPause
	numaLogger.Debugf("shouldBePaused=%t, pipelineNeedsToUpdate=%t, externalPauseRequest=%t, specBasedPause=%t, wontPause=%t",
		shouldBePaused, pipelineNeedsToUpdate, externalPauseRequest, specBasedPause, wontPause)

	// if we have incomplete pause request information (i.e. numaflowcontrollerrollout or isbservicerollout not yet reconciled), don't return
	// that it's okay to run
	if !shouldBePaused && !pauseRequestsKnown {
		numaLogger.Debugf("incomplete pause request information")
		return nil, nil
	}

	return &shouldBePaused, nil
}

// do we need to start the PPND process, if we haven't already?
// this is based on if:
//
//	there's any difference in spec between PipelineRollout and Pipeline
//	any pause request coming from isbsvc or Numaflow Controller
func (r *PipelineRolloutReconciler) needPPND(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, newPipelineDef *unstructured.Unstructured, pipelineUpdateRequiringPPND bool) (*bool, error) {
	numaLogger := logger.FromContext(ctx)

	var newPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(newPipelineDef.Object["spec"], &newPipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to convert new Pipeline spec %v into PipelineSpec type, err=%v", newPipelineDef.Object, err)
	}

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	externalPauseRequest, pauseRequestsKnown, err := r.checkForPauseRequest(ctx, pipelineRollout, newPipelineSpec.GetISBSvcName())
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

// make sure our Pipeline's Lifecycle is what we need it to be
func (r *PipelineRolloutReconciler) setPipelineLifecycle(ctx context.Context, pause bool, existingPipelineDef *unstructured.Unstructured) error {
	numaLogger := logger.FromContext(ctx)
	var existingPipelineSpec numaflowtypes.PipelineSpec
	if err := util.StructToStruct(existingPipelineDef.Object["spec"], &existingPipelineSpec); err != nil {
		return err
	}
	lifeCycleIsPaused := existingPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused)

	if pause && !lifeCycleIsPaused {
		numaLogger.Info("pausing pipeline")
		r.recorder.Eventf(existingPipelineDef, "Normal", "PipelinePause", "pausing pipeline")
		if err := ppnd.GetPauseModule().PausePipeline(ctx, r.client, existingPipelineDef); err != nil {
			return err
		}
	} else if !pause && lifeCycleIsPaused {
		numaLogger.Info("resuming pipeline")
		r.recorder.Eventf(existingPipelineDef, "Normal", "PipelineResume", "resuming pipeline")

		run, err := ppnd.GetPauseModule().RunPipelineIfSafe(ctx, r.client, existingPipelineDef)
		if err != nil {
			return err
		}
		if !run {
			numaLogger.Infof("new pause request, can't resume pipeline at this time, will try again later")
			r.recorder.Eventf(existingPipelineDef, "Normal", "PipelineResumeFailed", "new pause request, can't resume pipeline at this time, will try again later")
		}
	}
	return nil
}
