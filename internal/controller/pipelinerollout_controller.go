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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerPipelineRollout = "pipeline-rollout-controller"
	loggerName                = "pipelinerollout-reconciler"
	numWorkers                = 16 // can consider making configurable
)

var (
	pipelineROReconciler *PipelineRolloutReconciler
	initTime             time.Time
)

// PipelineSpec keep track of minimum number of fields we need to know about
type PipelineSpec struct {
	InterStepBufferServiceName string    `json:"interStepBufferServiceName"`
	Lifecycle                  lifecycle `json:"lifecycle,omitempty"`
}

type lifecycle struct {
	// DesiredPhase used to bring the pipeline from current phase to desired phase
	// +kubebuilder:default=Running
	// +optional
	DesiredPhase string `json:"desiredPhase,omitempty"`
}

// PipelineRolloutReconciler reconciles a PipelineRollout object
type PipelineRolloutReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	restConfig *rest.Config

	// queue contains the list of PipelineRollouts that currently need to be reconciled
	// both PipelineRolloutReconciler.Reconcile() and other Rollout reconcilers can add PipelineRollouts to this queue to be processed as needed
	// a set of Workers is used to process this queue
	queue workqueue.RateLimitingInterface
	// shutdownWorkerWaitGroup is used when shutting down the workers processing the queue for them to indicate that they're done
	shutdownWorkerWaitGroup *sync.WaitGroup
	// customMetrics is used to generate the custom metrics for the Pipeline
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder
}

func NewPipelineRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *PipelineRolloutReconciler {

	numaLogger := logger.GetBaseLogger().WithName(loggerName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx := logger.WithLogger(context.Background(), numaLogger)

	// create a queue to process PipelineRollout reconciliations
	// the benefit of the queue is that other reconciliation code can also add PipelineRollouts to it so they'll be processed
	pipelineRolloutQueue := util.NewWorkQueue("pipeline_rollout_queue")

	r := &PipelineRolloutReconciler{
		client,
		s,
		restConfig,
		pipelineRolloutQueue,
		&sync.WaitGroup{},
		customMetrics,
		recorder,
	}
	pipelineROReconciler = r

	r.runWorkers(ctx)

	return r
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=pipelinerollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=pipelinerollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=pipelinerollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *PipelineRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	numaLogger := logger.GetBaseLogger().WithName(loggerName).WithValues("pipelinerollout", req.NamespacedName)
	r.enqueuePipeline(req.NamespacedName)
	numaLogger.Debugf("PipelineRollout Reconciler added PipelineRollout %v to queue", req.NamespacedName)
	r.customMetrics.PipelineRolloutQueueLength.WithLabelValues().Set(float64(r.queue.Len()))
	return ctrl.Result{}, nil
}

func (r *PipelineRolloutReconciler) enqueuePipeline(namespacedName k8stypes.NamespacedName) {
	key := namespacedNameToKey(namespacedName)
	r.queue.Add(key)
}

func (r *PipelineRolloutReconciler) processPipelineRollout(ctx context.Context, namespacedName k8stypes.NamespacedName) (ctrl.Result, error) {
	syncStartTime := time.Now()
	numaLogger := logger.FromContext(ctx).WithValues("pipelinerollout", namespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)
	r.customMetrics.PipelinesSynced.WithLabelValues().Inc()

	// Get PipelineRollout CR
	pipelineRollout := &apiv1.PipelineRollout{}
	if err := r.client.Get(ctx, namespacedName, pipelineRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(pipelineRollout, err, "GetPipelineRolloutFailed", "Failed to get PipelineRollout")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	pipelineRolloutOrig := pipelineRollout
	pipelineRollout = pipelineRolloutOrig.DeepCopy()

	pipelineRollout.Status.Init(pipelineRollout.Generation)

	requeue, err := r.reconcile(ctx, pipelineRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(pipelineRollout, err, "ReconcileFailed", "Failed to reconcile PipelineRollout")
		statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// Update PipelineRollout Status based on child resource (Pipeline) Status
	err = r.processPipelineStatus(ctx, pipelineRollout)
	if err != nil {
		r.ErrorHandler(pipelineRollout, err, "ProcessPipelineStatusFailed", "Failed to process Pipeline Status")
		statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(pipelineRolloutOrig, pipelineRollout) {
		pipelineRolloutStatus := pipelineRollout.Status
		if err := r.client.Update(ctx, pipelineRollout); err != nil {
			r.ErrorHandler(pipelineRollout, err, "UpdateFailed", "Failed to update PipelineRollout")
			statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		pipelineRollout.Status = pipelineRolloutStatus
	}

	// Update the Status subresource
	if pipelineRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updatePipelineRolloutStatus(ctx, pipelineRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate the metrics for the Pipeline.
	r.customMetrics.IncPipelinesRunningMetrics(pipelineRollout.Name, pipelineRollout.Namespace)

	if requeue {
		return ctrl.Result{Requeue: true, RequeueAfter: 30 * time.Second}, nil
	}

	r.recorder.Eventf(pipelineRollout, "Normal", "ReconcileSuccess", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

	return ctrl.Result{}, nil
}

func (r *PipelineRolloutReconciler) Shutdown(ctx context.Context) {
	numaLogger := logger.FromContext(ctx)

	numaLogger.Info("shutting down PipelineRollout queue")
	r.queue.ShutDown()

	// wait for all the workers to have stopped
	r.shutdownWorkerWaitGroup.Wait()
}

// runWorkers starts up the workers processing the queue of PipelineRollouts
func (r *PipelineRolloutReconciler) runWorkers(ctx context.Context) {

	for i := 0; i < numWorkers; i++ {
		r.shutdownWorkerWaitGroup.Add(numWorkers)
		go r.runWorker(ctx)
	}
}

// runWorker starts up one of the workers processing the queue of PipelineRollouts
func (r *PipelineRolloutReconciler) runWorker(ctx context.Context) {
	numaLogger := logger.FromContext(ctx)

	for {
		key, quit := r.queue.Get()
		if quit {
			numaLogger.Info("PipelineRollout worker done")
			r.shutdownWorkerWaitGroup.Done()
			return
		}
		r.processQueueKey(ctx, key.(string))
		r.queue.Done(key)
	}

}

func (r *PipelineRolloutReconciler) processQueueKey(ctx context.Context, key string) {
	numaLogger := logger.FromContext(ctx).WithValues("key", key)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	// get namespace/name from key
	namespacedName, err := keyToNamespacedName(key)
	if err != nil {
		numaLogger.Fatal(err, "Queue key not derivable")
	}

	numaLogger.Debugf("processing PipelineRollout %v", namespacedName)
	result, err := r.processPipelineRollout(ctx, namespacedName)

	// based on result, may need to add this back to the queue
	if err != nil {
		numaLogger.Errorf(err, "PipelineRollout %v reconcile returned error: %v", namespacedName, err)
		r.queue.AddRateLimited(key)
	} else {
		if result.Requeue {
			numaLogger.Debugf("PipelineRollout %v reconcile requests requeue", namespacedName)
			r.queue.AddRateLimited(key)
		} else if result.RequeueAfter > 0 {
			numaLogger.Debugf("PipelineRollout %v reconcile requests requeue after %d seconds", namespacedName, result.RequeueAfter)
			r.queue.AddAfter(key, result.RequeueAfter)
		} else {
			numaLogger.Debugf("PipelineRollout %v reconcile complete", namespacedName)
		}
	}
}

func keyToNamespacedName(key string) (k8stypes.NamespacedName, error) {
	index := strings.Index(key, "/")
	if index < 0 {
		return k8stypes.NamespacedName{}, fmt.Errorf("improperly formatted key: %q", key)
	}
	return k8stypes.NamespacedName{Namespace: key[0:index], Name: key[index+1:]}, nil
}

func namespacedNameToKey(namespacedName k8stypes.NamespacedName) string {
	return fmt.Sprintf("%s/%s", namespacedName.Namespace, namespacedName.Name)
}

// reconcile does the real logic, it returns true if the event
// needs to be re-queued.
func (r *PipelineRolloutReconciler) reconcile(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
	syncStartTime time.Time,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	// is PipelineRollout being deleted? need to remove the finalizer, so it can
	// (OwnerReference will delete the underlying Pipeline through Cascading deletion)
	if !pipelineRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting PipelineRollout")
		if controllerutil.ContainsFinalizer(pipelineRollout, finalizerName) {
			controllerutil.RemoveFinalizer(pipelineRollout, finalizerName)
		}
		// generate the metrics for the Pipeline deletion.
		r.customMetrics.DecPipelineMetrics(pipelineRollout.Name, pipelineRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "delete").Observe(time.Since(syncStartTime).Seconds())
		return false, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(pipelineRollout, finalizerName) {
		controllerutil.AddFinalizer(pipelineRollout, finalizerName)
	}

	newPipelineDef, err := makePipelineDefinition(pipelineRollout)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	existingPipelineDef, err := kubernetes.GetCR(ctx, r.restConfig, newPipelineDef, "pipelines")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {

			pipelineRollout.Status.MarkPending()

			numaLogger.Debugf("Pipeline %s/%s doesn't exist so creating", pipelineRollout.Namespace, pipelineRollout.Name)
			err = kubernetes.CreateCR(ctx, r.restConfig, newPipelineDef, "pipelines")
			if err != nil {
				return false, err
			}
			pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "create").Observe(time.Since(syncStartTime).Seconds())
			return false, nil
		}

		return false, fmt.Errorf("error getting Pipeline: %v", err)
	}

	// Object already exists
	// if Pipeline is not owned by Rollout, fail and return
	if !checkOwnerRef(existingPipelineDef.OwnerReferences, pipelineRollout.UID) {
		pipelineRollout.Status.MarkFailed(fmt.Sprintf("Pipeline %s already exists in namespace", pipelineRollout.Name))
		return false, nil
	}
	newPipelineDef = mergePipeline(existingPipelineDef, newPipelineDef)
	err = r.processExistingPipeline(ctx, pipelineRollout, existingPipelineDef, newPipelineDef, syncStartTime)
	return false, err
}

// determine if this Pipeline is owned by this PipelineRollout
func checkOwnerRef(ownerRefs []metav1.OwnerReference, uid k8stypes.UID) bool {
	// no owners
	if len(ownerRefs) == 0 {
		return false
	}
	for _, ref := range ownerRefs {
		if ref.Kind == "PipelineRollout" && ref.UID == uid {
			return true
		}
	}
	return false
}

// take the existing pipeline and merge anything needed from the new pipeline definition
func mergePipeline(existingPipeline *kubernetes.GenericObject, newPipeline *kubernetes.GenericObject) *kubernetes.GenericObject {
	resultPipeline := existingPipeline.DeepCopy()
	resultPipeline.Spec = *newPipeline.Spec.DeepCopy()
	resultPipeline.Labels = maps.Clone(newPipeline.Labels)
	return resultPipeline
}
func (r *PipelineRolloutReconciler) processExistingPipeline(ctx context.Context, pipelineRollout *apiv1.PipelineRollout,
	existingPipelineDef *kubernetes.GenericObject, newPipelineDef *kubernetes.GenericObject, syncStartTime time.Time) error {

	// Get the fields we need from both the Pipeline spec we have and the one we want
	// TODO: consider having one struct which include our GenericObject plus our PipelineSpec so we can avoid multiple repeat conversions

	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(existingPipelineDef.Spec.Raw, &existingPipelineSpec); err != nil {
		return fmt.Errorf("failed to convert existing Pipeline spec %q into PipelineSpec type, err=%v", string(existingPipelineDef.Spec.Raw), err)
	}

	// Does pipeline spec need to be updated?
	pipelineNeedsToUpdate, err := pipelineNeedsUpdating(ctx, newPipelineDef, existingPipelineDef)
	if err != nil {
		return err
	}

	// set the Status appropriately to "Pending" or "Deployed"
	// if pipelineNeedsToUpdate - this means there's a mismatch between the desired Pipeline spec and actual Pipeline spec
	if pipelineNeedsToUpdate {
		pipelineRollout.Status.MarkPending()
	} else {
		pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
	}

	if common.DataLossPrevention { // feature flag
		if err = r.processExistingPipelineWithoutDataLoss(ctx, pipelineRollout, existingPipelineDef, newPipelineDef, pipelineNeedsToUpdate); err != nil {
			return err
		}
	} else if pipelineNeedsToUpdate {
		if err := updatePipelineSpec(ctx, r.restConfig, newPipelineDef); err != nil {
			return err
		}
		pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
	}

	// generate update metrics only if we actually updated the Pipeline
	if pipelineNeedsToUpdate {
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "update").Observe(time.Since(syncStartTime).Seconds())
	}
	return nil
}

// normal sequence of events when we need to pause:
// - set Pipeline's desiredPhase=Paused
// - wait for the desire to Pause to be reconciled completely
// - if we need to update the Pipeline spec:
//   - update it
//   - wait for the spec update to be reconciled completely
//
// - as long as there's no other requirement to pause, set desiredPhase=Running
func (r *PipelineRolloutReconciler) processExistingPipelineWithoutDataLoss(ctx context.Context, pipelineRollout *apiv1.PipelineRollout,
	existingPipelineDef, newPipelineDef *kubernetes.GenericObject, pipelineNeedsToUpdate bool) error {

	numaLogger := logger.FromContext(ctx)

	shouldBePaused, err := r.shouldBePaused(ctx, pipelineRollout, existingPipelineDef, newPipelineDef, pipelineNeedsToUpdate)
	if err != nil {
		return err
	}
	if shouldBePaused == nil { // not enough information to know
		return nil
	}

	if err = r.setPipelineLifecycle(ctx, *shouldBePaused, existingPipelineDef); err != nil {
		return err
	}

	// if it's safe to Update and we need to, do it now
	if pipelineNeedsToUpdate {
		if !*shouldBePaused || (*shouldBePaused && isPipelinePausedOrUnpausible(ctx, existingPipelineDef)) {
			numaLogger.Infof("it's safe to update Pipeline so updating now")
			r.recorder.Eventf(pipelineRollout, "Normal", "PipelineUpdate", "it's safe to update Pipeline so updating now")
			// make sure lifecycle is left set to "Paused" in the new spec
			unstruc, err := kubernetes.ObjectToUnstructured(newPipelineDef)
			if err != nil {
				return err
			}
			err = unstructured.SetNestedField(unstruc.Object, "Paused", "spec", "lifecycle", "desiredPhase")
			if err != nil {
				return err
			}
			err = kubernetes.UpdateUnstructuredCR(ctx, r.restConfig, unstruc, common.PipelineGVR, pipelineRollout.Namespace, pipelineRollout.Name)
			if err != nil {
				return err
			}
			pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
		}
	}
	return nil
}

// return whether to pause, not to pause, or otherwise unknown
func (r *PipelineRolloutReconciler) shouldBePaused(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, existingPipelineDef, newPipelineDef *kubernetes.GenericObject, pipelineNeedsToUpdate bool) (*bool, error) {
	numaLogger := logger.FromContext(ctx)

	var newPipelineSpec PipelineSpec
	if err := json.Unmarshal(newPipelineDef.Spec.Raw, &newPipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to convert new Pipeline spec %q into PipelineSpec type, err=%v", string(newPipelineDef.Spec.Raw), err)
	}
	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(existingPipelineDef.Spec.Raw, &existingPipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to convert existing Pipeline spec %q into PipelineSpec type, err=%v", string(existingPipelineDef.Spec.Raw), err)
	}

	// is the Pipeline currently being reconciled?
	pipelineUpdating, err := pipelineIsUpdating(newPipelineDef, existingPipelineDef)
	if err != nil {
		return nil, err
	}

	// is the Pipeline currently being reconciled while our desiredPhase==Paused?
	// only in this circumstance do we need to make sure to remain Paused until that reconciliation is complete
	existingPipelinePauseDesired := existingPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused)
	pipelineUpdating = pipelineUpdating && existingPipelinePauseDesired

	numaLogger.Debugf("pipelineNeedsToUpdate: %t, pipelineUpdating: %t", pipelineNeedsToUpdate, pipelineUpdating)

	// If there is a need to update, does it require a pause?
	// Here we look for whether we are trying to update Pipeline, or whether we are already processing an update which required us to pause
	var pipelineUpdateRequiresPause bool
	if pipelineNeedsToUpdate || pipelineUpdating {
		pipelineUpdateRequiresPause, err = needsPausing(existingPipelineDef, newPipelineDef)
		if err != nil {
			return nil, err
		}
	}

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	externalPauseRequest, pauseRequestsKnown, err := r.checkForPauseRequest(ctx, pipelineRollout, getISBSvcName(newPipelineSpec))
	if err != nil {
		return nil, err
	}
	if !pauseRequestsKnown {
		numaLogger.Debugf("incomplete pause request information")
		return nil, nil
	}

	// check to see if the PipelineRollout spec itself says to Pause
	specBasedPause := (newPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused) || newPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePausing))

	unpausible := checkPipelineStatus(ctx, existingPipelineDef, numaflowv1.PipelinePhaseFailed)

	shouldBePaused := (pipelineUpdateRequiresPause || externalPauseRequest || specBasedPause) && !unpausible
	numaLogger.Debugf("shouldBePaused=%t, pipelineUpdateRequiresPause=%t, externalPauseRequest=%t, specBasedPause=%t, unpausible=%t",
		shouldBePaused, pipelineUpdateRequiresPause, externalPauseRequest, specBasedPause, unpausible)

	return &shouldBePaused, nil
}

func pipelineNeedsUpdating(ctx context.Context, newPipelineDef *kubernetes.GenericObject, existingPipelineDef *kubernetes.GenericObject) (bool, error) {
	// Does pipeline spec need to be updated?
	pipelineSpecsEqual, err := pipelineSpecEqual(ctx, existingPipelineDef, newPipelineDef)
	if err != nil {
		return false, err
	}
	return !pipelineSpecsEqual, nil
}

// return true if Pipeline (or its children) is still in the process of being reconciled
func pipelineIsUpdating(newPipelineDef *kubernetes.GenericObject, existingPipelineDef *kubernetes.GenericObject) (bool, error) {
	existingPipelineStatus, err := kubernetes.ParseStatus(existingPipelineDef)
	if err != nil {
		return false, err
	}
	// if Pipeline's ObservedGeneration is old, then Numaflow Controller hasn't even seen the generation change yet
	if !pipelineObservedGenerationCurrent(newPipelineDef.Generation, existingPipelineStatus.ObservedGeneration) {
		return true, nil
	}

	// note if Pipeline's children are still being updated
	unhealthyOrProgressing, _ := checkChildResources(existingPipelineStatus.Conditions, func(c metav1.Condition) bool {
		// TODO: once we have "inProgressStrategy" we can just look for c.Status == metav1.ConditionFalse while inProgressStrategy==PPND
		return c.Reason == "Progressing" || c.Reason == "GetDaemonServiceFailed" //return c.Status == metav1.ConditionFalse
	})

	return unhealthyOrProgressing, nil

}

// make sure our Pipeline's Lifecycle is what we need it to be
func (r *PipelineRolloutReconciler) setPipelineLifecycle(ctx context.Context, pause bool, existingPipelineDef *kubernetes.GenericObject) error {
	numaLogger := logger.FromContext(ctx)
	var existingPipelineSpec PipelineSpec
	if err := json.Unmarshal(existingPipelineDef.Spec.Raw, &existingPipelineSpec); err != nil {
		return err
	}
	lifeCycleIsPaused := existingPipelineSpec.Lifecycle.DesiredPhase == string(numaflowv1.PipelinePhasePaused)

	if pause && !lifeCycleIsPaused {
		numaLogger.Info("pausing pipeline")
		r.recorder.Eventf(existingPipelineDef, "Normal", "PipelinePause", "pausing pipeline")
		if err := GetPauseModule().pausePipeline(ctx, r.restConfig, existingPipelineDef); err != nil {
			return err
		}
	} else if !pause && lifeCycleIsPaused {
		numaLogger.Info("resuming pipeline")
		r.recorder.Eventf(existingPipelineDef, "Normal", "PipelineResume", "resuming pipeline")

		run, err := GetPauseModule().runPipelineIfSafe(ctx, r.restConfig, existingPipelineDef)
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

// check for all pause requests for this Pipeline (i.e. both from Numaflow Controller and ISBService)
// return:
// - whether there's a pause request
// - whether all pause requests are known
// - error if any
func (r *PipelineRolloutReconciler) checkForPauseRequest(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, isbsvcName string) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	pm := GetPauseModule()

	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	controllerPauseRequest, found := pm.getPauseRequest(pm.getNumaflowControllerKey(pipelineRollout.Namespace))
	if !found {
		numaLogger.Debugf("No pause request found for numaflow controller on namespace %q", pipelineRollout.Namespace)
		return false, false, nil

	}
	controllerRequestsPause := controllerPauseRequest != nil && *controllerPauseRequest

	isbsvcPauseRequest, found := pm.getPauseRequest(pm.getISBServiceKey(pipelineRollout.Namespace, isbsvcName))
	if !found {
		numaLogger.Debugf("No pause request found for isbsvc %q on namespace %q", isbsvcName, pipelineRollout.Namespace)
		return false, false, nil
	}
	isbsvcRequestsPause := (isbsvcPauseRequest != nil && *isbsvcPauseRequest)

	return controllerRequestsPause || isbsvcRequestsPause, true, nil
}

func pipelineObservedGenerationCurrent(generation int64, observedGeneration int64) bool {
	return generation <= observedGeneration
}

// Set the Condition in the Status for child resource health

func (r *PipelineRolloutReconciler) processPipelineStatus(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	numaLogger := logger.FromContext(ctx)

	pipelineDef, err := makePipelineDefinition(pipelineRollout)
	if err != nil {
		return err
	}

	// Get existing Pipeline
	existingPipelineDef, err := kubernetes.GetCR(ctx, r.restConfig, pipelineDef, "pipelines")
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.WithValues("pipelineDefinition", *pipelineDef).Warn("Pipeline not found. Unable to process status during this reconciliation.")
		} else {
			return fmt.Errorf("error getting Pipeline for status processing: %v", err)
		}
	}

	pipelineStatus, err := kubernetes.ParseStatus(existingPipelineDef)
	if err != nil {
		return fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", existingPipelineDef, err)
	}

	numaLogger.Debugf("pipeline status: %+v", pipelineStatus)

	r.setChildResourcesHealthCondition(pipelineRollout, existingPipelineDef, &pipelineStatus)
	r.setChildResourcesPauseCondition(pipelineRollout, existingPipelineDef, &pipelineStatus)

	return nil
}

func (r *PipelineRolloutReconciler) setChildResourcesHealthCondition(pipelineRollout *apiv1.PipelineRollout, pipeline *kubernetes.GenericObject, pipelineStatus *kubernetes.GenericStatus) {

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)
	pipelineChildResourceStatus, pipelineChildResourceReason := getPipelineChildResourceHealth(pipelineStatus.Conditions)

	if pipelineChildResourceReason == "Progressing" || !pipelineObservedGenerationCurrent(pipeline.Generation, pipelineStatus.ObservedGeneration) {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("Progressing", "Pipeline Progressing", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhaseFailed {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Phase=Failed", pipelineRollout.Generation)
	} else if pipelineChildResourceStatus == "False" {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Failed, Pipeline Child Resource(s) Unhealthy", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhasePaused || pipelinePhase == numaflowv1.PipelinePhasePausing {
		pipelineRollout.Status.MarkChildResourcesHealthUnknown("PipelineUnknown", "Pipeline Pausing - health unknown", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhaseDeleting {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineDeleting", "Pipeline Deleting", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhaseUnknown || pipelineChildResourceStatus == "Unknown" {
		pipelineRollout.Status.MarkChildResourcesHealthUnknown("PipelineUnknown", "Pipeline Phase Unknown", pipelineRollout.Generation)
	} else {
		pipelineRollout.Status.MarkChildResourcesHealthy(pipelineRollout.Generation)
	}

}

func (r *PipelineRolloutReconciler) setChildResourcesPauseCondition(pipelineRollout *apiv1.PipelineRollout, pipeline *kubernetes.GenericObject, pipelineStatus *kubernetes.GenericStatus) {

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)

	if pipelinePhase == numaflowv1.PipelinePhasePaused || pipelinePhase == numaflowv1.PipelinePhasePausing {
		// if BeginTime hasn't been set yet, we must have just started pausing - set it
		if pipelineRollout.Status.PauseStatus.LastPauseBeginTime == metav1.NewTime(initTime) || !pipelineRollout.Status.PauseStatus.LastPauseBeginTime.After(pipelineRollout.Status.PauseStatus.LastPauseEndTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseBeginTime = metav1.NewTime(time.Now())
		}
		reason := fmt.Sprintf("Pipeline%s", string(pipelinePhase))
		msg := fmt.Sprintf("Pipeline %s", strings.ToLower(string(pipelinePhase)))
		r.updatePauseMetric(pipelineRollout)
		pipelineRollout.Status.MarkPipelinePausingOrPaused(reason, msg, pipelineRollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		if (pipelineRollout.Status.PauseStatus.LastPauseBeginTime != metav1.NewTime(initTime)) && !pipelineRollout.Status.PauseStatus.LastPauseEndTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseEndTime = metav1.NewTime(time.Now())
			r.updatePauseMetric(pipelineRollout)
		}
		pipelineRollout.Status.MarkPipelineUnpaused(pipelineRollout.Generation)
	}

}

func (r *PipelineRolloutReconciler) updatePauseMetric(pipelineRollout *apiv1.PipelineRollout) {
	timeElapsed := time.Since(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time)
	r.customMetrics.PipelinePausedSeconds.WithLabelValues(pipelineRollout.Name).Set(timeElapsed.Seconds())
}

func (r *PipelineRolloutReconciler) needsUpdate(old, new *apiv1.PipelineRollout) bool {
	if old == nil {
		return true
	}

	// check for any fields we might update in the Spec - generally we'd only update a Finalizer or maybe something in the metadata
	if !equality.Semantic.DeepEqual(old.Finalizers, new.Finalizers) {
		return true
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {

	controller, err := runtimecontroller.New(ControllerPipelineRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch PipelineRollouts
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.PipelineRollout{}), &handler.EnqueueRequestForObject{}, predicate.GenerationChangedPredicate{}); err != nil {
		return err
	}

	// Watch Pipelines
	if err := controller.Watch(source.Kind(mgr.GetCache(), &numaflowv1.Pipeline{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &apiv1.PipelineRollout{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		return err
	}

	return nil
}

func isPipelinePausedOrUnpausible(ctx context.Context, pipeline *kubernetes.GenericObject) bool {
	// contract with Numaflow is that unpausible Pipelines are "Failed" pipelines
	return checkPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhasePaused) || checkPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhaseFailed)
}

// pipelineSpecEqual() tests for essential equality, with any irrelevant fields eliminated from the comparison
func pipelineSpecEqual(ctx context.Context, a *kubernetes.GenericObject, b *kubernetes.GenericObject) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	// remove lifecycle field from comparison, as well as any nulls to test for equality
	pipelineWithoutLifecycleA, err := pipelineWithoutLifecycle(a)
	if err != nil {
		return false, err
	}
	pipelineWithoutLifecycleB, err := pipelineWithoutLifecycle(b)
	if err != nil {
		return false, err
	}
	numaLogger.Debugf("comparing specs: pipelineWithoutLifecycleA=%v, pipelineWithoutLifecycleB=%v\n", pipelineWithoutLifecycleA, pipelineWithoutLifecycleB)

	//TODO: revisit this - why is reflect.DeepEqual sometimes sufficient?
	return util.CompareMapsIgnoringNulls(pipelineWithoutLifecycleA, pipelineWithoutLifecycleB), nil
}

// remove 'lifecycle' key/value pair from Pipeline spec
func pipelineWithoutLifecycle(obj *kubernetes.GenericObject) (map[string]interface{}, error) {
	unstruc, err := kubernetes.ObjectToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	_, found, err := unstructured.NestedString(unstruc.Object, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return nil, err
	}
	if found {
		unstrucNew := unstruc.DeepCopy()
		specMapAsInterface, found := unstrucNew.Object["spec"]
		if found {
			specMap, ok := specMapAsInterface.(map[string]interface{})
			if ok {
				lifecycleMapAsInterface, found := specMap["lifecycle"]
				if found {
					lifecycleMap, ok := lifecycleMapAsInterface.(map[string]interface{})
					if ok {
						delete(lifecycleMap, "desiredPhase")
						specMap["lifecycle"] = lifecycleMap
						return specMap, nil
					}
				}
			}

			return nil, fmt.Errorf("failed to clear spec.lifecycle.desiredPhase from object: %+v", unstruc.Object)
		}
	}
	return unstruc.Object["spec"].(map[string]interface{}), nil
}

// TODO: detect engine determines when Pipeline spec change requires pausing
func needsPausing(_ *kubernetes.GenericObject, _ *kubernetes.GenericObject) (bool, error) {
	return true, nil
}

func checkPipelineStatus(ctx context.Context, pipeline *kubernetes.GenericObject, phase numaflowv1.PipelinePhase) bool {
	numaLogger := logger.FromContext(ctx)
	pipelineStatus, err := kubernetes.ParseStatus(pipeline)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Pipeline Status from pipeline CR: %+v, %v", pipeline, err)
		return false
	}

	numaLogger.Debugf("pipeline status: %+v", pipelineStatus)

	return numaflowv1.PipelinePhase(pipelineStatus.Phase) == phase
}

func updatePipelineSpec(ctx context.Context, restConfig *rest.Config, obj *kubernetes.GenericObject) error {
	return kubernetes.UpdateCR(ctx, restConfig, obj, "pipelines")
}

func pipelineLabels(pipelineRollout *apiv1.PipelineRollout) (map[string]string, error) {
	var pipelineSpec PipelineSpec
	labelMapping := map[string]string{
		common.LabelKeyISBServiceNameForPipeline: "default",
	}
	if err := json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pipeline spec: %v", err)
	}
	if pipelineSpec.InterStepBufferServiceName != "" {
		labelMapping[common.LabelKeyISBServiceNameForPipeline] = pipelineSpec.InterStepBufferServiceName
	}

	return labelMapping, nil
}
func (r *PipelineRolloutReconciler) updatePipelineRolloutStatus(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	rawSpec := runtime.RawExtension{}
	err := util.StructToStruct(&pipelineRollout.Spec, &rawSpec)
	if err != nil {
		return fmt.Errorf("unable to convert PipelineRollout Spec to GenericObject Spec: %v", err)
	}

	rawStatus := runtime.RawExtension{}
	err = util.StructToStruct(&pipelineRollout.Status, &rawStatus)
	if err != nil {
		return fmt.Errorf("unable to convert PipelineRollout Status to GenericObject Status: %v", err)
	}

	obj := kubernetes.GenericObject{
		TypeMeta:   pipelineRollout.TypeMeta,
		ObjectMeta: pipelineRollout.ObjectMeta,
		Spec:       rawSpec,
		Status:     rawStatus,
	}

	return kubernetes.UpdateStatus(ctx, r.restConfig, &obj, "pipelinerollouts")
}

func (r *PipelineRolloutReconciler) updatePipelineRolloutStatusToFailed(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, err error) error {
	pipelineRollout.Status.MarkFailed(err.Error())
	return r.updatePipelineRolloutStatus(ctx, pipelineRollout)
}

func makePipelineDefinition(pipelineRollout *apiv1.PipelineRollout) (*kubernetes.GenericObject, error) {
	labels, err := pipelineLabels(pipelineRollout)
	if err != nil {
		return nil, err
	}

	return &kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            pipelineRollout.Name,
			Namespace:       pipelineRollout.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pipelineRollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)},
		},
		Spec: pipelineRollout.Spec.Pipeline.Spec,
	}, nil
}

func getISBSvcName(pipeline PipelineSpec) string {
	if pipeline.InterStepBufferServiceName == "" {
		return "default"
	}
	return pipeline.InterStepBufferServiceName
}

func getPipelineChildResourceHealth(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, cond := range conditions {
		switch cond.Type {
		case "VerticesHealthy", "SideInputsManagersHealthy", "DaemonServiceHealthy":
			// if any child resource unhealthy return status (false/unknown)
			if cond.Status != "True" {
				return cond.Status, cond.Reason
			}
		}
	}
	return "True", ""
}

func checkChildResources(conditions []metav1.Condition, f func(metav1.Condition) bool) (bool, metav1.Condition) {
	for _, cond := range conditions {
		if f(cond) {
			return true, cond
		}
	}
	return false, metav1.Condition{}
}

func (r *PipelineRolloutReconciler) ErrorHandler(pipelineRollout *apiv1.PipelineRollout, err error, reason, msg string) {
	r.customMetrics.PipelinesSyncFailed.WithLabelValues().Inc()
	r.recorder.Eventf(pipelineRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}
