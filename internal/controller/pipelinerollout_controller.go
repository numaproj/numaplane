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
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
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

var pipelineROReconciler *PipelineRolloutReconciler

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
}

func NewPipelineRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
	customMetrics *metrics.CustomMetrics,
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
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PipelineRollout object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *PipelineRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	numaLogger := logger.GetBaseLogger().WithName(loggerName).WithValues("pipelinerollout", req.NamespacedName)

	r.enqueuePipeline(req.NamespacedName)
	numaLogger.Debugf("PipelineRollout Reconciler added PipelineRollout %v to queue", req.NamespacedName)
	return ctrl.Result{}, nil
}

func (r *PipelineRolloutReconciler) enqueuePipeline(namespacedName k8stypes.NamespacedName) {
	key := namespacedNameToKey(namespacedName)
	r.queue.AddRateLimited(key)
}

func (r *PipelineRolloutReconciler) processPipelineRollout(ctx context.Context, namespacedName k8stypes.NamespacedName) (ctrl.Result, error) {
	numaLogger := logger.FromContext(ctx).WithValues("pipelinerollout", namespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	// Get PipelineRollout CR
	pipelineRollout := &apiv1.PipelineRollout{}
	if err := r.client.Get(ctx, namespacedName, pipelineRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			numaLogger.Error(err, "Unable to get PipelineRollout")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	pipelineRolloutOrig := pipelineRollout
	pipelineRollout = pipelineRolloutOrig.DeepCopy()

	pipelineRollout.Status.Init(pipelineRollout.Generation)

	requeue, err := r.reconcile(ctx, pipelineRollout)
	if err != nil {
		statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
		if statusUpdateErr != nil {
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(pipelineRolloutOrig, pipelineRollout) {
		pipelineRolloutStatus := pipelineRollout.Status
		if err := r.client.Update(ctx, pipelineRollout); err != nil {
			numaLogger.Error(err, "Error Updating PipelineRollout", "PipelineRollout", pipelineRollout)

			statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
			if statusUpdateErr != nil {
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
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate the metrics for the Pipeline.
	r.customMetrics.IncPipelineMetrics(pipelineRollout.Name, pipelineRollout.Namespace)

	if requeue {
		return ctrl.Result{Requeue: true, RequeueAfter: 30 * time.Second}, nil
	}

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
		return k8stypes.NamespacedName{}, fmt.Errorf("Improperly formatted key: %q", key)
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
		return false, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(pipelineRollout, finalizerName) {
		controllerutil.AddFinalizer(pipelineRollout, finalizerName)
	}

	defer pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)

	var newPipelineSpec PipelineSpec
	if err := json.Unmarshal(pipelineRollout.Spec.Pipeline.Raw, &newPipelineSpec); err != nil {
		return false, fmt.Errorf("failed to convert PipelineRollout Pipeline spec %q into PipelineSpec type, err=%v", string(pipelineRollout.Spec.Pipeline.Raw), err)
	}

	labels, err := pipelineLabels(&newPipelineSpec)
	if err != nil {
		return false, err
	}

	newPipelineDef := kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: common.NumaflowGroupVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            pipelineRollout.Name,
			Namespace:       pipelineRollout.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pipelineRollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)},
		},
		Spec: pipelineRollout.Spec.Pipeline,
	}

	// Get the object to see if it exists
	existingPipelineDef, err := kubernetes.GetCR(ctx, r.restConfig, &newPipelineDef, "pipelines")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("Pipeline %s/%s doesn't exist so creating", pipelineRollout.Namespace, pipelineRollout.Name)
			err = kubernetes.CreateCR(ctx, r.restConfig, &newPipelineDef, "pipelines")
			if err != nil {
				return false, err
			}

			return false, nil
		}

		return false, fmt.Errorf("error getting Pipeline: %v", err)
	}

	// Object already exists

	// propagate the pipeline's status into PipelineRollout's status
	var pipelineStatus kubernetes.GenericStatus
	processPipelineStatus(ctx, existingPipelineDef, pipelineRollout, &pipelineStatus)
	pipelineReconciled := pipelineRollout.Status.GetCondition(apiv1.ConditionChildResourceHealthy).Reason != "Progressing"

	// Get the fields we need from both the Pipeline spec we have and the one we want
	// todo: consider having a Pipeline struct which includes everything
	var existingPipelineSpec PipelineSpec
	if err = json.Unmarshal(existingPipelineDef.Spec.Raw, &existingPipelineSpec); err != nil {
		return false, fmt.Errorf("failed to convert existing Pipeline spec %q into PipelineSpec type, err=%v", string(existingPipelineDef.Spec.Raw), err)
	}

	// Does pipeline spec need to be updated?
	pipelineSpecsEqual, err := pipelineSpecEqual(ctx, existingPipelineDef, &newPipelineDef)
	if err != nil {
		return false, err
	}
	pipelineNeedsToOrIsUpdating := !pipelineSpecsEqual || !pipelineReconciled

	numaLogger.Debugf("pipelineNeedsToOrIsUpdating=%t, pipelineSpecsEqual=%t, pipelineReconciled=%t", pipelineNeedsToOrIsUpdating, pipelineSpecsEqual, pipelineReconciled)

	// If there is a need to update, does it require a pause?
	var pipelineUpdateRequiresPause bool
	if pipelineNeedsToOrIsUpdating {
		pipelineUpdateRequiresPause, err = needsPausing(existingPipelineDef, &newPipelineDef)
		if err != nil {
			return false, err
		}
	}
	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	externalPauseRequest, pauseRequestsKnown, err := r.checkForPauseRequest(ctx, pipelineRollout, getISBSvcName(newPipelineSpec))
	if err != nil {
		return false, err
	}
	if !pauseRequestsKnown {
		numaLogger.Debugf("incomplete pause request information")
		return false, nil
	}

	// make sure our Lifecycle is what we need it to be
	shouldBePaused := pipelineUpdateRequiresPause || externalPauseRequest
	numaLogger.Debugf("shouldBePaused=%t, pipelineUpdateRequiresPause=%t, externalPauseRequest=%t", shouldBePaused, pipelineUpdateRequiresPause, externalPauseRequest)
	err = r.setPipelineLifecycle(ctx, pipelineRollout, shouldBePaused, existingPipelineDef)
	if err != nil {
		return false, err
	}

	// if it's safe to Update and we need to, do it now
	if !pipelineSpecsEqual {
		if !pipelineUpdateRequiresPause || (pipelineUpdateRequiresPause && isPipelinePaused(ctx, existingPipelineDef)) {
			numaLogger.Infof("it's safe to update Pipeline so updating now")
			err = applyPipelineSpec(ctx, r.restConfig, &newPipelineDef)
			if err != nil {
				return false, err
			}
		}
	}

	return false, nil
}

// make sure our Lifecycle is what we need it to be
func (r *PipelineRolloutReconciler) setPipelineLifecycle(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, paused bool, existingPipelineDef *kubernetes.GenericObject) error {
	numaLogger := logger.FromContext(ctx)
	var existingPipelineSpec PipelineSpec //todo: consider that I'm doing this here and in the methods being called both
	if err := json.Unmarshal(existingPipelineDef.Spec.Raw, &existingPipelineSpec); err != nil {
		return err
	}
	lifeCycleIsPaused := existingPipelineSpec.Lifecycle.DesiredPhase == "Paused"

	if paused && !lifeCycleIsPaused {
		numaLogger.Info("pausing pipeline")
		if err := GetPauseModule().pausePipeline(ctx, r.restConfig, existingPipelineDef); err != nil {
			return err
		}
	} else if !paused && lifeCycleIsPaused {
		numaLogger.Info("resuming pipeline")
		run, err := GetPauseModule().runPipelineIfSafe(ctx, r.restConfig, existingPipelineDef)
		if err != nil {
			return err
		}
		if !run {
			numaLogger.Infof("new pause request, can't resume pipeline at this time, will try again later")
		}
	}
	return nil
}

func (r *PipelineRolloutReconciler) checkForPauseRequest(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, isbsvcName string) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)
	// Is either Numaflow Controller or ISBService trying to update (such that we need to pause)?
	controllerPauseRequest, found := GetPauseModule().getControllerPauseRequest(pipelineRollout.Namespace)
	//controllerRolloutExists := true
	if !found {
		// this can happen for 2 reasons: either the ControllerRollout exists but hasn't been reconciled yet (Numaplane startup)
		// or it doesn't exist at all - in the first case, we need to wait to find out what it needs)
		// todo: revisit this - maybe we should wait in any case since pausing won't work if the Numaflow Controller isn't up to reconcile
		numaLogger.Debugf("No pause request found for numaflow controller on namespace %q", pipelineRollout.Namespace)
		return false, false, nil
		// see if NumaflowControllerRollout exists
		/*numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
		if err := r.client.Get(ctx, k8stypes.NamespacedName{Namespace: pipelineRollout.Namespace, Name: NumaflowControllerDeploymentName}, numaflowControllerRollout); err != nil {
			if apierrors.IsNotFound(err) {
				controllerRolloutExists = false
				numaLogger.Debugf("No pause request found for numaflow controller on namespace %q because numaflow controller rollout doesn't exist", pipelineRollout.Namespace)
			} else {
				return false, err
			}
		}*/

	}
	controllerRequestsPause := controllerPauseRequest != nil && *controllerPauseRequest

	isbsvcPauseRequest, found := GetPauseModule().getISBSvcPauseRequest(pipelineRollout.Namespace, isbsvcName)
	//isbsvcRolloutExists := true
	if !found {
		// this can happen for 2 reasons: either the ISBServiceRollout exists but hasn't been reconciled yet (Numaplane startup)
		// or it doesn't exist at all - in the first case, we need to wait to find out what it needs)
		numaLogger.Debugf("No pause request found for isbsvc %q on namespace %q", isbsvcName, pipelineRollout.Namespace)
		return false, false, nil
		// see if ISBServiceRollout exists
		/*isbsvcRollout := &apiv1.ISBServiceRollout{}
		if err := r.client.Get(ctx, k8stypes.NamespacedName{Namespace: pipelineRollout.Namespace, Name: isbsvcName}, isbsvcRollout); err != nil {
			if apierrors.IsNotFound(err) {
				isbsvcRolloutExists = false
				numaLogger.Debugf("No pause request found for isbsvc %q on namespace %q because isbservice rollout doesn't exist", isbsvcName, pipelineRollout.Namespace)
			} else {
				return false, err
			}
		}*/
	}
	isbsvcRequestsPause := (isbsvcPauseRequest != nil && *isbsvcPauseRequest)

	return controllerRequestsPause || isbsvcRequestsPause, true, nil
}

func setPipelineHealthStatus(pipeline *kubernetes.GenericObject, pipelineRollout *apiv1.PipelineRollout, pipelineObservedGeneration int64) {
	if pipelineObservedGenerationCurrent(pipeline.Generation, pipelineObservedGeneration) {
		pipelineRollout.Status.MarkChildResourcesHealthy(pipelineRollout.Generation)
	} else {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("Progressing", "Mismatch between Pipeline Generation and ObservedGeneration", pipelineRollout.Generation)
	}
}

func pipelineObservedGenerationCurrent(generation int64, observedGeneration int64) bool {
	// NOTE: this assumes that Numaflow default ObservedGeneration is -1
	// `pipelineObservedGeneration == 0` is used to avoid backward compatibility
	// issues for Numaflow versions that do not have ObservedGeneration
	return observedGeneration == 0 || generation <= observedGeneration
}

// Set the Condition in the Status for child resource health
// pipeline and pipelineRollout are passed in; pipelineStatus is passed back
func processPipelineStatus(ctx context.Context, pipeline *kubernetes.GenericObject, pipelineRollout *apiv1.PipelineRollout, pipelineStatus *kubernetes.GenericStatus) {
	numaLogger := logger.FromContext(ctx)
	status, err := kubernetes.ParseStatus(pipeline)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Pipeline Status from pipeline CR: %+v, %v", pipeline, err)
		return
	}
	pipelineStatus = &status

	numaLogger.Debugf("pipeline status: %+v", pipelineStatus)

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)
	switch pipelinePhase {
	case numaflowv1.PipelinePhaseFailed:
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Failed", pipelineRollout.Generation)
	case numaflowv1.PipelinePhasePaused, numaflowv1.PipelinePhasePausing:
		pipelineRollout.Status.MarkPipelinePausingOrPausedWithReason(fmt.Sprintf("Pipeline%s", string(pipelinePhase)), pipelineRollout.Generation)
		setPipelineHealthStatus(pipeline, pipelineRollout, pipelineStatus.ObservedGeneration)
	case numaflowv1.PipelinePhaseUnknown:
		pipelineRollout.Status.MarkChildResourcesHealthUnknown("PipelineUnknown", "Pipeline Phase Unknown", pipelineRollout.Generation)
	case numaflowv1.PipelinePhaseDeleting:
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineDeleting", "Pipeline Deleting", pipelineRollout.Generation)
	default:
		setPipelineHealthStatus(pipeline, pipelineRollout, pipelineStatus.ObservedGeneration)
	}

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

func isPipelinePausing(ctx context.Context, pipeline *kubernetes.GenericObject) bool {
	return checkPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhasePausing)
}

func isPipelinePaused(ctx context.Context, pipeline *kubernetes.GenericObject) bool {
	return checkPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhasePaused)
}

func pipelineSpecEqual(ctx context.Context, a *kubernetes.GenericObject, b *kubernetes.GenericObject) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelineWithoutLifecycleA, err := pipelineWithoutLifecycle(a)
	if err != nil {
		return false, err
	}
	pipelineWithoutLifecycleB, err := pipelineWithoutLifecycle(b)
	if err != nil {
		return false, err
	}
	numaLogger.Debugf("comparing specs: pipelineWithoutLifecycleA=%v, pipelineWithoutLifecycleB=%v\n", pipelineWithoutLifecycleA, pipelineWithoutLifecycleB)

	//todo: revisit this - in the main branch it seems sufficient to do reflect.DeepEqual on the two RawExtensions
	return util.CompareMapsIgnoringNulls(pipelineWithoutLifecycleA, pipelineWithoutLifecycleB), nil
}

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
					if ok {
						lifecycleMap, ok := lifecycleMapAsInterface.(map[string]interface{})
						if ok {
							delete(lifecycleMap, "desiredPhase")
							specMap["lifecycle"] = lifecycleMap
							//unstrucNew.Object["spec"] = specMap
							return specMap, nil
						}
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

func applyPipelineSpec(
	ctx context.Context,
	restConfig *rest.Config,
	obj *kubernetes.GenericObject,
) error {
	numaLogger := logger.FromContext(ctx)

	// TODO: use UpdateSpec instead
	err := kubernetes.ApplyCRSpec(ctx, restConfig, obj, "pipelines")
	if err != nil {
		numaLogger.Errorf(err, "failed to apply Pipeline: %v", err)
		return err
	}

	return nil
}

func pipelineLabels(pipelineSpec *PipelineSpec) (map[string]string, error) {
	labelMapping := map[string]string{
		"isbsvc-name": "default",
	}
	if pipelineSpec.InterStepBufferServiceName != "" {
		labelMapping["isbsvc-name"] = pipelineSpec.InterStepBufferServiceName
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
	numaLogger := logger.FromContext(ctx)

	pipelineRollout.Status.MarkFailed(pipelineRollout.Generation, err.Error())

	statusUpdateErr := r.updatePipelineRolloutStatus(ctx, pipelineRollout)
	if statusUpdateErr != nil {
		numaLogger.Error(statusUpdateErr, "Error updating PipelineRollout status", "namespace", pipelineRollout.Namespace, "name", pipelineRollout.Name)
	}

	return statusUpdateErr

}

func getISBSvcName(pipeline PipelineSpec) string {
	if pipeline.InterStepBufferServiceName == "" {
		return "default"
	}
	return pipeline.InterStepBufferServiceName
}

// keep track of the minimum number of fields we need to know about
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
