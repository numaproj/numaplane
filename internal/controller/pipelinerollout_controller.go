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

	namespacedName := namespacedNameToKey(req.NamespacedName)
	r.queue.Add(namespacedName)
	numaLogger.Debugf("added PipelineRollout %v to queue", namespacedName)
	return ctrl.Result{}, nil
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

	// Update PipelineRollout Status based on child resource (Pipeline) Status
	err = r.processPipelineStatus(ctx, pipelineRollout)
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
		r.queue.AddRateLimited(key)
	} else {
		if result.Requeue {
			r.queue.AddRateLimited(key)
		} else if result.RequeueAfter > 0 {
			r.queue.AddAfter(key, result.RequeueAfter)
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

	newPipelineDef, err := makePipelineDefinition(pipelineRollout)
	if err != nil {
		return false, err
	}

	// Get the object to see if it exists
	existingPipelineDef, err := kubernetes.GetCR(ctx, r.restConfig, newPipelineDef, "pipelines")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {
			err = kubernetes.CreateCR(ctx, r.restConfig, newPipelineDef, "pipelines")
			if err != nil {
				return false, err
			}

			return false, nil
		}

		return false, fmt.Errorf("error getting Pipeline: %v", err)
	}

	// If the pipeline already exists, first check if the pipeline status
	// is pausing. If so, re-enqueue immediately.
	pausing := isPipelinePausing(ctx, existingPipelineDef)
	if pausing {
		// re-enqueue
		return true, nil
	}

	// Check if the pipeline status is paused. If so, apply the change and
	// resume.
	paused := isPipelinePaused(ctx, existingPipelineDef)
	if paused {
		// Apply the new spec and resume the pipeline
		// TODO: in the future, need to take into account whether Numaflow Controller
		//       or ISBService is being installed to determine whether it's safe to unpause
		newObj, err := setPipelineDesiredStatus(newPipelineDef, "Running")
		if err != nil {
			return false, err
		}
		newPipelineDef = newObj

		err = applyPipelineSpec(ctx, r.restConfig, newPipelineDef)
		if err != nil {
			return false, err
		}

		return false, nil
	}

	// If pipeline status is not above, detect if pausing is required.
	shouldPause, err := needsPausing(existingPipelineDef, newPipelineDef)
	if err != nil {
		return false, err
	}
	if shouldPause {
		// Use the existing spec, then pause and re-enqueue
		newPipelineDef.Spec = existingPipelineDef.Spec
		newObj, err := setPipelineDesiredStatus(newPipelineDef, "Paused")
		if err != nil {
			return false, err
		}
		newPipelineDef = newObj

		err = applyPipelineSpec(ctx, r.restConfig, newPipelineDef)
		if err != nil {
			return false, err
		}
		return true, err
	}

	// If no need to pause, just apply the spec
	err = applyPipelineSpec(ctx, r.restConfig, newPipelineDef)
	if err != nil {
		return false, err
	}

	pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)

	return false, nil
}

func setPipelineHealthStatus(pipeline *kubernetes.GenericObject, pipelineRollout *apiv1.PipelineRollout, pipelineObservedGeneration int64) {
	// NOTE: this assumes that Numaflow default ObservedGeneration is -1
	// `pipelineObservedGeneration == 0` is used to avoid backward compatibility
	// issues for Numaflow versions that do not have ObservedGeneration
	if pipelineObservedGeneration == 0 || pipeline.Generation <= pipelineObservedGeneration {
		pipelineRollout.Status.MarkChildResourcesHealthy(pipelineRollout.Generation)
	} else {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("Progressing", "Mismatch between Pipeline Generation and ObservedGeneration", pipelineRollout.Generation)
	}
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

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)
	switch pipelinePhase {
	case numaflowv1.PipelinePhaseFailed:
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Failed", pipelineRollout.Generation)
	case numaflowv1.PipelinePhasePaused, numaflowv1.PipelinePhasePausing:
		pipelineRollout.Status.MarkPipelinePausingOrPausedWithReason(fmt.Sprintf("Pipeline%s", string(pipelinePhase)), pipelineRollout.Generation)
		setPipelineHealthStatus(existingPipelineDef, pipelineRollout, pipelineStatus.ObservedGeneration)
	case numaflowv1.PipelinePhaseUnknown:
		pipelineRollout.Status.MarkChildResourcesHealthUnknown("PipelineUnknown", "Pipeline Phase Unknown", pipelineRollout.Generation)
	case numaflowv1.PipelinePhaseDeleting:
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineDeleting", "Pipeline Deleting", pipelineRollout.Generation)
	default:
		setPipelineHealthStatus(existingPipelineDef, pipelineRollout, pipelineStatus.ObservedGeneration)
	}

	return nil
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

func setPipelineDesiredStatus(obj *kubernetes.GenericObject, status string) (*kubernetes.GenericObject, error) {
	unstruc, err := kubernetes.ObjectToUnstructured(obj)
	if err != nil {
		return nil, err
	}

	err = unstructured.SetNestedField(unstruc.Object, status, "spec", "lifecycle", "desiredPhase")
	if err != nil {
		return nil, err
	}

	newObj, err := kubernetes.UnstructuredToObject(unstruc)
	if err != nil {
		return nil, err
	}
	return newObj, nil
}

func isPipelinePausing(ctx context.Context, pipeline *kubernetes.GenericObject) bool {
	return checkPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhasePausing)
}

func isPipelinePaused(ctx context.Context, pipeline *kubernetes.GenericObject) bool {
	return checkPipelineStatus(ctx, pipeline, numaflowv1.PipelinePhasePaused)
}

// TODO: detect engine, now always not pause, enable to pause when we can detect spec change
func needsPausing(_ *kubernetes.GenericObject, _ *kubernetes.GenericObject) (bool, error) {
	return false, nil
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

func pipelineLabels(pipelineRollout *apiv1.PipelineRollout) (map[string]string, error) {
	var pipelineSpec struct {
		InterStepBufferServiceName string `json:"interStepBufferServiceName"`
	}
	labelMapping := map[string]string{
		"isbsvc-name": "default",
	}
	if err := json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pipeline spec: %v", err)
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
		Spec: pipelineRollout.Spec.Pipeline,
	}, nil
}
