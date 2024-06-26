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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ControllerPipelineRollout = "pipeline-rollout-controller"
)

// PipelineRolloutReconciler reconciles a PipelineRollout object
type PipelineRolloutReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	restConfig *rest.Config
}

func NewPipelineRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
) *PipelineRolloutReconciler {
	return &PipelineRolloutReconciler{
		client,
		s,
		restConfig,
	}
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
	// update the Base Logger's level according to the Numaplane Config
	logger.RefreshBaseLoggerLevel()
	numaLogger := logger.GetBaseLogger().WithName("pipelinerollout-reconciler").WithValues("pipelinerollout", req.NamespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	// Get PipelineRollout CR
	pipelineRollout := &apiv1.PipelineRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, pipelineRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			numaLogger.Error(err, "Unable to get PipelineRollout", "request", req)
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	pipelineRolloutOrig := pipelineRollout
	pipelineRollout = pipelineRolloutOrig.DeepCopy()

	pipelineRollout.Status.InitConditions()

	requeue, err := r.reconcile(ctx, pipelineRollout)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(pipelineRolloutOrig, pipelineRollout) {
		pipelineRolloutStatus := pipelineRollout.Status
		if err := r.client.Update(ctx, pipelineRollout); err != nil {
			numaLogger.Error(err, "Error Updating PipelineRollout", "PipelineRollout", pipelineRollout)
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		pipelineRollout.Status = pipelineRolloutStatus
	}

	// Update the Status subresource
	if pipelineRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latestPipelineRollout := &apiv1.PipelineRollout{}
			if err := r.client.Get(ctx, req.NamespacedName, latestPipelineRollout); err != nil {
				return err
			}

			latestPipelineRollout.Status = pipelineRollout.Status
			return r.client.Status().Update(ctx, latestPipelineRollout)
		})
		if err != nil {
			numaLogger.Error(err, "Error Updating PipelineRollout Status", "PipelineRollout", pipelineRollout)
			return ctrl.Result{}, err
		}
	}

	if requeue {
		return ctrl.Result{Requeue: true, RequeueAfter: 30 * time.Second}, nil
	}
	numaLogger.Debug("reconciliation successful")

	return ctrl.Result{}, nil
}

// reconcile does the real logic, it returns true if the event
// needs to be re-queued.
func (r *PipelineRolloutReconciler) reconcile(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// is PipelineRollout being deleted? need to remove the finalizer so it can
	// (OwnerReference will delete the underlying Pipeline through Cascading deletion)
	if !pipelineRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting PipelineRollout")
		if controllerutil.ContainsFinalizer(pipelineRollout, finalizerName) {
			controllerutil.RemoveFinalizer(pipelineRollout, finalizerName)
		}
		return false, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(pipelineRollout, finalizerName) {
		controllerutil.AddFinalizer(pipelineRollout, finalizerName)
	}

	newPipelineDef := kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            pipelineRollout.Name,
			Namespace:       pipelineRollout.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pipelineRollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)},
		},
		Spec: pipelineRollout.Spec.Pipeline,
	}

	// Get the object to see if it exists
	existingPipelineDef, err := kubernetes.GetCR(ctx, r.restConfig, &newPipelineDef, "pipelines")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {
			err = kubernetes.CreateCR(ctx, r.restConfig, &newPipelineDef, "pipelines")
			if err != nil {
				return false, err
			}

			pipelineRollout.Status.MarkRunning()

			return false, nil
		}

		return false, fmt.Errorf("error getting Pipeline: %v", err)
	}

	// propagate the pipeline's status into PipelineRollout's status
	processPipelineStatus(ctx, existingPipelineDef, pipelineRollout)

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
		newObj, err := setPipelineDesiredStatus(&newPipelineDef, "Running")
		if err != nil {
			return false, err
		}
		newPipelineDef = *newObj

		err = applyPipelineSpec(ctx, r.restConfig, &newPipelineDef, pipelineRollout)
		if err != nil {
			return false, err
		}

		return false, nil
	}

	// If pipeline status is not above, detect if pausing is required.
	shouldPause, err := needsPausing(existingPipelineDef, &newPipelineDef)
	if err != nil {
		return false, err
	}
	if shouldPause {
		// Use the existing spec, then pause and re-enqueue
		newPipelineDef.Spec = existingPipelineDef.Spec
		newObj, err := setPipelineDesiredStatus(&newPipelineDef, "Paused")
		if err != nil {
			return false, err
		}
		newPipelineDef = *newObj

		err = applyPipelineSpec(ctx, r.restConfig, &newPipelineDef, pipelineRollout)
		if err != nil {
			return false, err
		}
		return true, err
	}

	// If no need to pause, just apply the spec
	err = applyPipelineSpec(ctx, r.restConfig, &newPipelineDef, pipelineRollout)
	if err != nil {
		return false, err
	}

	pipelineRollout.Status.MarkRunning()

	return false, nil
}

// Set the Condition in the Status for child resource health
func processPipelineStatus(ctx context.Context, pipeline *kubernetes.GenericObject, pipelineRollout *apiv1.PipelineRollout) {
	numaLogger := logger.FromContext(ctx)
	pipelineStatus, err := kubernetes.ParseStatus(pipeline)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Pipeline Status from pipeline CR: %+v, %v", pipeline, err)
		return
	}

	numaLogger.Debugf("pipeline status: %+v", pipelineStatus)

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)
	switch pipelinePhase {
	case numaflowv1.PipelinePhaseFailed:
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Failed")
	case numaflowv1.PipelinePhaseUnknown:
		// this will have been set to Unknown in the call to InitConditions()
	default:
		pipelineRollout.Status.MarkChildResourcesHealthy()
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
	pipelineRollout *apiv1.PipelineRollout,
) error {
	numaLogger := logger.FromContext(ctx)

	// TODO: use UpdateSpec instead
	err := kubernetes.ApplyCRSpec(ctx, restConfig, obj, "pipelines")
	if err != nil {
		numaLogger.Errorf(err, "failed to apply Pipeline: %v", err)
		pipelineRollout.Status.MarkFailed("ApplyPipelineFailure", err.Error())
		return err
	}
	return nil
}
