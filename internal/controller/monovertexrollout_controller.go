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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	ControllerMonoVertexRollout = "monovertex-rollout-controller"
)

// MonoVertexRolloutReconciler reconciles a MonoVertexRollout object
type MonoVertexRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	restConfig    *rest.Config
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder
}

func NewMonoVertexRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *MonoVertexRolloutReconciler {

	return &MonoVertexRolloutReconciler{
		client,
		s,
		restConfig,
		customMetrics,
		recorder,
	}
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=monovertexrollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=monovertexrollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=monovertexrollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *MonoVertexRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	syncStartTime := time.Now()
	numaLogger := logger.GetBaseLogger().WithName("monovertexrollout-reconciler").WithValues("monovertexrollout", req.NamespacedName)

	// update the context with this Logger
	ctx = logger.WithLogger(ctx, numaLogger)
	r.customMetrics.MonoVerticesSynced.WithLabelValues().Inc()

	monoVertexRollout := &apiv1.MonoVertexRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, monoVertexRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(monoVertexRollout, err, "GetMonoVertexFailed", "Failed to get MonoVertexRollout")
		}
	}

	// store copy of original rollout
	monoVertexRolloutOrig := monoVertexRollout
	monoVertexRollout = monoVertexRolloutOrig.DeepCopy()

	monoVertexRollout.Status.Init(monoVertexRollout.Generation)

	result, err := r.reconcile(ctx, monoVertexRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(monoVertexRollout, err, "ReconcileFailed", "Failed to reconcile MonoVertexRollout")
		statusUpdateErr := r.updateMonoVertexRolloutStatusToFailed(ctx, monoVertexRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(monoVertexRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update MonoVertexRollout status")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	// update spec if needed
	if r.needsUpdate(monoVertexRolloutOrig, monoVertexRollout) {
		monoVertexRolloutStatus := monoVertexRollout.Status
		if err := r.client.Update(ctx, monoVertexRollout); err != nil {
			r.ErrorHandler(monoVertexRollout, err, "UpdateFailed", "Failed to update MonoVertexRollout")
			statusUpdateErr := r.updateMonoVertexRolloutStatusToFailed(ctx, monoVertexRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(monoVertexRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update MonoVertexRollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
		// restore original status which is lost during last call to Update()
		monoVertexRollout.Status = monoVertexRolloutStatus
	}

	if monoVertexRollout.DeletionTimestamp.IsZero() {
		statusUpdateErr := r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(monoVertexRollout, statusUpdateErr, "StatusUpdateFailed", "Failed to update MonoVertexRollout")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate metrics for MonoVertex
	r.customMetrics.IncMonoVertexMetrics(monoVertexRollout.Name, monoVertexRollout.Namespace)
	r.recorder.Eventf(monoVertexRollout, corev1.EventTypeNormal, "ReconciliationSuccessful", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

	return result, nil
}

func (r *MonoVertexRolloutReconciler) reconcile(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, syncStartTime time.Time) (ctrl.Result, error) {

	startTime := time.Now()
	numaLogger := logger.FromContext(ctx)

	defer func() {
		if monoVertexRollout.Status.IsHealthy() {
			r.customMetrics.MonoVerticesRolloutHealth.WithLabelValues(monoVertexRollout.Namespace, monoVertexRollout.Name).Set(1)
		} else {
			r.customMetrics.MonoVerticesRolloutHealth.WithLabelValues(monoVertexRollout.Namespace, monoVertexRollout.Name).Set(0)
		}
	}()

	// remove finalizers if monoVertexRollout is being deleted
	if !monoVertexRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting MonoVertexRollout")
		if controllerutil.ContainsFinalizer(monoVertexRollout, finalizerName) {
			controllerutil.RemoveFinalizer(monoVertexRollout, finalizerName)
		}
		// generate metrics for MonoVertex deletion
		r.customMetrics.DecMonoVertexMetrics(monoVertexRollout.Name, monoVertexRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "delete").Observe(time.Since(startTime).Seconds())
		r.customMetrics.MonoVerticesRolloutHealth.DeleteLabelValues(monoVertexRollout.Namespace, monoVertexRollout.Name)
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(monoVertexRollout, finalizerName) {
		controllerutil.AddFinalizer(monoVertexRollout, finalizerName)
	}

	newMonoVertexDef := &kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "MonoVertex",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            monoVertexRollout.Name,
			Namespace:       monoVertexRollout.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(monoVertexRollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)},
			Annotations:     monoVertexRollout.Spec.MonoVertex.Annotations,
			Labels:          monoVertexRollout.Spec.MonoVertex.Labels,
		},
		Spec: monoVertexRollout.Spec.MonoVertex.Spec,
	}

	existingMonoVertexDef, err := kubernetes.GetLiveResource(ctx, r.restConfig, newMonoVertexDef, "monovertices")
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("MonoVertex %s/%s doesn't exist so creating", monoVertexRollout.Namespace, monoVertexRollout.Name)
			monoVertexRollout.Status.MarkPending()

			if err := kubernetes.CreateCR(ctx, r.restConfig, newMonoVertexDef, "monovertices"); err != nil {
				return ctrl.Result{}, err
			}

			monoVertexRollout.Status.MarkDeployed(monoVertexRollout.Generation)
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "create").Observe(time.Since(startTime).Seconds())
		} else {
			return ctrl.Result{}, fmt.Errorf("error getting MonoVertex: %v", err)
		}
	} else {
		// merge and update
		// we directly apply changes as there is no need for draining MonoVertex
		newMonoVertexDef = mergeMonoVertex(existingMonoVertexDef, newMonoVertexDef)
		err := r.updateMonoVertex(ctx, monoVertexRollout, newMonoVertexDef)
		if err != nil {
			return ctrl.Result{}, err
		}
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "update").Observe(time.Since(syncStartTime).Seconds())
	}

	// process status
	r.processMonoVertexStatus(ctx, existingMonoVertexDef, monoVertexRollout)

	return ctrl.Result{}, nil

}

// SetupWithManager sets up the controller with the Manager.
func (r *MonoVertexRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {

	controller, err := runtimecontroller.New(ControllerMonoVertexRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch MonoVertexRollouts
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.MonoVertexRollout{}), &handler.EnqueueRequestForObject{}, predicate.GenerationChangedPredicate{}); err != nil {
		return err
	}

	// Watch MonoVertices
	if err := controller.Watch(source.Kind(mgr.GetCache(), &numaflowv1.MonoVertex{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &apiv1.MonoVertexRollout{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		return err
	}

	return nil
}

func mergeMonoVertex(existingMonoVertex *kubernetes.GenericObject, newMonoVertex *kubernetes.GenericObject) *kubernetes.GenericObject {
	resultMonoVertex := existingMonoVertex.DeepCopy()
	resultMonoVertex.Spec = *newMonoVertex.Spec.DeepCopy()
	return resultMonoVertex
}

func (r *MonoVertexRolloutReconciler) processMonoVertexStatus(ctx context.Context, monoVertex *kubernetes.GenericObject, rollout *apiv1.MonoVertexRollout) {
	numaLogger := logger.FromContext(ctx)
	monoVertexStatus, err := kubernetes.ParseStatus(monoVertex)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse status from MonoVertex: %+v, %v", monoVertex, err)
		return
	}

	numaLogger.Debugf("monoVertex status: %+v", monoVertexStatus)

	monoVertexPhase := numaflowv1.MonoVertexPhase(monoVertexStatus.Phase)
	monoVertexChildResourceStatus, monoVertexChildResourceReason := getMonoVertexChildResourceHealth(monoVertexStatus.Conditions)

	if monoVertexChildResourceReason == "Progressing" {
		rollout.Status.MarkChildResourcesUnhealthy("Progressing", "MonoVertex Progressing", rollout.Generation)
	} else if monoVertexPhase == numaflowv1.MonoVertexPhaseFailed || monoVertexChildResourceStatus == "False" {
		rollout.Status.MarkChildResourcesUnhealthy("MonoVertexFailed", "MonoVertex Failed", rollout.Generation)
	} else if monoVertexPhase == numaflowv1.MonoVertexPhaseUnknown || monoVertexChildResourceStatus == "Unknown" {
		rollout.Status.MarkChildResourcesHealthUnknown("MonoVertexUnkown", "MonoVertex Phase Unknown", rollout.Generation)
	} else {
		rollout.Status.MarkChildResourcesHealthy(rollout.Generation)
	}

}

func (r *MonoVertexRolloutReconciler) needsUpdate(old, new *apiv1.MonoVertexRollout) bool {
	if old == nil {
		return true
	}
	if !equality.Semantic.DeepEqual(old.Finalizers, new.Finalizers) {
		return true
	}
	return false
}

func (r *MonoVertexRolloutReconciler) updateMonoVertex(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, newMonoVertexDef *kubernetes.GenericObject) error {
	err := kubernetes.UpdateCR(ctx, r.restConfig, newMonoVertexDef, "monovertices")
	if err != nil {
		return err
	}

	monoVertexRollout.Status.MarkDeployed(monoVertexRollout.Generation)
	return nil
}

func (r *MonoVertexRolloutReconciler) updateMonoVertexRolloutStatus(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout) error {
	return r.client.Status().Update(ctx, monoVertexRollout)
}

func (r *MonoVertexRolloutReconciler) updateMonoVertexRolloutStatusToFailed(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, err error) error {
	monoVertexRollout.Status.MarkFailed(err.Error())
	return r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
}

func (r *MonoVertexRolloutReconciler) ErrorHandler(monoVertexRollout *apiv1.MonoVertexRollout, err error, reason, msg string) {
	r.customMetrics.MonoVerticesSyncFailed.WithLabelValues().Inc()
	r.recorder.Eventf(monoVertexRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

func getMonoVertexChildResourceHealth(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, cond := range conditions {
		switch cond.Type {
		case "DaemonHealthy", "PodsHealthy":
			if cond.Status != "True" {
				return cond.Status, cond.Reason
			}
		}
	}
	return "True", ""
}
