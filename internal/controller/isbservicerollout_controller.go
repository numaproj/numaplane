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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
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
)

const (
	ControllerISBSVCRollout = "isbsvc-rollout-controller"
)

// ISBServiceRolloutReconciler reconciles an ISBServiceRollout object
type ISBServiceRolloutReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	restConfig *rest.Config
}

func NewISBServiceRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
) *ISBServiceRolloutReconciler {
	return &ISBServiceRolloutReconciler{
		client,
		s,
		restConfig,
	}
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=isbservicerollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=isbservicerollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=isbservicerollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ISBServiceRollout object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *ISBServiceRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// update the Base Logger's level according to the Numaplane Config
	logger.RefreshBaseLoggerLevel()
	numaLogger := logger.GetBaseLogger().WithName("isbservicerollout-reconciler").WithValues("isbservicerollout", req.NamespacedName)

	isbServiceRollout := &apiv1.ISBServiceRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, isbServiceRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			numaLogger.Error(err, "Unable to get ISBServiceRollout", "request", req)
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	isbServiceRolloutOrig := isbServiceRollout
	isbServiceRollout = isbServiceRolloutOrig.DeepCopy()

	isbServiceRollout.Status.InitConditions()

	err := r.reconcile(ctx, isbServiceRollout)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(isbServiceRolloutOrig, isbServiceRollout) {
		isbServiceRolloutStatus := isbServiceRollout.Status
		if err := r.client.Update(ctx, isbServiceRollout); err != nil {
			numaLogger.Error(err, "Error Updating ISBServiceRollout", "ISBServiceRollout", isbServiceRollout)
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		isbServiceRollout.Status = isbServiceRolloutStatus
	}

	// Update the Status subresource
	if isbServiceRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		if err := r.client.Status().Update(ctx, isbServiceRollout); err != nil {
			numaLogger.Error(err, "Error Updating isbServiceRollout Status", "isbServiceRollout", isbServiceRollout)
			return ctrl.Result{}, err
		}
	}

	numaLogger.Debug("reconciliation successful")

	return ctrl.Result{}, nil
}

// reconcile does the real logic
func (r *ISBServiceRolloutReconciler) reconcile(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) error {
	numaLogger := logger.FromContext(ctx)

	// is isbServiceRollout being deleted? need to remove the finalizer so it can
	// (OwnerReference will delete the underlying ISBService through Cascading deletion)
	if !isbServiceRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting ISBServiceRollout")
		if controllerutil.ContainsFinalizer(isbServiceRollout, finalizerName) {
			controllerutil.RemoveFinalizer(isbServiceRollout, finalizerName)
		}
		return nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(isbServiceRollout, finalizerName) {
		controllerutil.AddFinalizer(isbServiceRollout, finalizerName)
	}

	// make an InterStepBufferService object and add/update spec hash on the ISBServiceRollout object
	obj, rolloutChildOp, err := makeChildResourceFromRolloutAndUpdateSpecHash(ctx, r.restConfig, isbServiceRollout)
	if err != nil {
		numaLogger.Errorf(err, "failed to make an InterStepBufferService object and to update the ISBServiceRollout: %v", err)
		return err
	}

	// TODO: instead of doing this, modify the ApplyCRSpec below to be similar to what is done on the PipelineRollout controller code
	if rolloutChildOp == RolloutChildNoop {
		numaLogger.Debug("InterStepBufferService spec is unchanged. No updates will be performed")
		return nil
	}

	err = kubernetes.ApplyCRSpec(ctx, r.restConfig, obj, "interstepbufferservices")
	if err != nil {
		numaLogger.Errorf(err, "failed to apply CR: %v", err)
		isbServiceRollout.Status.MarkFailed("ApplyISBServiceFailure", err.Error())
		return err
	}

	// after the Apply, Get the ISBService so that we can propagate its health into our Status
	isbsvc, err := kubernetes.GetCR(ctx, r.restConfig, obj, "interstepbufferservices")
	if err != nil {
		numaLogger.Errorf(err, "failed to get ISBServices: %v", err)
		return err
	}

	processISBServiceStatus(ctx, isbsvc, isbServiceRollout)

	isbServiceRollout.Status.MarkRunning()

	return nil
}

func processISBServiceStatus(ctx context.Context, isbsvc *kubernetes.GenericObject, rollout *apiv1.ISBServiceRollout) {
	numaLogger := logger.FromContext(ctx)
	isbsvcStatus, err := kubernetes.ParseStatus(isbsvc)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Status from InterstepBuffer CR: %+v, %v", isbsvc, err)
		return
	}

	numaLogger.Debugf("isbsvc status: %+v", isbsvcStatus)

	isbSvcPhase := numaflowv1.ISBSvcPhase(isbsvcStatus.Phase)
	switch isbSvcPhase {
	case numaflowv1.ISBSvcPhaseFailed:
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcFailed", "ISBService Failed")
	case numaflowv1.ISBSvcPhaseUnknown:
		// this will have been set to Unknown in the call to InitConditions()
	default:
		rollout.Status.MarkChildResourcesHealthy()
	}
}

func (r *ISBServiceRolloutReconciler) needsUpdate(old, new *apiv1.ISBServiceRollout) bool {
	if old == nil {
		return true
	}

	// check for any fields we might update in the Spec - generally we'd only update a Finalizer or maybe something in the metadata
	// TODO: we would need to update this if we ever add anything else, like a label or annotation - unless there's a generic check that makes sense
	// Checking only the Finalizers and Annotations allows to have more control on updating only when certain changes have been made.
	// However, do we want to be also more specific? For instance, check specific finalizers and annotations (ex: .DeepEqual(old.Annotations["somekey"], new.Annotations["somekey"]))
	if !equality.Semantic.DeepEqual(old.Finalizers, new.Finalizers) || !equality.Semantic.DeepEqual(old.Annotations, new.Annotations) {
		return true
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *ISBServiceRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {

	controller, err := runtimecontroller.New(ControllerISBSVCRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch ISBServiceRollouts
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.ISBServiceRollout{}), &handler.EnqueueRequestForObject{}, predicate.GenerationChangedPredicate{}); err != nil {
		return err
	}

	// Watch InterStepBufferServices
	if err := controller.Watch(source.Kind(mgr.GetCache(), &numaflowv1.InterStepBufferService{}),
		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &apiv1.ISBServiceRollout{}, handler.OnlyControllerOwner()),
		predicate.ResourceVersionChangedPredicate{}); err != nil {
		return err
	}

	return nil
}
