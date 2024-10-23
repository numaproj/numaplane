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
	"reflect"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
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

	// maintain inProgressStrategies in memory and in MonoVertexRollout Status
	inProgressStrategyMgr *inProgressStrategyMgr
}

func NewMonoVertexRolloutReconciler(
	c client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *MonoVertexRolloutReconciler {

	r := &MonoVertexRolloutReconciler{
		c,
		s,
		restConfig,
		customMetrics,
		recorder,
		nil,
	}

	r.inProgressStrategyMgr = newInProgressStrategyMgr(
		// getRolloutStrategy function:
		func(ctx context.Context, rollout client.Object) *apiv1.UpgradeStrategy {
			monoVertexRollout := rollout.(*apiv1.MonoVertexRollout)

			if monoVertexRollout.Status.UpgradeInProgress != "" {
				return (*apiv1.UpgradeStrategy)(&monoVertexRollout.Status.UpgradeInProgress)
			} else {
				return nil
			}
		},
		// setRolloutStrategy function:
		func(ctx context.Context, rollout client.Object, strategy apiv1.UpgradeStrategy) {
			monoVertexRollout := rollout.(*apiv1.MonoVertexRollout)
			monoVertexRollout.Status.SetUpgradeInProgress(strategy)
		},
	)

	return r
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
		if err := r.processExistingMonoVertex(ctx, monoVertexRollout, existingMonoVertexDef, newMonoVertexDef, syncStartTime); err != nil {
			return ctrl.Result{}, fmt.Errorf("error processing existing MonoVertex: %v", err)
		}
	}

	// process status
	r.processMonoVertexStatus(ctx, existingMonoVertexDef, monoVertexRollout)

	return ctrl.Result{}, nil

}

func (r *MonoVertexRolloutReconciler) processExistingMonoVertex(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout,
	existingMonoVertexDef, newMonoVertexDef *kubernetes.GenericObject, syncStartTime time.Time) error {

	numaLogger := logger.FromContext(ctx)

	// determine if we're trying to update the MonoVertex spec
	// if it's a simple change, direct apply
	// if not and if user-preferred strategy is "Progressive", it will require Progressive rollout to perform the update with guaranteed no-downtime
	// and capability to rollback an unhealthy one
	mvNeedsToUpdate, upgradeStrategyType, err := usde.ResourceNeedsUpdating(ctx, newMonoVertexDef, existingMonoVertexDef)
	if err != nil {
		return err
	}
	numaLogger.
		WithValues("mvNeedsToUpdate", mvNeedsToUpdate, "upgradeStrategyType", upgradeStrategyType).
		Debug("Upgrade decision result")

	// set the Status appropriately to "Pending" or "Deployed"
	// if mvNeedsToUpdate - this means there's a mismatch between the desired ISBService spec and actual ISBService spec
	// Note that this will be reset to "Deployed" later on if a deployment occurs
	if mvNeedsToUpdate {
		monoVertexRollout.Status.MarkPending()
	} else {
		monoVertexRollout.Status.MarkDeployed(monoVertexRollout.Generation)
	}

	// is there currently an inProgressStrategy for the isbService? (This will override any new decision)
	inProgressStrategy := r.inProgressStrategyMgr.getStrategy(ctx, monoVertexRollout)
	inProgressStrategySet := (inProgressStrategy != apiv1.UpgradeStrategyNoOp)

	// if not, should we set one?
	if !inProgressStrategySet {
		if upgradeStrategyType == apiv1.UpgradeStrategyProgressive {
			inProgressStrategy = apiv1.UpgradeStrategyProgressive
			r.inProgressStrategyMgr.setStrategy(ctx, monoVertexRollout, inProgressStrategy)
		} else {
			numaLogger.Warnf("invalid inProgressStrategy=%v", inProgressStrategy)
			r.inProgressStrategyMgr.setStrategy(ctx, monoVertexRollout, apiv1.UpgradeStrategyNoOp)
		}
	}
	switch inProgressStrategy {
	case apiv1.UpgradeStrategyProgressive:
		if mvNeedsToUpdate {
			numaLogger.Debug("processing MonoVertex with Progressive")
			done, err := processResourceWithProgressive(ctx, monoVertexRollout, existingMonoVertexDef, r, r.restConfig)
			if err != nil {
				return err
			}
			if done {
				r.inProgressStrategyMgr.unsetStrategy(ctx, monoVertexRollout)
			}
		}

	case apiv1.UpgradeStrategyNoOp:
		if mvNeedsToUpdate {
			err := r.updateMonoVertex(ctx, monoVertexRollout, newMonoVertexDef)
			if err != nil {
				return err
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "update").Observe(time.Since(syncStartTime).Seconds())
		}
	}

	return nil
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

func parseMonoVertexStatus(obj *kubernetes.GenericObject) (numaflowv1.MonoVertexStatus, error) {
	if obj == nil || len(obj.Status.Raw) == 0 {
		return numaflowv1.MonoVertexStatus{}, nil
	}

	var status numaflowv1.MonoVertexStatus
	err := json.Unmarshal(obj.Status.Raw, &status)
	if err != nil {
		return numaflowv1.MonoVertexStatus{}, err
	}

	return status, nil
}

// the following functions enable MonoVertexRolloutReconciler to implement progressiveController interface
func (r *MonoVertexRolloutReconciler) listChildren(ctx context.Context, rolloutObject RolloutObject, labelSelector string, fieldSelector string) ([]*kubernetes.GenericObject, error) {
	pipelineRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	return kubernetes.ListLiveResource(
		ctx, r.restConfig, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "monovertices",
		pipelineRollout.Namespace, labelSelector, fieldSelector)
}

func (r *MonoVertexRolloutReconciler) createBaseChildDefinition(rolloutObject RolloutObject, name string) (*kubernetes.GenericObject, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
}

func (r *MonoVertexRolloutReconciler) getCurrentChildCount(rolloutObject RolloutObject) (int32, bool) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	if monoVertexRollout.Status.NameCount == nil {
		return int32(0), false
	} else {
		return *monoVertexRollout.Status.NameCount, true
	}
}

func (r *MonoVertexRolloutReconciler) updateCurrentChildCount(ctx context.Context, rolloutObject RolloutObject, nameCount int32) error {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	monoVertexRollout.Status.NameCount = &nameCount
	return r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
}

// increment the child count for the Rollout and return the count to use
func (r *MonoVertexRolloutReconciler) incrementChildCount(ctx context.Context, rolloutObject RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return int32(0), err
		}
	}

	// TODO: why in MonoVertexRolloutReconciler.calPipelineName() do we only update the Status subresource when it's 0?
	err := r.updateCurrentChildCount(ctx, rolloutObject, currentNameCount+1)
	if err != nil {
		return int32(0), err
	}
	return currentNameCount, nil
}

func (r *MonoVertexRolloutReconciler) childIsDrained(ctx context.Context, monoVertexDef *kubernetes.GenericObject) (bool, error) {
	monoVertexStatus, err := parseMonoVertexStatus(monoVertexDef)
	if err != nil {
		return false, fmt.Errorf("failed to parse MonoVertex Status from MonoVertex CR: %+v, %v", monoVertexDef, err)
	}
	monoVertexPhase := monoVertexStatus.Phase

	return monoVertexPhase == numaflowv1.MonoVertexPhasePaused /*&& monoVertexStatus.DrainedOnPause*/, nil // TODO: Numaflow should implement?
}

func (r *MonoVertexRolloutReconciler) drain(ctx context.Context, monoVertexDef *kubernetes.GenericObject) error {
	patchJson := `{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}`
	return kubernetes.PatchCR(ctx, r.restConfig, monoVertexDef, "monovertices", patchJson, k8stypes.MergePatchType)
}

// childNeedsUpdating() tests for essential equality, with any irrelevant fields eliminated from the comparison
func (r *MonoVertexRolloutReconciler) childNeedsUpdating(ctx context.Context, a *kubernetes.GenericObject, b *kubernetes.GenericObject) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	// remove lifecycle.desiredPhase field from comparison to test for equality
	mvWithoutDesiredPhaseA, err := withoutDesiredPhase(a)
	if err != nil {
		return false, err
	}
	mvWithoutDesiredPhaseB, err := withoutDesiredPhase(b)
	if err != nil {
		return false, err
	}
	numaLogger.Debugf("comparing specs: mvWithoutDesiredPhaseA=%v, mvWithoutDesiredPhaseB=%v\n", mvWithoutDesiredPhaseA, mvWithoutDesiredPhaseB)

	return !reflect.DeepEqual(mvWithoutDesiredPhaseA, mvWithoutDesiredPhaseB), nil
}
