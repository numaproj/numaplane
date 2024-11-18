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

package monovertexrollout

import (
	"context"
	"fmt"

	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	numaflowtypes "github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerMonoVertexRollout = "monovertex-rollout-controller"
)

// MonoVertexRolloutReconciler reconciles a MonoVertexRollout object
type MonoVertexRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder

	// maintain inProgressStrategies in memory and in MonoVertexRollout Status
	inProgressStrategyMgr *ctlrcommon.InProgressStrategyMgr
}

func NewMonoVertexRolloutReconciler(
	c client.Client,
	s *runtime.Scheme,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *MonoVertexRolloutReconciler {

	r := &MonoVertexRolloutReconciler{
		c,
		s,
		customMetrics,
		recorder,
		nil,
	}

	r.inProgressStrategyMgr = ctlrcommon.NewInProgressStrategyMgr(
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
	r.customMetrics.MonoVertexROSyncs.WithLabelValues().Inc()

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
	r.customMetrics.IncMonoVertexRollouts(monoVertexRollout.Name, monoVertexRollout.Namespace)
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
		if controllerutil.ContainsFinalizer(monoVertexRollout, common.FinalizerName) {
			controllerutil.RemoveFinalizer(monoVertexRollout, common.FinalizerName)
		}
		// generate metrics for MonoVertex deletion
		r.customMetrics.DecMonoVertexRollouts(monoVertexRollout.Name, monoVertexRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "delete").Observe(time.Since(startTime).Seconds())
		r.customMetrics.MonoVerticesRolloutHealth.DeleteLabelValues(monoVertexRollout.Namespace, monoVertexRollout.Name)
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(monoVertexRollout, common.FinalizerName) {
		controllerutil.AddFinalizer(monoVertexRollout, common.FinalizerName)
	}

	newMonoVertexDef, err := r.makeRunningMonoVertexDefinition(ctx, monoVertexRollout)
	if err != nil {
		return ctrl.Result{}, err
	}

	existingMonoVertexDef, err := kubernetes.GetResource(ctx, r.client, newMonoVertexDef.GroupVersionKind(),
		k8stypes.NamespacedName{Namespace: newMonoVertexDef.GetNamespace(), Name: newMonoVertexDef.GetName()})
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("MonoVertex %s/%s doesn't exist so creating", monoVertexRollout.Namespace, monoVertexRollout.Name)
			monoVertexRollout.Status.MarkPending()

			if err := kubernetes.CreateResource(ctx, r.client, newMonoVertexDef); err != nil {
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
		newMonoVertexDef, err = r.Merge(existingMonoVertexDef, newMonoVertexDef)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.processExistingMonoVertex(ctx, monoVertexRollout, existingMonoVertexDef, newMonoVertexDef, syncStartTime); err != nil {
			return ctrl.Result{}, fmt.Errorf("error processing existing MonoVertex: %v", err)
		}
	}

	// process status
	r.processMonoVertexStatus(ctx, existingMonoVertexDef, monoVertexRollout)

	return ctrl.Result{}, nil

}

func (r *MonoVertexRolloutReconciler) processExistingMonoVertex(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout,
	existingMonoVertexDef, newMonoVertexDef *unstructured.Unstructured, syncStartTime time.Time) error {

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
	// if mvNeedsToUpdate - this means there's a mismatch between the desired MonoVertex spec and actual MonoVertex spec
	// Note that this will be reset to "Deployed" later on if a deployment occurs
	if mvNeedsToUpdate {
		monoVertexRollout.Status.MarkPending()
	} else {
		monoVertexRollout.Status.MarkDeployed(monoVertexRollout.Generation)
	}

	// is there currently an inProgressStrategy for the MonoVertex? (This will override any new decision)
	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, monoVertexRollout)
	inProgressStrategySet := (inProgressStrategy != apiv1.UpgradeStrategyNoOp)

	// if not, should we set one?
	if !inProgressStrategySet {
		if upgradeStrategyType == apiv1.UpgradeStrategyProgressive {
			inProgressStrategy = apiv1.UpgradeStrategyProgressive
			r.inProgressStrategyMgr.SetStrategy(ctx, monoVertexRollout, inProgressStrategy)
		}
	}
	switch inProgressStrategy {
	case apiv1.UpgradeStrategyProgressive:

		// don't risk out-of-date cache while performing Progressive strategy - get
		// the most current version of the MonoVertex just in case
		existingMonoVertexDef, err = kubernetes.GetLiveResource(ctx, newMonoVertexDef, "monovertices")
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.WithValues("monoVertexDefinition", *existingMonoVertexDef).Warn("MonoVertex not found.")
			} else {
				return fmt.Errorf("error getting MonoVertex for status processing: %v", err)
			}
		}
		newMonoVertexDef, err = r.Merge(existingMonoVertexDef, newMonoVertexDef)
		if err != nil {
			return err
		}

		//if mvNeedsToUpdate {
		numaLogger.Debug("processing MonoVertex with Progressive")
		done, err := progressive.ProcessResourceWithProgressive(ctx, monoVertexRollout, existingMonoVertexDef, mvNeedsToUpdate, r, r.client)
		if err != nil {
			return err
		}
		if done {
			r.inProgressStrategyMgr.UnsetStrategy(ctx, monoVertexRollout)
		}
		//}

	default:
		if mvNeedsToUpdate {
			err := r.updateMonoVertex(ctx, monoVertexRollout, newMonoVertexDef)
			if err != nil {
				return err
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "update").Observe(time.Since(syncStartTime).Seconds())
		}
	}
	// clean up recyclable monovertices
	err = progressive.GarbageCollectChildren(ctx, monoVertexRollout, r, r.client)
	if err != nil {
		return err
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
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.MonoVertexRollout{},
		&handler.TypedEnqueueRequestForObject[*apiv1.MonoVertexRollout]{}, predicate.TypedGenerationChangedPredicate[*apiv1.MonoVertexRollout]{})); err != nil {
		return fmt.Errorf("failed to watch MonoVertexRollouts: %w", err)
	}

	// Watch MonoVertices
	monoVertexUns := &unstructured.Unstructured{}
	monoVertexUns.SetGroupVersionKind(schema.GroupVersionKind{
		Kind:    common.NumaflowMonoVertexKind,
		Group:   common.NumaflowAPIGroup,
		Version: common.NumaflowAPIVersion,
	})
	if err := controller.Watch(source.Kind(mgr.GetCache(), monoVertexUns,
		handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.MonoVertexRollout{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{})); err != nil {
		return fmt.Errorf("failed to watch MonoVertices: %w", err)
	}

	return nil
}

func (r *MonoVertexRolloutReconciler) Merge(existingMonoVertex, newMonoVertex *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	resultMonoVertex := existingMonoVertex.DeepCopy()

	var specAsMap map[string]interface{}
	if err := util.StructToStruct(newMonoVertex.Object["spec"], &specAsMap); err != nil {
		return resultMonoVertex, fmt.Errorf("failed to get spec from new MonoVertex: %w", err)
	}
	resultMonoVertex.Object["spec"] = specAsMap

	if newMonoVertex.GetAnnotations() != nil {
		resultMonoVertex.SetAnnotations(newMonoVertex.GetAnnotations())
	}

	if newMonoVertex.GetLabels() != nil {
		resultMonoVertex.SetLabels(newMonoVertex.GetLabels())
	}

	// Use the same replicas as the existing MonoVertex
	resultMonoVertex, err := withExistingMvtxReplicas(existingMonoVertex, resultMonoVertex)
	return resultMonoVertex, err
}

// withExistingMvtxReplicas sets the replicas of the new MonoVertex to the existing MonoVertex's replicas if it exists.
func withExistingMvtxReplicas(existingMonoVertex, newMonoVertex *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	existingReplicas, existing, err := unstructured.NestedFieldNoCopy(existingMonoVertex.Object, "spec", "replicas")
	if err != nil {
		return newMonoVertex, fmt.Errorf("failed to get replicas from existing MonoVertex: %w", err)
	}
	if existing {
		err = unstructured.SetNestedField(newMonoVertex.Object, existingReplicas, "spec", "replicas")
		if err != nil {
			return newMonoVertex, fmt.Errorf("failed to set replicas in new MonoVertex: %w", err)
		}
	}
	return newMonoVertex, nil

}

func (r *MonoVertexRolloutReconciler) processMonoVertexStatus(ctx context.Context, monoVertex *unstructured.Unstructured, rollout *apiv1.MonoVertexRollout) {
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
	} else if monoVertexPhase == numaflowv1.MonoVertexPhasePaused {
		rollout.Status.MarkChildResourcesHealthUnknown("MonoVertexUnknown", "MonoVertex Pausing - health unknown", rollout.Generation)
	} else if monoVertexPhase == numaflowv1.MonoVertexPhaseUnknown || monoVertexChildResourceStatus == "Unknown" {
		rollout.Status.MarkChildResourcesHealthUnknown("MonoVertexUnkown", "MonoVertex Phase Unknown", rollout.Generation)
	} else {
		rollout.Status.MarkChildResourcesHealthy(rollout.Generation)
	}

	r.setChildResourcesPauseCondition(rollout, monoVertexPhase)

}

func (r *MonoVertexRolloutReconciler) setChildResourcesPauseCondition(rollout *apiv1.MonoVertexRollout, mvtxPhase numaflowv1.MonoVertexPhase) {

	if mvtxPhase == numaflowv1.MonoVertexPhasePaused {
		// TODO: METRICS
		// if BeginTime hasn't been set yet, we must have just started pausing - set it
		// if rollout.Status.PauseStatus.LastPauseBeginTime == metav1.NewTime(initTime) || !rollout.Status.PauseStatus.LastPauseBeginTime.After(rollout.Status.PauseStatus.LastPauseEndTime.Time) {
		// 	rollout.Status.PauseStatus.LastPauseBeginTime = metav1.NewTime(time.Now())
		// }
		reason := fmt.Sprintf("MonoVertex%s", string(mvtxPhase))
		msg := fmt.Sprintf("MonoVertex %s", strings.ToLower(string(mvtxPhase)))
		// r.updatePauseMetric(rollout)
		rollout.Status.MarkMonoVertexPaused(reason, msg, rollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		// if (rollout.Status.PauseStatus.LastPauseBeginTime != metav1.NewTime(initTime)) && !rollout.Status.PauseStatus.LastPauseEndTime.After(rollout.Status.PauseStatus.LastPauseBeginTime.Time) {
		// 	rollout.Status.PauseStatus.LastPauseEndTime = metav1.NewTime(time.Now())
		// 	r.updatePauseMetric(rollout)
		// }
		rollout.Status.MarkMonoVertexUnpaused(rollout.Generation)
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

func (r *MonoVertexRolloutReconciler) updateMonoVertex(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, newMonoVertexDef *unstructured.Unstructured) error {
	err := kubernetes.UpdateResource(ctx, r.client, newMonoVertexDef)
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
	r.customMetrics.MonoVertexROSyncErrors.WithLabelValues().Inc()
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

// create the definition for the MonoVertex child of the Rollout which is labeled "promoted"
func (r *MonoVertexRolloutReconciler) makeRunningMonoVertexDefinition(
	ctx context.Context,
	monoVertexRollout *apiv1.MonoVertexRollout,
) (*unstructured.Unstructured, error) {
	monoVertexName, err := progressive.GetChildName(ctx, monoVertexRollout, r, string(common.LabelValueUpgradePromoted))
	if err != nil {
		return nil, err
	}

	metadata, err := getBaseMonoVertexMetadata(monoVertexRollout)
	if err != nil {
		return nil, err
	}
	metadata.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)

	return r.makeMonoVertexDefinition(monoVertexRollout, monoVertexName, metadata)
}

func (r *MonoVertexRolloutReconciler) makeMonoVertexDefinition(
	monoVertexRollout *apiv1.MonoVertexRollout,
	monoVertexName string,
	metadata apiv1.Metadata,
) (*unstructured.Unstructured, error) {
	monoVertexDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	monoVertexDef.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)
	monoVertexDef.SetName(monoVertexName)
	monoVertexDef.SetNamespace(monoVertexRollout.Namespace)
	monoVertexDef.SetLabels(metadata.Labels)
	monoVertexDef.SetAnnotations(metadata.Annotations)
	monoVertexDef.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(monoVertexRollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)})
	var monoVertexSpec map[string]interface{}
	if err := util.StructToStruct(monoVertexRollout.Spec.MonoVertex.Spec, &monoVertexSpec); err != nil {
		return nil, err
	}
	monoVertexDef.Object["spec"] = monoVertexSpec

	return monoVertexDef, nil
}

// take the Metadata (Labels and Annotations) specified in the MonoVertexRollout plus any others that apply to all MonoVertices
func getBaseMonoVertexMetadata(monoVertexRollout *apiv1.MonoVertexRollout) (apiv1.Metadata, error) {
	labelMapping := map[string]string{}
	for key, val := range monoVertexRollout.Spec.MonoVertex.Labels {
		labelMapping[key] = val
	}
	labelMapping[common.LabelKeyParentRollout] = monoVertexRollout.Name

	return apiv1.Metadata{Labels: labelMapping, Annotations: monoVertexRollout.Spec.MonoVertex.Annotations}, nil

}

// the following functions enable MonoVertexRolloutReconciler to implement progressiveController interface
func (r *MonoVertexRolloutReconciler) ListChildren(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, labelSelector string, fieldSelector string) (*unstructured.UnstructuredList, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	return kubernetes.ListLiveResource(
		ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "monovertices",
		monoVertexRollout.Namespace, labelSelector, fieldSelector)
}

func (r *MonoVertexRolloutReconciler) CreateBaseChildDefinition(rolloutObject ctlrcommon.RolloutObject, name string) (*unstructured.Unstructured, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	metadata, err := getBaseMonoVertexMetadata(monoVertexRollout)
	if err != nil {
		return nil, err
	}
	return r.makeMonoVertexDefinition(monoVertexRollout, name, metadata)
}

func (r *MonoVertexRolloutReconciler) getCurrentChildCount(rolloutObject ctlrcommon.RolloutObject) (int32, bool) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	if monoVertexRollout.Status.NameCount == nil {
		return int32(0), false
	} else {
		return *monoVertexRollout.Status.NameCount, true
	}
}

func (r *MonoVertexRolloutReconciler) updateCurrentChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, nameCount int32) error {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	monoVertexRollout.Status.NameCount = &nameCount
	return r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
}

// increment the child count for the Rollout and return the count to use
func (r *MonoVertexRolloutReconciler) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return int32(0), err
		}
	}

	err := r.updateCurrentChildCount(ctx, rolloutObject, currentNameCount+1)
	if err != nil {
		return int32(0), err
	}
	return currentNameCount, nil
}

func (r *MonoVertexRolloutReconciler) ChildIsDrained(ctx context.Context, monoVertexDef *unstructured.Unstructured) (bool, error) {
	monoVertexStatus, err := numaflowtypes.ParseMonoVertexStatus(monoVertexDef)
	if err != nil {
		return false, fmt.Errorf("failed to parse MonoVertex Status from MonoVertex CR: %+v, %v", monoVertexDef, err)
	}
	monoVertexPhase := monoVertexStatus.Phase

	return monoVertexPhase == "Paused" /*&& monoVertexStatus.DrainedOnPause*/, nil // TODO: should Numaflow implement?
}

func (r *MonoVertexRolloutReconciler) Drain(ctx context.Context, monoVertexDef *unstructured.Unstructured) error {
	patchJson := `{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}`
	return kubernetes.PatchResource(ctx, r.client, monoVertexDef, patchJson, k8stypes.MergePatchType)
}

// ChildNeedsUpdating() tests for essential equality, with any irrelevant fields eliminated from the comparison
func (r *MonoVertexRolloutReconciler) ChildNeedsUpdating(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	// remove lifecycle.desiredPhase field from comparison to test for equality
	mvWithoutDesiredPhaseA, err := numaflowtypes.WithoutDesiredPhase(from)
	if err != nil {
		return false, err
	}
	mvWithoutDesiredPhaseB, err := numaflowtypes.WithoutDesiredPhase(to)
	if err != nil {
		return false, err
	}
	numaLogger.Debugf("comparing specs: mvWithoutDesiredPhaseA=%v, mvWithoutDesiredPhaseB=%v\n", mvWithoutDesiredPhaseA, mvWithoutDesiredPhaseB)

	specsEqual := reflect.DeepEqual(mvWithoutDesiredPhaseA, mvWithoutDesiredPhaseB)
	numaLogger.Debugf("specsEqual: %t, pipelineWithoutDesiredPhaseA=%v, pipelineWithoutDesiredPhaseB=%v\n",
		specsEqual, mvWithoutDesiredPhaseA, mvWithoutDesiredPhaseB)
	labelsEqual := reflect.DeepEqual(from.Object["labels"], to.Object["labels"])
	numaLogger.Debugf("labelsEqual: %t, from Labels=%v, to Labels=%v", labelsEqual, from.Object["labels"], to.Object["labels"])
	annotationsEqual := reflect.DeepEqual(from.Object["annotations"], to.Object["annotations"])
	numaLogger.Debugf("annotationsEqual: %t, from Annotations=%v, to Annotations=%v", annotationsEqual, from.Object["annotations"], to.Object["annotations"])

	return !specsEqual || !labelsEqual || !annotationsEqual, nil

}
