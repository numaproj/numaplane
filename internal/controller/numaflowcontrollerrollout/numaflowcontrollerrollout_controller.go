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

package numaflowcontrollerrollout

import (
	"context"
	"fmt"
	"reflect"
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

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/pipelinerollout"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerNumaflowControllerRollout = "numaflow-controller-rollout-controller"
)

// NumaflowControllerRolloutReconciler reconciles a NumaflowControllerRollout object
type NumaflowControllerRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	customMetrics *metrics.CustomMetrics

	// the recorder is used to record events
	recorder record.EventRecorder

	// maintain inProgressStrategies in memory and in NumaflowControllerRollout Status
	inProgressStrategyMgr *ctlrcommon.InProgressStrategyMgr
}

func NewNumaflowControllerRolloutReconciler(
	cli client.Client,
	scheme *runtime.Scheme,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *NumaflowControllerRolloutReconciler {

	inProgressStrategyMgr := ctlrcommon.NewInProgressStrategyMgr(
		// getRolloutStrategy function:
		func(ctx context.Context, rollout client.Object) *apiv1.UpgradeStrategy {
			numaflowControllerRollout := rollout.(*apiv1.NumaflowControllerRollout)

			if numaflowControllerRollout.Status.UpgradeInProgress != "" {
				return (*apiv1.UpgradeStrategy)(&numaflowControllerRollout.Status.UpgradeInProgress)
			}

			return nil
		},
		// setRolloutStrategy function:
		func(ctx context.Context, rollout client.Object, strategy apiv1.UpgradeStrategy) {
			numaflowControllerRollout := rollout.(*apiv1.NumaflowControllerRollout)
			numaflowControllerRollout.Status.SetUpgradeInProgress(strategy)
		},
	)

	return &NumaflowControllerRolloutReconciler{
		cli,
		scheme,
		customMetrics,
		recorder,
		inProgressStrategyMgr,
	}
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollerrollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollerrollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollerrollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *NumaflowControllerRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	syncStartTime := time.Now()
	numaLogger := logger.GetBaseLogger().WithName("numaflowcontrollerrollout-reconciler").WithValues("numaflowcontrollerrollout", req.NamespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)
	r.customMetrics.NumaflowControllerRolloutSyncs.WithLabelValues().Inc()

	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, numaflowControllerRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(numaflowControllerRollout, err, "GetNumaflowControllerRolloutFailed", "Failed to get NumaflowControllerRollout")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	numaflowControllerRolloutOrig := numaflowControllerRollout
	numaflowControllerRollout = numaflowControllerRolloutOrig.DeepCopy()

	numaflowControllerRollout.Status.Init(numaflowControllerRollout.Generation)

	result, err := r.reconcile(ctx, numaflowControllerRollout, req.Namespace, syncStartTime)
	if err != nil {
		r.ErrorHandler(numaflowControllerRollout, err, "ReconcileFailed", "Failed to reconcile NumaflowControllerRollout")
		statusUpdateErr := r.updateNumaflowControllerRolloutStatusToFailed(ctx, numaflowControllerRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(numaflowControllerRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of NumaflowControllerRollout")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(numaflowControllerRolloutOrig, numaflowControllerRollout) {
		numaflowControllerRolloutStatus := numaflowControllerRollout.Status
		if err := r.client.Update(ctx, numaflowControllerRollout); err != nil {
			r.ErrorHandler(numaflowControllerRollout, err, "UpdateFailed", "Failed to update NumaflowControllerRollout")
			statusUpdateErr := r.updateNumaflowControllerRolloutStatusToFailed(ctx, numaflowControllerRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(numaflowControllerRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of NumaflowControllerRollout")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		numaflowControllerRollout.Status = numaflowControllerRolloutStatus
	}

	// Update the Status subresource
	if numaflowControllerRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updateNumaflowControllerRolloutStatus(ctx, numaflowControllerRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(numaflowControllerRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of NumaflowControllerRollout")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate metrics for NumaflowControllerRollout
	r.customMetrics.IncNumaflowControllerRollouts(numaflowControllerRollout.Name, numaflowControllerRollout.Namespace)

	r.recorder.Eventf(numaflowControllerRollout, corev1.EventTypeNormal, "ReconcilationSuccessful", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

	return result, nil
}

// reconcile does the real logic
func (r *NumaflowControllerRolloutReconciler) reconcile(
	ctx context.Context,
	nfcRollout *apiv1.NumaflowControllerRollout,
	namespace string,
	syncStartTime time.Time,
) (ctrl.Result, error) {
	startTime := time.Now()
	numaLogger := logger.FromContext(ctx)

	defer func() {
		if nfcRollout.Status.IsHealthy() {
			r.customMetrics.NumaflowControllerRolloutsHealth.WithLabelValues(nfcRollout.Namespace, nfcRollout.Name).Set(1)
		} else {
			r.customMetrics.NumaflowControllerRolloutsHealth.WithLabelValues(nfcRollout.Namespace, nfcRollout.Name).Set(0)
		}
	}()

	controllerKey := ppnd.GetPauseModule().GetNumaflowControllerKey(namespace)

	if !nfcRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting NumaflowControllerRollout")
		r.recorder.Eventf(nfcRollout, corev1.EventTypeNormal, "Deleting", "Deleting NumaflowControllerRollout")
		if controllerutil.ContainsFinalizer(nfcRollout, common.FinalizerName) {
			ppnd.GetPauseModule().DeletePauseRequest(controllerKey)
			controllerutil.RemoveFinalizer(nfcRollout, common.FinalizerName)
		}

		// generate the metrics for the numaflow controller rollout deletion.
		r.customMetrics.DecNumaflowControllerRollouts(nfcRollout.Name, nfcRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "delete").Observe(time.Since(startTime).Seconds())
		r.customMetrics.NumaflowControllerRolloutsHealth.DeleteLabelValues(nfcRollout.Namespace, nfcRollout.Name)

		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(nfcRollout, common.FinalizerName) {
		controllerutil.AddFinalizer(nfcRollout, common.FinalizerName)
	}

	_, pauseRequestExists := ppnd.GetPauseModule().GetPauseRequest(controllerKey)
	if !pauseRequestExists {
		// this is just creating an entry in the map if it doesn't already exist
		ppnd.GetPauseModule().NewPauseRequest(controllerKey)
	}

	newNumaflowControllerDef, err := generateNewNumaflowControllerDef(nfcRollout)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error generating NumaflowController: %v", err)
	}

	// Using unstructured object since USDE needs unstructured type to extract paths and perform comparisons.
	// Also, keeping the code consistent between ISBSvcRollout and NumaflowControllerRollout for easier maintainability
	// and to be able to possibly reduce code duplication at some point.
	existingNumaflowControllerDef, err := kubernetes.GetResource(ctx, r.client, newNumaflowControllerDef.GroupVersionKind(),
		k8stypes.NamespacedName{Namespace: newNumaflowControllerDef.GetNamespace(), Name: newNumaflowControllerDef.GetName()})
	if err != nil {
		// create an object as it doesn't exist
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("NumaflowController %s/%s doesn't exist so creating", nfcRollout.Namespace, nfcRollout.Name)
			nfcRollout.Status.MarkPending()

			if err = kubernetes.CreateResource(ctx, r.client, newNumaflowControllerDef); err != nil {
				return ctrl.Result{}, fmt.Errorf("error creating NumaflowController: %v", err)
			}

			nfcRollout.Status.MarkDeployed(nfcRollout.Generation)
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "create").Observe(time.Since(startTime).Seconds())
			return ctrl.Result{}, nil
		} else {
			return ctrl.Result{}, fmt.Errorf("error getting NumaflowController: %v", err)
		}
	}

	// Object already exists: perform logic related to updating
	newNumaflowControllerDef = r.merge(existingNumaflowControllerDef, newNumaflowControllerDef)
	needsRequeue, err := r.processExistingNumaflowController(ctx, nfcRollout, existingNumaflowControllerDef, newNumaflowControllerDef, syncStartTime)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error processing existing NumaflowController: %v", err)
	}
	if needsRequeue {
		return common.DefaultDelayedRequeue, nil
	}

	return ctrl.Result{}, nil
}

// for the purpose of logging
func (r *NumaflowControllerRolloutReconciler) GetChildTypeString() string {
	return "numaflowcontroller"
}

func (r *NumaflowControllerRolloutReconciler) GetPipelineList(ctx context.Context, rolloutNamespace string, rolloutName string) (*unstructured.UnstructuredList, error) {
	gvk := schema.GroupVersionKind{Group: common.NumaflowAPIGroup, Version: common.NumaflowAPIVersion, Kind: common.NumaflowPipelineKind}
	// List all the pipelines since they are all managed by the same single NumaplaneController
	return kubernetes.ListResources(ctx, r.client, gvk, rolloutNamespace)
}

func (r *NumaflowControllerRolloutReconciler) GetRolloutKey(rolloutNamespace string, rolloutName string) string {
	// TODO: we may want to also use the rolloutName to identify and store the NumaflowControllers
	return ppnd.GetPauseModule().GetNumaflowControllerKey(rolloutNamespace)
}

// take the existing NumaflowController and merge anything needed from the new NumaflowController definition
func (r *NumaflowControllerRolloutReconciler) merge(existingNumaflowController, newNumaflowController *unstructured.Unstructured) *unstructured.Unstructured {
	resultNumaflowController := existingNumaflowController.DeepCopy()
	resultNumaflowController.Object["spec"] = newNumaflowController.Object["spec"]
	resultNumaflowController.SetAnnotations(util.MergeMaps(existingNumaflowController.GetAnnotations(), newNumaflowController.GetAnnotations()))
	resultNumaflowController.SetLabels(util.MergeMaps(existingNumaflowController.GetLabels(), newNumaflowController.GetLabels()))
	return resultNumaflowController
}

// process an existing NumaflowController
// return:
// - true if needs a requeue
// - error if any
func (r *NumaflowControllerRolloutReconciler) processExistingNumaflowController(ctx context.Context, nfcRollout *apiv1.NumaflowControllerRollout,
	existingNumaflowControllerDef, newNumaflowControllerDef *unstructured.Unstructured, syncStartTime time.Time) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// update our Status with the NumaflowController's Status
	err := r.processNumaflowControllerStatus(ctx, nfcRollout, existingNumaflowControllerDef)
	if err != nil {
		return false, fmt.Errorf("error determining the NumaflowController status: %v", err)
	}

	_, numaflowControllerIsUpdating, err := r.isNumaflowControllerUpdating(ctx, nfcRollout, existingNumaflowControllerDef)
	if err != nil {
		return false, fmt.Errorf("error determining if NumaflowController is updating: %v", err)
	}

	// determine if we're trying to update the NumaflowController spec
	// if it's a simple change, direct apply
	// if not, it will require PPND or Progressive
	numaflowControllerNeedsToUpdate, upgradeStrategyType, err := usde.ResourceNeedsUpdating(ctx, newNumaflowControllerDef, existingNumaflowControllerDef)
	if err != nil {
		return false, err
	}

	numaLogger.
		WithValues("numaflowControllerNeedsToUpdate", numaflowControllerNeedsToUpdate, "upgradeStrategyType", upgradeStrategyType).
		Debug("Upgrade decision result")

	// set the Status appropriately to "Pending" or "Deployed"
	// if numaflowControllerNeedsToUpdate - this means there's a mismatch between the desired NumaflowController spec and actual NumaflowController spec
	// Note that this will be reset to "Deployed" later on if a deployment occurs
	if numaflowControllerNeedsToUpdate {
		nfcRollout.Status.MarkPending()
	} else {
		nfcRollout.Status.MarkDeployed(nfcRollout.Generation)
	}

	// is there currently an inProgressStrategy for the NumaflowController? (This will override any new decision)
	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, nfcRollout)
	inProgressStrategySet := (inProgressStrategy != apiv1.UpgradeStrategyNoOp)

	// if not, should we set one?
	if !inProgressStrategySet {
		if upgradeStrategyType == apiv1.UpgradeStrategyPPND {
			inProgressStrategy = apiv1.UpgradeStrategyPPND
			r.inProgressStrategyMgr.SetStrategy(ctx, nfcRollout, inProgressStrategy)
		}
		if upgradeStrategyType == apiv1.UpgradeStrategyProgressive {
			inProgressStrategy = apiv1.UpgradeStrategyProgressive
			r.inProgressStrategyMgr.SetStrategy(ctx, nfcRollout, inProgressStrategy)
		}
	}

	switch inProgressStrategy {
	case apiv1.UpgradeStrategyPPND:
		done, err := ppnd.ProcessChildObjectWithPPND(ctx, r.client, nfcRollout, r, numaflowControllerNeedsToUpdate, numaflowControllerIsUpdating, func() error {
			r.recorder.Eventf(nfcRollout, corev1.EventTypeNormal, "PipelinesPaused", "All Pipelines have paused for NumaflowController update")
			err = r.updateNumaflowController(ctx, nfcRollout, newNumaflowControllerDef)
			if err != nil {
				return fmt.Errorf("error updating NumaflowController, %s: %v", apiv1.UpgradeStrategyPPND, err)
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "update").Observe(time.Since(syncStartTime).Seconds())
			return nil
		},
			pipelinerollout.PipelineROReconciler.EnqueuePipeline)
		if err != nil {
			return false, err
		}
		if done {
			r.inProgressStrategyMgr.UnsetStrategy(ctx, nfcRollout)
		} else {
			// requeue if done with PPND is false
			return true, nil
		}
	// TODO: Progressive strategy should ideally be creating a second parallel NumaflowController, and all Pipelines should be on it;
	// for now we just create a 2nd NumaflowControllerRollout, so we need the Apply path to work
	case apiv1.UpgradeStrategyNoOp, apiv1.UpgradeStrategyProgressive:
		if numaflowControllerNeedsToUpdate {
			// update NumaflowController
			err = r.updateNumaflowController(ctx, nfcRollout, newNumaflowControllerDef)
			if err != nil {
				return false, fmt.Errorf("error updating NumaflowController, %s: %v", apiv1.UpgradeStrategyNoOp, err)
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "update").Observe(time.Since(syncStartTime).Seconds())
		}
	default:
		return false, fmt.Errorf("%v strategy not recognized", inProgressStrategy)
	}

	return false, nil
}

func (r *NumaflowControllerRolloutReconciler) updateNumaflowController(ctx context.Context, nfcRollout *apiv1.NumaflowControllerRollout, newNumaflowControllerDef *unstructured.Unstructured) error {
	if err := kubernetes.UpdateResource(ctx, r.client, newNumaflowControllerDef); err != nil {
		return err
	}

	nfcRollout.Status.MarkDeployed(nfcRollout.Generation)
	return nil
}

func (r *NumaflowControllerRolloutReconciler) MarkRolloutPaused(ctx context.Context, rollout client.Object, paused bool) {
	nfcRollout := rollout.(*apiv1.NumaflowControllerRollout)

	uninitialized := metav1.NewTime(time.Time{})

	if paused {
		// if BeginTime hasn't been set yet, we must have just started pausing - set it
		if nfcRollout.Status.PauseRequestStatus.LastPauseBeginTime == uninitialized || !nfcRollout.Status.PauseRequestStatus.LastPauseBeginTime.After(nfcRollout.Status.PauseRequestStatus.LastPauseEndTime.Time) {
			nfcRollout.Status.PauseRequestStatus.LastPauseBeginTime = metav1.NewTime(time.Now())
		}
		r.updatePauseMetric(nfcRollout)
		nfcRollout.Status.MarkPausingPipelines(nfcRollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		if (nfcRollout.Status.PauseRequestStatus.LastPauseBeginTime != uninitialized) && !nfcRollout.Status.PauseRequestStatus.LastPauseEndTime.After(nfcRollout.Status.PauseRequestStatus.LastPauseBeginTime.Time) {
			nfcRollout.Status.PauseRequestStatus.LastPauseEndTime = metav1.NewTime(time.Now())
			r.updatePauseMetric(nfcRollout)
		}
		nfcRollout.Status.MarkUnpausingPipelines(nfcRollout.Generation)
	}
}

func (r *NumaflowControllerRolloutReconciler) updatePauseMetric(nfcRollout *apiv1.NumaflowControllerRollout) {
	timeElapsed := time.Since(nfcRollout.Status.PauseRequestStatus.LastPauseBeginTime.Time)
	r.customMetrics.NumaflowControllerRolloutPausedSeconds.WithLabelValues(nfcRollout.Name).Set(timeElapsed.Seconds())
}

// return:
// - whether NumaflowController needs to update
// - whether it's in the process of being updated
// - error if any
func (r *NumaflowControllerRolloutReconciler) isNumaflowControllerUpdating(ctx context.Context, numaflowControllerRollout *apiv1.NumaflowControllerRollout, existingNumaflowControllerDef *unstructured.Unstructured) (bool, bool, error) {

	numaflowControllerReconciled, _, err := r.isNumaflowControllerReconciled(ctx, existingNumaflowControllerDef)
	if err != nil {
		return false, false, err
	}

	existingSpecAsMap, found, err := unstructured.NestedMap(existingNumaflowControllerDef.Object, "spec")
	if err != nil || !found {
		return false, false, err
	}

	newSpecAsMap := make(map[string]interface{})
	err = util.StructToStruct(&numaflowControllerRollout.Spec.Controller, &newSpecAsMap)
	if err != nil {
		return false, false, err
	}

	NumaflowControllerNeedsToUpdate := !reflect.DeepEqual(existingSpecAsMap, newSpecAsMap)

	return NumaflowControllerNeedsToUpdate, !numaflowControllerReconciled, nil
}

// determine if the NumaflowController, including its underlying Deployment, has been reconciled
// so, this requires:
// 1. NumaflowController.Status.ObservedGeneration == NumaflowController.Generation
// 2. Deployment.Status.ObservedGeneration == Deployment.Generation
// 3. Deployment.Status.UpdatedReplicas == Deployment.Spec.Replicas
func (r *NumaflowControllerRolloutReconciler) isNumaflowControllerReconciled(ctx context.Context, numaflowController *unstructured.Unstructured) (bool, string, error) {
	numaLogger := logger.FromContext(ctx)

	numaflowControllerStatus, err := kubernetes.ParseStatus(numaflowController)
	if err != nil {
		return false, "", fmt.Errorf("failed to parse Status from NumaflowController CR: %+v, %v", numaflowController, err)
	}
	numaLogger.Debugf("numaflowController status: %+v", numaflowControllerStatus)

	var nfcStatus apiv1.NumaflowControllerStatus
	err = util.StructToStruct(numaflowControllerStatus, &nfcStatus)
	if err != nil {
		return false, "", fmt.Errorf("failed to convert NumaflowController Status: %+v, %v", numaflowController, err)
	}

	healthyChildCond := nfcStatus.GetCondition(apiv1.ConditionChildResourceHealthy)

        ncProgressing := healthyChildCond.Reason == apiv1.ProgressingReasonString
	numaflowControllerReconciled := numaflowController.GetGeneration() <= numaflowControllerStatus.ObservedGeneration && !ncProgressing

	if !numaflowControllerReconciled {
		return false, "Mismatch between NumaflowController Generation and ObservedGeneration", nil
	}

	return true, "", nil
}

func (r *NumaflowControllerRolloutReconciler) processNumaflowControllerStatus(
	ctx context.Context,
	nfcRollout *apiv1.NumaflowControllerRollout,
	existingNumaflowControllerDef *unstructured.Unstructured,
) error {

	if existingNumaflowControllerDef != nil && len(existingNumaflowControllerDef.Object) > 0 {
		var existingNumaflowControllerStatus apiv1.NumaflowControllerStatus
		err := util.StructToStruct(existingNumaflowControllerDef.Object["status"], &existingNumaflowControllerStatus)
		if err != nil {
			return err
		}

		healthyChildCond := existingNumaflowControllerStatus.GetCondition(apiv1.ConditionChildResourceHealthy)

		if existingNumaflowControllerStatus.IsHealthy() &&
			healthyChildCond != nil && existingNumaflowControllerDef.GetGeneration() <= healthyChildCond.ObservedGeneration &&
			healthyChildCond.Status == metav1.ConditionTrue {

			nfcRollout.Status.MarkChildResourcesHealthy(nfcRollout.Generation)
		} else {
			if healthyChildCond != nil {
				nfcRollout.Status.MarkChildResourcesUnhealthy(healthyChildCond.Reason, healthyChildCond.Message, nfcRollout.Generation)
			} else {
				nfcRollout.Status.MarkChildResourcesUnhealthy(apiv1.ProgressingReasonString, "Progressing", nfcRollout.Generation)
			}
		}
	}

	// check if PPND strategy is requesting Pipelines to pause, and set true/false
	// (currently, only PPND is accounted for as far as system pausing, not Progressive)
	r.MarkRolloutPaused(ctx, nfcRollout, ppnd.IsRequestingPause(r, nfcRollout))

	return nil
}

func (r *NumaflowControllerRolloutReconciler) needsUpdate(old, new *apiv1.NumaflowControllerRollout) bool {
	if old == nil {
		return true
	}

	// check for any fields we might update in the Spec - generally we'd only update a Finalizer or maybe something in the metadata
	// TODO: we would need to update this if we ever add anything else, like a label or annotation - unless there's a generic check that makes sense
	if !equality.Semantic.DeepEqual(old.Finalizers, new.Finalizers) {
		return true
	}

	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *NumaflowControllerRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	controller, err := runtimecontroller.New(ControllerNumaflowControllerRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	// Watch NumaflowControllerRollout
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowControllerRollout{},
		&handler.TypedEnqueueRequestForObject[*apiv1.NumaflowControllerRollout]{}, predicate.TypedGenerationChangedPredicate[*apiv1.NumaflowControllerRollout]{})); err != nil {
		return fmt.Errorf("failed to watch NumaflowControllerRollout: %w", err)
	}

	// Watch NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowController{},
		handler.TypedEnqueueRequestForOwner[*apiv1.NumaflowController](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowControllerRollout{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*apiv1.NumaflowController]{})); err != nil {
		return fmt.Errorf("failed to watch NumaflowController: %v", err)
	}

	return nil
}

func (r *NumaflowControllerRolloutReconciler) updateNumaflowControllerRolloutStatus(ctx context.Context, nfcRollout *apiv1.NumaflowControllerRollout) error {
	return r.client.Status().Update(ctx, nfcRollout)
}

func (r *NumaflowControllerRolloutReconciler) updateNumaflowControllerRolloutStatusToFailed(ctx context.Context, nfcRollout *apiv1.NumaflowControllerRollout, err error) error {
	nfcRollout.Status.MarkFailed(err.Error())
	return r.updateNumaflowControllerRolloutStatus(ctx, nfcRollout)
}

func (r *NumaflowControllerRolloutReconciler) ErrorHandler(nfcRollout *apiv1.NumaflowControllerRollout, err error, reason, msg string) {
	r.customMetrics.NumaflowControllerRolloutSyncErrors.WithLabelValues().Inc()
	r.recorder.Eventf(nfcRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

func generateNewNumaflowControllerDef(nfcRollout *apiv1.NumaflowControllerRollout) (*unstructured.Unstructured, error) {
	newNumaflowControllerDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	newNumaflowControllerDef.SetName(nfcRollout.Name)
	newNumaflowControllerDef.SetNamespace(nfcRollout.Namespace)
	newNumaflowControllerDef.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(nfcRollout.GetObjectMeta(), apiv1.NumaflowControllerRolloutGroupVersionKind)})
	newNumaflowControllerDef.SetGroupVersionKind(apiv1.NumaflowControllerGroupVersionKind)

	// Update spec of NumaflowController to match the NumaflowControllerRollout spec
	var numaflowControllerSpec map[string]interface{}
	if err := util.StructToStruct(nfcRollout.Spec.Controller, &numaflowControllerSpec); err != nil {
		return nil, err
	}
	newNumaflowControllerDef.Object["spec"] = numaflowControllerSpec

	return newNumaflowControllerDef, nil
}
