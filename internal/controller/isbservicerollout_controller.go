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
	"errors"
	"fmt"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/rest"
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
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerISBSVCRollout = "isbsvc-rollout-controller"
)

// ISBServiceRolloutReconciler reconciles an ISBServiceRollout object
type ISBServiceRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	restConfig    *rest.Config
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder
}

func NewISBServiceRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *ISBServiceRolloutReconciler {

	return &ISBServiceRolloutReconciler{
		client,
		s,
		restConfig,
		customMetrics,
		recorder,
	}
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=isbservicerollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=isbservicerollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=isbservicerollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *ISBServiceRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	syncStartTime := time.Now()
	numaLogger := logger.GetBaseLogger().WithName("isbservicerollout-reconciler").WithValues("isbservicerollout", req.NamespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)
	r.customMetrics.ISBServicesSynced.WithLabelValues().Inc()

	isbServiceRollout := &apiv1.ISBServiceRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, isbServiceRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(isbServiceRollout, err, "GetISBServiceFailed", "Failed to get isb service rollout")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	isbServiceRolloutOrig := isbServiceRollout
	isbServiceRollout = isbServiceRolloutOrig.DeepCopy()

	isbServiceRollout.Status.Init(isbServiceRollout.Generation)

	result, err := r.reconcile(ctx, isbServiceRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(isbServiceRollout, err, "ReconcileFailed", "Failed to reconcile isb service rollout")
		statusUpdateErr := r.updateISBServiceRolloutStatusToFailed(ctx, isbServiceRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(isbServiceRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update isb service rollout status")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(isbServiceRolloutOrig, isbServiceRollout) {
		isbServiceRolloutStatus := isbServiceRollout.Status
		if err := r.client.Update(ctx, isbServiceRollout); err != nil {
			r.ErrorHandler(isbServiceRollout, err, "UpdateFailed", "Failed to update isb service rollout")
			statusUpdateErr := r.updateISBServiceRolloutStatusToFailed(ctx, isbServiceRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(isbServiceRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update isb service rollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		isbServiceRollout.Status = isbServiceRolloutStatus
	}

	// Update the Status subresource
	if isbServiceRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updateISBServiceRolloutStatus(ctx, isbServiceRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(isbServiceRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update isb service rollout status")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate metrics for ISB Service.
	r.customMetrics.IncISBServiceMetrics(isbServiceRollout.Name, isbServiceRollout.Namespace)
	r.recorder.Eventf(isbServiceRollout, corev1.EventTypeNormal, "ReconcilationSuccessful", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

	return result, nil
}

// reconcile does the real logic
func (r *ISBServiceRolloutReconciler) reconcile(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, syncStartTime time.Time) (ctrl.Result, error) {
	startTime := time.Now()
	numaLogger := logger.FromContext(ctx)

	defer func() {
		if isbServiceRollout.Status.IsHealthy() {
			r.customMetrics.ISBServicesHealth.WithLabelValues(isbServiceRollout.Namespace, isbServiceRollout.Name).Set(1)
		} else {
			r.customMetrics.ISBServicesHealth.WithLabelValues(isbServiceRollout.Namespace, isbServiceRollout.Name).Set(0)
		}
	}()

	isbsvcKey := GetPauseModule().getISBServiceKey(isbServiceRollout.Namespace, isbServiceRollout.Name)

	// is isbServiceRollout being deleted? need to remove the finalizer so it can
	// (OwnerReference will delete the underlying ISBService through Cascading deletion)
	if !isbServiceRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting ISBServiceRollout")
		if controllerutil.ContainsFinalizer(isbServiceRollout, finalizerName) {
			GetPauseModule().deletePauseRequest(isbsvcKey)
			controllerutil.RemoveFinalizer(isbServiceRollout, finalizerName)
		}
		// generate metrics for ISB Service deletion.
		r.customMetrics.DecISBServiceMetrics(isbServiceRollout.Name, isbServiceRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "delete").Observe(time.Since(startTime).Seconds())
		r.customMetrics.ISBServicesHealth.DeleteLabelValues(isbServiceRollout.Namespace, isbServiceRollout.Name)
		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(isbServiceRollout, finalizerName) {
		controllerutil.AddFinalizer(isbServiceRollout, finalizerName)
	}

	_, pauseRequestExists := GetPauseModule().getPauseRequest(isbsvcKey)
	if !pauseRequestExists {
		// this is just creating an entry in the map if it doesn't already exist
		GetPauseModule().newPauseRequest(isbsvcKey)
	}

	newISBServiceDef := &kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "InterStepBufferService",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            isbServiceRollout.Name,
			Namespace:       isbServiceRollout.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isbServiceRollout.GetObjectMeta(), apiv1.ISBServiceRolloutGroupVersionKind)},
		},
		Spec: isbServiceRollout.Spec.InterStepBufferService.Spec,
	}

	existingISBServiceDef, err := kubernetes.GetCR(ctx, r.restConfig, newISBServiceDef, "interstepbufferservices")
	if err != nil {
		// create an object as it doesn't exist
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("ISBService %s/%s doesn't exist so creating", isbServiceRollout.Namespace, isbServiceRollout.Name)
			isbServiceRollout.Status.MarkPending()

			if err := kubernetes.CreateCR(ctx, r.restConfig, newISBServiceDef, "interstepbufferservices"); err != nil {
				return ctrl.Result{}, err
			}

			isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "create").Observe(time.Since(startTime).Seconds())
		} else {
			return ctrl.Result{}, fmt.Errorf("error getting ISBService: %v", err)
		}

	} else {
		// Object already exists
		// perform logic related to updating

		newISBServiceDef = mergeISBService(existingISBServiceDef, newISBServiceDef)
		needsRequeue, err := r.processExistingISBService(ctx, isbServiceRollout, existingISBServiceDef, newISBServiceDef, syncStartTime)
		if err != nil {
			return ctrl.Result{}, err
		}
		if needsRequeue {
			return common.DefaultDelayedRequeue, nil
		}
	}

	if err = r.applyPodDisruptionBudget(ctx, isbServiceRollout); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to apply PodDisruptionBudget for ISBServiceRollout %s, err: %v", isbServiceRollout.Name, err)
	}

	return ctrl.Result{}, nil
}

// for the purpose of logging
func (r *ISBServiceRolloutReconciler) getChildTypeString() string {
	return "interstepbufferservice"
}

// take the existing ISBService and merge anything needed from the new ISBService definition
func mergeISBService(existingISBService *kubernetes.GenericObject, newISBService *kubernetes.GenericObject) *kubernetes.GenericObject {
	resultISBService := existingISBService.DeepCopy()
	resultISBService.Spec = *newISBService.Spec.DeepCopy()
	return resultISBService
}

// process an existing ISBService
// return:
// - true if needs a requeue
// - error if any
func (r *ISBServiceRolloutReconciler) processExistingISBService(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout,
	existingISBServiceDef *kubernetes.GenericObject, newISBServiceDef *kubernetes.GenericObject, syncStartTime time.Time) (bool, error) {

	numaLogger := logger.FromContext(ctx)

	// update our Status with the ISBService's Status
	r.processISBServiceStatus(ctx, existingISBServiceDef, isbServiceRollout)

	// if I need to update or am in the middle of an update of the ISBService, then I need to make sure all the Pipelines are pausing
	isbServiceNeedsUpdating, isbServiceIsUpdating, err := r.isISBServiceUpdating(ctx, isbServiceRollout, existingISBServiceDef)
	if err != nil {
		return false, err
	}

	numaLogger.Debugf("isbServiceNeedsUpdating=%t, isbServiceIsUpdating=%t", isbServiceNeedsUpdating, isbServiceIsUpdating)

	// set the Status appropriately to "Pending" or "Deployed"
	// if isbServiceNeedsUpdating - this means there's a mismatch between the desired ISBService spec and actual ISBService spec
	// Note that this will be reset to "Deployed" later on if a deployment occurs
	if isbServiceNeedsUpdating {
		isbServiceRollout.Status.MarkPending()
	} else {
		isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
	}

	// determine the Upgrade Strategy user prefers
	upgradeStrategy, err := usde.GetUserStrategy(ctx, isbServiceRollout.Namespace)
	if err != nil {
		return false, err
	}

	switch upgradeStrategy {
	case config.PPNDStrategyID:

		return processChildObjectWithPPND(ctx, isbServiceRollout, r, isbServiceNeedsUpdating, isbServiceIsUpdating, func() error {
			r.recorder.Eventf(isbServiceRollout, corev1.EventTypeNormal, "PipelinesPaused", "All Pipelines have paused for ISBService update")
			err = r.updateISBService(ctx, isbServiceRollout, newISBServiceDef)
			if err != nil {
				return err
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "update").Observe(time.Since(syncStartTime).Seconds())
			return nil
		})
	case config.NoStrategyID:
		if isbServiceNeedsUpdating {
			// update ISBService
			err = r.updateISBService(ctx, isbServiceRollout, newISBServiceDef)
			if err != nil {
				return false, err
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "update").Observe(time.Since(syncStartTime).Seconds())
		}
	case config.ProgressiveStrategyID:
		return false, errors.New("Progressive Strategy not supported yet")
	default:
		return false, fmt.Errorf("%v strategy not recognized", upgradeStrategy)
	}

	return false, nil
}

func (r *ISBServiceRolloutReconciler) updateISBService(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, newISBServiceDef *kubernetes.GenericObject) error {
	err := kubernetes.UpdateCR(ctx, r.restConfig, newISBServiceDef, "interstepbufferservices")
	if err != nil {
		return err
	}

	isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
	return nil
}

func (r *ISBServiceRolloutReconciler) markRolloutPaused(ctx context.Context, rollout client.Object, paused bool) error {

	isbServiceRollout := rollout.(*apiv1.ISBServiceRollout)

	if paused {
		// if BeginTime hasn't been set yet, we must have just started pausing - set it
		if isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime == metav1.NewTime(initTime) || !isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime.After(isbServiceRollout.Status.PauseRequestStatus.LastPauseEndTime.Time) {
			isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime = metav1.NewTime(time.Now())
		}
		r.updatePauseMetric(isbServiceRollout)
		isbServiceRollout.Status.MarkPausingPipelines(isbServiceRollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		if (isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime != metav1.NewTime(initTime)) && !isbServiceRollout.Status.PauseRequestStatus.LastPauseEndTime.After(isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime.Time) {
			isbServiceRollout.Status.PauseRequestStatus.LastPauseEndTime = metav1.NewTime(time.Now())
			r.updatePauseMetric(isbServiceRollout)
		}
		isbServiceRollout.Status.MarkUnpausingPipelines(isbServiceRollout.Generation)
	}

	return nil
}

func (r *ISBServiceRolloutReconciler) updatePauseMetric(isbServiceRollout *apiv1.ISBServiceRollout) {
	timeElapsed := time.Since(isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime.Time)
	r.customMetrics.ISBServicePausedSeconds.WithLabelValues(isbServiceRollout.Name).Set(timeElapsed.Seconds())
}

func (r *ISBServiceRolloutReconciler) getRolloutKey(rolloutNamespace string, rolloutName string) string {
	return GetPauseModule().getISBServiceKey(rolloutNamespace, rolloutName)
}

// return:
// - whether ISBService needs to update
// - whether it's in the process of being updated
// - error if any
func (r *ISBServiceRolloutReconciler) isISBServiceUpdating(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, existingISBSVCDef *kubernetes.GenericObject) (bool, bool, error) {

	isbServiceReconciled, _, err := r.isISBServiceReconciled(ctx, existingISBSVCDef)
	if err != nil {
		return false, false, err
	}

	existingSpecAsMap := make(map[string]interface{})
	err = json.Unmarshal(existingISBSVCDef.Spec.Raw, &existingSpecAsMap)
	if err != nil {
		return false, false, err
	}
	newSpecAsMap := make(map[string]interface{})
	err = util.StructToStruct(&isbServiceRollout.Spec.InterStepBufferService.Spec, &newSpecAsMap)
	if err != nil {
		return false, false, err
	}

	isbServiceNeedsToUpdate := !reflect.DeepEqual(existingSpecAsMap, newSpecAsMap)

	return isbServiceNeedsToUpdate, !isbServiceReconciled, nil
}

func (r *ISBServiceRolloutReconciler) getPipelineList(ctx context.Context, rolloutNamespace string, rolloutName string) ([]*kubernetes.GenericObject, error) {
	// here
	return kubernetes.ListCR(ctx, r.restConfig, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", rolloutNamespace, fmt.Sprintf("%s=%s,%s", common.LabelKeyISBServiceNameForPipeline, rolloutName, common.LabelKeyPipelineRolloutForPipeline), "")
}

// Apply pod disruption budget for the ISBService
func (r *ISBServiceRolloutReconciler) applyPodDisruptionBudget(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) error {
	pdb := kubernetes.NewPodDisruptionBudget(isbServiceRollout.Name, isbServiceRollout.Namespace, 1,
		[]metav1.OwnerReference{*metav1.NewControllerRef(isbServiceRollout.GetObjectMeta(), apiv1.ISBServiceRolloutGroupVersionKind)},
	)

	// Create the pdb only if it doesn't exist
	existingPDB := &policyv1.PodDisruptionBudget{}
	if err := r.client.Get(ctx, client.ObjectKey{Name: pdb.Name, Namespace: pdb.Namespace}, existingPDB); err != nil {
		if apierrors.IsNotFound(err) {
			if err = r.client.Create(ctx, pdb); err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		// Update the pdb if needed
		if existingPDB.Spec.MaxUnavailable != pdb.Spec.MaxUnavailable {
			existingPDB.Spec.MaxUnavailable = pdb.Spec.MaxUnavailable
			if err := r.client.Update(ctx, existingPDB); err != nil {
				return err
			}
		}
	}

	return nil
}

// Each ISBService has one underlying StatefulSet
func (r *ISBServiceRolloutReconciler) getStatefulSet(ctx context.Context, isbsvc *kubernetes.GenericObject) (*appsv1.StatefulSet, error) {
	statefulSetSelector := labels.NewSelector()
	requirement, err := labels.NewRequirement(numaflowv1.KeyISBSvcName, selection.Equals, []string{isbsvc.Name})
	if err != nil {
		return nil, err
	}
	statefulSetSelector = statefulSetSelector.Add(*requirement)

	var statefulSetList appsv1.StatefulSetList
	err = r.client.List(ctx, &statefulSetList, &client.ListOptions{Namespace: isbsvc.Namespace, LabelSelector: statefulSetSelector}) //TODO: add Watch to StatefulSet (unless we decide to use isbsvc to get all the info directly)
	if err != nil {
		return nil, err
	}
	if len(statefulSetList.Items) > 1 {
		return nil, fmt.Errorf("unexpected: isbsvc %s/%s has multiple StatefulSets: %+v", isbsvc.Namespace, isbsvc.Name, statefulSetList.Items)
	} else if len(statefulSetList.Items) == 0 {
		return nil, nil
	} else {
		return &(statefulSetList.Items[0]), nil
	}
}

// determine if the ISBService, including its underlying StatefulSet, has been reconciled
// so, this requires:
// 1. ISBService.Status.ObservedGeneration == ISBService.Generation
// 2. StatefulSet.Status.ObservedGeneration == StatefulSet.Generation
// 3. StatefulSet.Status.UpdatedReplicas == StatefulSet.Spec.Replicas
func (r *ISBServiceRolloutReconciler) isISBServiceReconciled(ctx context.Context, isbsvc *kubernetes.GenericObject) (bool, string, error) {
	numaLogger := logger.FromContext(ctx)
	isbsvcStatus, err := kubernetes.ParseStatus(isbsvc)
	if err != nil {
		return false, "", fmt.Errorf("failed to parse Status from InterstepBufferService CR: %+v, %v", isbsvc, err)
	}
	numaLogger.Debugf("isbsvc status: %+v", isbsvcStatus)

	statefulSet, err := r.getStatefulSet(ctx, isbsvc)
	if err != nil {
		return false, "", err
	}

	isbsvcReconciled := isbsvc.Generation <= isbsvcStatus.ObservedGeneration

	if !isbsvcReconciled {
		return false, "Mismatch between ISBService Generation and ObservedGeneration", nil
	}
	if statefulSet == nil {
		return false, "StatefulSet not found, may not have been created", nil
	}
	if statefulSet.Generation != statefulSet.Status.ObservedGeneration {
		return false, "Mismatch between StatefulSet Generation and ObservedGeneration", nil
	}
	specifiedReplicas := int32(1)
	if statefulSet.Spec.Replicas != nil {
		specifiedReplicas = *statefulSet.Spec.Replicas
	}
	if specifiedReplicas != statefulSet.Status.UpdatedReplicas { // TODO: keep this, or is this a better test?: UpdatedRevision == CurrentRevision
		return false, fmt.Sprintf("StatefulSet UpdatedReplicas (%d) != specified replicas (%d)", statefulSet.Status.UpdatedReplicas, specifiedReplicas), nil
	}
	return true, "", nil
}

func (r *ISBServiceRolloutReconciler) processISBServiceStatus(ctx context.Context, isbsvc *kubernetes.GenericObject, rollout *apiv1.ISBServiceRollout) {
	numaLogger := logger.FromContext(ctx)
	isbsvcStatus, err := kubernetes.ParseStatus(isbsvc)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Status from InterstepBuffer CR: %+v, %v", isbsvc, err)
		return
	}

	numaLogger.Debugf("isbsvc status: %+v", isbsvcStatus)

	isbSvcPhase := numaflowv1.ISBSvcPhase(isbsvcStatus.Phase)
	isbsvcChildResourceStatus, isbsvcChildResourceReason := getISBServiceChildResourceHealth(isbsvcStatus.Conditions)

	if isbsvcChildResourceReason == "Progressing" {
		rollout.Status.MarkChildResourcesUnhealthy("Progressing", "ISBService Progressing", rollout.Generation)
	} else if isbSvcPhase == numaflowv1.ISBSvcPhaseFailed || isbsvcChildResourceStatus == "False" {
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcFailed", "ISBService Failed", rollout.Generation)
	} else if isbSvcPhase == numaflowv1.ISBSvcPhasePending || isbsvcChildResourceStatus == "Unknown" {
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcPending", "ISBService Pending", rollout.Generation)
	} else if isbSvcPhase == numaflowv1.ISBSvcPhaseUnknown {
		rollout.Status.MarkChildResourcesHealthUnknown("ISBSvcUnknown", "ISBService Phase Unknown", rollout.Generation)
	} else {
		reconciled, nonreconciledMsg, err := r.isISBServiceReconciled(ctx, isbsvc)
		if err != nil {
			numaLogger.Errorf(err, "failed while determining if ISBService is fully reconciled: %+v, %v", isbsvc, err)
			return
		}
		if reconciled && isbsvcChildResourceStatus == "True" {
			rollout.Status.MarkChildResourcesHealthy(rollout.Generation)
		} else {
			rollout.Status.MarkChildResourcesUnhealthy("Progressing", nonreconciledMsg, rollout.Generation)
		}
	}

}

func (r *ISBServiceRolloutReconciler) needsUpdate(old, new *apiv1.ISBServiceRollout) bool {
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

func (r *ISBServiceRolloutReconciler) updateISBServiceRolloutStatus(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) error {
	return r.client.Status().Update(ctx, isbServiceRollout)
}

func (r *ISBServiceRolloutReconciler) updateISBServiceRolloutStatusToFailed(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, err error) error {
	isbServiceRollout.Status.MarkFailed(err.Error())
	return r.updateISBServiceRolloutStatus(ctx, isbServiceRollout)
}

func (r *ISBServiceRolloutReconciler) ErrorHandler(isbServiceRollout *apiv1.ISBServiceRollout, err error, reason, msg string) {
	r.customMetrics.ISBServicesSyncFailed.WithLabelValues().Inc()
	r.recorder.Eventf(isbServiceRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

func getISBServiceChildResourceHealth(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, cond := range conditions {
		if cond.Type == "ChildrenResourcesHealthy" && cond.Status != "True" {
			return cond.Status, cond.Reason
		}
	}
	return "True", ""
}
