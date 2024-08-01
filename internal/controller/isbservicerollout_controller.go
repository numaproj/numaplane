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

	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
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
	ControllerISBSVCRollout = "isbsvc-rollout-controller"
)

// ISBServiceRolloutReconciler reconciles an ISBServiceRollout object
type ISBServiceRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	restConfig    *rest.Config
	customMetrics *metrics.CustomMetrics
}

func NewISBServiceRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
	customMetrics *metrics.CustomMetrics,
) *ISBServiceRolloutReconciler {

	return &ISBServiceRolloutReconciler{
		client,
		s,
		restConfig,
		customMetrics,
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
	numaLogger := logger.GetBaseLogger().WithName("isbservicerollout-reconciler").WithValues("isbservicerollout", req.NamespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

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

	isbServiceRollout.Status.Init(isbServiceRollout.Generation)

	result, err := r.reconcile(ctx, isbServiceRollout)
	if err != nil {
		numaLogger.Errorf(err, "ISBServiceRollout %v reconcile returned error: %v", req.NamespacedName, err)
		statusUpdateErr := r.updateISBServiceRolloutStatusToFailed(ctx, isbServiceRollout, err)
		if statusUpdateErr != nil {
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(isbServiceRolloutOrig, isbServiceRollout) {
		isbServiceRolloutStatus := isbServiceRollout.Status
		if err := r.client.Update(ctx, isbServiceRollout); err != nil {
			numaLogger.Error(err, "Error Updating ISBServiceRollout", "ISBServiceRollout", isbServiceRollout)

			statusUpdateErr := r.updateISBServiceRolloutStatusToFailed(ctx, isbServiceRollout, err)
			if statusUpdateErr != nil {
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
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate metrics for ISB Service.
	r.customMetrics.IncISBServiceMetrics(isbServiceRollout.Name, isbServiceRollout.Namespace)

	numaLogger.Debug("reconciliation successful")

	return result, nil
}

// reconcile does the real logic
func (r *ISBServiceRolloutReconciler) reconcile(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) (ctrl.Result, error) {
	numaLogger := logger.FromContext(ctx)

	// is isbServiceRollout being deleted? need to remove the finalizer so it can
	// (OwnerReference will delete the underlying ISBService through Cascading deletion)
	if !isbServiceRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting ISBServiceRollout")
		if controllerutil.ContainsFinalizer(isbServiceRollout, finalizerName) {
			GetPauseModule().deleteISBServicePauseRequest(isbServiceRollout.Namespace, isbServiceRollout.Name)
			controllerutil.RemoveFinalizer(isbServiceRollout, finalizerName)
		}
		// generate metrics for ISB Service deletion.
		r.customMetrics.DecISBServiceMetrics(isbServiceRollout.Name, isbServiceRollout.Namespace)
		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(isbServiceRollout, finalizerName) {
		controllerutil.AddFinalizer(isbServiceRollout, finalizerName)
	}

	_, pauseRequestExists := GetPauseModule().getISBSvcPauseRequest(isbServiceRollout.Namespace, isbServiceRollout.Name)
	if !pauseRequestExists {
		// this is just creating an entry in the map if it doesn't already exist
		GetPauseModule().newISBServicePauseRequest(isbServiceRollout.Namespace, isbServiceRollout.Name)
	}

	newISBSVCDef := kubernetes.GenericObject{
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

	existingISBServiceDef, err := kubernetes.GetCR(ctx, r.restConfig, &newISBSVCDef, "interstepbufferservices")
	if err != nil {
		// create object as it doesn't exist
		if apierrors.IsNotFound(err) {
			numaLogger.Debugf("ISBService %s/%s doesn't exist so creating", isbServiceRollout.Namespace, isbServiceRollout.Name)
			isbServiceRollout.Status.MarkPending()

			err = kubernetes.CreateCR(ctx, r.restConfig, &newISBSVCDef, "interstepbufferservices")
			if err != nil {
				return ctrl.Result{}, err
			}

			isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)

		} else {
			return ctrl.Result{}, fmt.Errorf("error getting ISBService: %v", err)
		}

	} else {
		// Object already exists
		// perform logic related to updating

		newISBSVCDef = *mergeISBService(existingISBServiceDef, &newISBSVCDef)

		// update our Status with the ISBService's Status
		r.processISBServiceStatus(ctx, existingISBServiceDef, isbServiceRollout)

		// if I need to update or am in the middle of an update of the ISBService, then I need to make sure all the Pipelines are pausing
		isbServiceNeedsUpdating, isbServiceIsUpdating, err := r.isISBServiceUpdating(ctx, isbServiceRollout, existingISBServiceDef)
		if err != nil {
			return ctrl.Result{}, err
		}

		numaLogger.Debugf("isbServiceNeedsUpdating=%t, isbServiceIsUpdating=%t", isbServiceNeedsUpdating, isbServiceIsUpdating)

		// set the Status appropriately to "Pending" or "Deployed"
		// if isbServiceNeedsUpdating - this means there's a mismatch between the desired ISBService spec and actual ISBService spec
		// if there's a generation mismatch - this means we haven't even observed the current generation
		// we may match the first case and not the second when we've observed the generation change but we're pausing pipelines
		// we may match the second case and not the first if we need to update something other than ISBService spec
		if isbServiceNeedsUpdating || isbServiceRollout.Status.ObservedGeneration < isbServiceRollout.Generation {
			isbServiceRollout.Status.MarkPending()
		} else {
			isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
		}

		if common.DataLossPrevention {

			if isbServiceNeedsUpdating || isbServiceIsUpdating {
				numaLogger.Info("ISBService either needs to or is in the process of updating")
				// TODO: maybe only pause if the update requires pausing

				// request pause if we haven't already
				pauseRequestUpdated, err := r.requestPipelinesPause(ctx, isbServiceRollout, existingISBServiceDef, true)
				if err != nil {
					return ctrl.Result{}, err
				}
				if !pauseRequestUpdated { // don't do this yet if we just made a request - it's too soon for anything to have happened
					// check if the pipelines are all paused and we're trying to update the spec
					if isbServiceNeedsUpdating {
						allPaused, err := r.allPipelinesPaused(ctx, existingISBServiceDef)
						if err != nil {
							return ctrl.Result{}, err
						}
						if allPaused {
							numaLogger.Infof("confirmed all Pipelines have paused so ISBService can safely update")
							// update ISBService
							err = kubernetes.UpdateCR(ctx, r.restConfig, &newISBSVCDef, "interstepbufferservices")
							if err != nil {
								return ctrl.Result{}, err
							}

							isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
						} else {
							numaLogger.Debugf("not all Pipelines have paused")
						}
					}

				}
				return delayedRequeue, nil

			} else {
				// remove any pause requirement if necessary
				_, err := r.requestPipelinesPause(ctx, isbServiceRollout, existingISBServiceDef, false)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
		} else {
			// update ISBService
			err = kubernetes.UpdateCR(ctx, r.restConfig, &newISBSVCDef, "interstepbufferservices")
			if err != nil {
				return ctrl.Result{}, err
			}

			isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
		}
	}

	if err = r.applyPodDisruptionBudget(ctx, isbServiceRollout); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to apply PodDisruptionBudget for ISBServiceRollout %s, err: %v", isbServiceRollout.Name, err)
	}

	return ctrl.Result{}, nil
}

// take the existing ISBService and merge anything needed from the new ISBService definition
func mergeISBService(existingISBService *kubernetes.GenericObject, newISBService *kubernetes.GenericObject) *kubernetes.GenericObject {
	resultISBService := existingISBService.DeepCopy()
	resultISBService.Spec = *newISBService.Spec.DeepCopy()
	return resultISBService
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
	// TODO: see if we can just use DeepEqual or if there may be null values we need to ignore
	isbServiceNeedsToUpdate := !util.CompareMapsIgnoringNulls(existingSpecAsMap, newSpecAsMap)

	return isbServiceNeedsToUpdate, !isbServiceReconciled, nil
}

// request that Pipelines pause
// return whether an update was made
func (r *ISBServiceRolloutReconciler) requestPipelinesPause(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, isbService *kubernetes.GenericObject, pause bool) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	updated := GetPauseModule().updateISBServicePauseRequest(isbService.Namespace, isbService.Name, pause)
	if updated { // if the value is different from what it was then make sure we queue the pipelines to be processed
		numaLogger.Infof("updated pause request = %t", pause)
		pipelines, err := r.getPipelines(ctx, isbService)
		if err != nil {
			return false, err
		}
		for _, pipeline := range pipelines {
			pipelineROReconciler.enqueuePipeline(k8stypes.NamespacedName{Namespace: pipeline.Namespace, Name: pipeline.Name})
		}
	}

	if pause {
		isbServiceRollout.Status.MarkPausingPipelines(isbServiceRollout.Generation)
	} else {
		isbServiceRollout.Status.MarkUnpausingPipelines(isbServiceRollout.Generation)
	}
	return updated, nil
}

func (r *ISBServiceRolloutReconciler) getPipelines(ctx context.Context, isbService *kubernetes.GenericObject) ([]*kubernetes.GenericObject, error) {
	return kubernetes.ListCR(ctx, r.restConfig, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", isbService.Namespace, fmt.Sprintf("%s=%s", common.LabelKeyISBServiceNameForPipeline, isbService.Name), "")
}

func (r *ISBServiceRolloutReconciler) allPipelinesPaused(ctx context.Context, isbService *kubernetes.GenericObject) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelines, err := r.getPipelines(ctx, isbService)
	if err != nil {
		return false, err
	}
	for _, pipeline := range pipelines {
		status, err := kubernetes.ParseStatus(pipeline)
		if err != nil {
			return false, err
		}
		if status.Phase != "Paused" {
			numaLogger.Debugf("pipeline %q has status.phase=%q", pipeline.Name, status.Phase)
			return false, nil
		}
	}
	return true, nil
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
	switch isbSvcPhase {
	case numaflowv1.ISBSvcPhaseFailed:
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcFailed", "ISBService Failed", rollout.Generation)
	case numaflowv1.ISBSvcPhasePending:
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcPending", "ISBService Pending", rollout.Generation)
	case numaflowv1.ISBSvcPhaseUnknown:
		rollout.Status.MarkChildResourcesHealthUnknown("ISBSvcUnknown", "ISBService Phase Unknown", rollout.Generation)
	default:

		reconciled, nonreconciledMsg, err := r.isISBServiceReconciled(ctx, isbsvc)
		if err != nil {
			numaLogger.Errorf(err, "failed while determining if ISBService is fully reconciled: %+v, %v", isbsvc, err)
			return
		}

		if reconciled {
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
	rawSpec := runtime.RawExtension{}
	err := util.StructToStruct(&isbServiceRollout.Spec, &rawSpec)
	if err != nil {
		return fmt.Errorf("unable to convert ISBServiceRollout Spec to GenericObject Spec: %v", err)
	}

	rawStatus := runtime.RawExtension{}
	err = util.StructToStruct(&isbServiceRollout.Status, &rawStatus)
	if err != nil {
		return fmt.Errorf("unable to convert ISBServiceRollout Status to GenericObject Status: %v", err)
	}

	obj := kubernetes.GenericObject{
		TypeMeta:   isbServiceRollout.TypeMeta,
		ObjectMeta: isbServiceRollout.ObjectMeta,
		Spec:       rawSpec,
		Status:     rawStatus,
	}

	return kubernetes.UpdateStatus(ctx, r.restConfig, &obj, "isbservicerollouts")
}

func (r *ISBServiceRolloutReconciler) updateISBServiceRolloutStatusToFailed(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, err error) error {
	numaLogger := logger.FromContext(ctx)

	isbServiceRollout.Status.MarkFailed(err.Error())

	statusUpdateErr := r.updateISBServiceRolloutStatus(ctx, isbServiceRollout)
	if statusUpdateErr != nil {
		numaLogger.Error(statusUpdateErr, "Error updating ISBServiceRollout status", "namespace", isbServiceRollout.Namespace, "name", isbServiceRollout.Name)
	}

	return statusUpdateErr
}
