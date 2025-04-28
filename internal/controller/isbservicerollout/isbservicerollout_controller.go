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

package isbservicerollout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
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
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/pipelinerollout"
	"github.com/numaproj/numaplane/internal/controller/ppnd"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerISBSVCRollout     = "isbsvc-rollout-controller"
	TemplateISBServiceName      = ".isbsvc-name"
	TemplateISBServiceNamespace = ".isbsvc-namespace"
)

// ISBServiceRolloutReconciler reconciles an ISBServiceRollout object
type ISBServiceRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder

	// maintain inProgressStrategies in memory and in ISBServiceRollout Status
	inProgressStrategyMgr *ctlrcommon.InProgressStrategyMgr
}

func NewISBServiceRolloutReconciler(
	c client.Client,
	s *runtime.Scheme,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *ISBServiceRolloutReconciler {

	r := &ISBServiceRolloutReconciler{
		c,
		s,
		customMetrics,
		recorder,
		nil,
	}

	r.inProgressStrategyMgr = ctlrcommon.NewInProgressStrategyMgr(
		// getRolloutStrategy function:
		func(ctx context.Context, rollout client.Object) *apiv1.UpgradeStrategy {
			isbServiceRollout := rollout.(*apiv1.ISBServiceRollout)

			if isbServiceRollout.Status.UpgradeInProgress != "" {
				return (*apiv1.UpgradeStrategy)(&isbServiceRollout.Status.UpgradeInProgress)
			} else {
				return nil
			}
		},
		// setRolloutStrategy function:
		func(ctx context.Context, rollout client.Object, strategy apiv1.UpgradeStrategy) {
			isbServiceRollout := rollout.(*apiv1.ISBServiceRollout)
			isbServiceRollout.Status.SetUpgradeInProgress(strategy)
		},
	)

	return r
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
	r.customMetrics.ISBServiceROSyncs.WithLabelValues().Inc()

	// Get the live ISBServiceRollout since we need latest Status for Progressive rollout case
	// TODO: consider storing ISBServiceRollout Status in a local cache instead of this
	isbServiceRollout, err := getLiveISBServiceRollout(ctx, req.NamespacedName.Name, req.NamespacedName.Namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Info("ISBServiceRollout not found, %v", err)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("error getting the live ISB Service rollout: %w", err)
	}

	// save off a copy of the original before we modify it
	isbServiceRolloutOrig := isbServiceRollout
	isbServiceRollout = isbServiceRolloutOrig.DeepCopy()

	isbServiceRollout.Status.Init(isbServiceRollout.Generation)

	result, err := r.reconcile(ctx, isbServiceRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(ctx, isbServiceRollout, err, "ReconcileFailed", "Failed to reconcile isb service rollout")
		statusUpdateErr := r.updateISBServiceRolloutStatusToFailed(ctx, isbServiceRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(ctx, isbServiceRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update isb service rollout status")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	// Update the resource definition (everything except the Status subresource)
	if r.needsUpdate(isbServiceRolloutOrig, isbServiceRollout) {
		if err := r.client.Patch(ctx, isbServiceRollout, client.MergeFrom(isbServiceRolloutOrig)); err != nil {
			r.ErrorHandler(ctx, isbServiceRollout, err, "UpdateFailed", "Failed to patch isb service rollout")
			if statusUpdateErr := r.updateISBServiceRolloutStatusToFailed(ctx, isbServiceRollout, err); statusUpdateErr != nil {
				r.ErrorHandler(ctx, isbServiceRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update isb service rollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
	}

	// Update the Status subresource
	if isbServiceRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updateISBServiceRolloutStatus(ctx, isbServiceRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(ctx, isbServiceRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update isb service rollout status")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate metrics for ISB Service.
	r.customMetrics.IncISBServiceRollouts(isbServiceRollout.Name, isbServiceRollout.Namespace)
	r.recorder.Eventf(isbServiceRollout, corev1.EventTypeNormal, "ReconcilationSuccessful", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

	return result, nil
}

// reconcile does the real logic
func (r *ISBServiceRolloutReconciler) reconcile(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, syncStartTime time.Time) (ctrl.Result, error) {
	startTime := time.Now()
	numaLogger := logger.FromContext(ctx)

	requeueDelay := time.Duration(0)

	defer func() {
		r.customMetrics.SetISBServicesRolloutHealth(isbServiceRollout.Namespace, isbServiceRollout.Name, string(isbServiceRollout.Status.Phase))
	}()

	isbsvcKey := ppnd.GetPauseModule().GetISBServiceKey(isbServiceRollout.Namespace, isbServiceRollout.Name)

	// is isbServiceRollout being deleted? need to remove the finalizer so it can
	// (OwnerReference will delete the underlying ISBService through Cascading deletion)
	if !isbServiceRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting ISBServiceRollout")
		if controllerutil.ContainsFinalizer(isbServiceRollout, common.FinalizerName) {
			ppnd.GetPauseModule().DeletePauseRequest(isbsvcKey)
			// delete the ISBServiceRollout child objects once the ISBServiceRollout is being deleted
			requeue, err := r.listAndDeleteChildISBServices(ctx, isbServiceRollout)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("error deleting ISBServiceRollout child: %v", err)
			}
			// if we have any ISBServices that are still in the process of being deleted, requeue
			if requeue {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			controllerutil.RemoveFinalizer(isbServiceRollout, common.FinalizerName)
		}
		// generate metrics for ISB Service deletion.
		r.customMetrics.DecISBServiceRollouts(isbServiceRollout.Name, isbServiceRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "delete").Observe(time.Since(startTime).Seconds())
		r.customMetrics.DeleteISBServicesRolloutHealth(isbServiceRollout.Namespace, isbServiceRollout.Name)
		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(isbServiceRollout, common.FinalizerName) {
		controllerutil.AddFinalizer(isbServiceRollout, common.FinalizerName)
	}

	_, pauseRequestExists := ppnd.GetPauseModule().GetPauseRequest(isbsvcKey)
	if !pauseRequestExists {
		// this is just creating an entry in the map if it doesn't already exist
		ppnd.GetPauseModule().NewPauseRequest(isbsvcKey)
	}

	// check if there's a promoted isbsvc yet
	promotedISBSvcs, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, isbServiceRollout, common.LabelValueUpgradePromoted, nil, false, r.client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error looking for promoted ISBService: %v", err)
	}

	if promotedISBSvcs == nil || len(promotedISBSvcs.Items) == 0 {

		deleteRecreateLabel := common.LabelValueDeleteRecreateChild

		// first check if there's a "recyclable" isbsvc; if there is, it could be in the middle of a delete/recreate process, and we don't want to create a new one until it's been deleted
		recyclableISBSvcs, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, isbServiceRollout, common.LabelValueUpgradeRecyclable, &deleteRecreateLabel, false, r.client)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error looking for recyclable ISBServices: %v", err)
		}
		if recyclableISBSvcs != nil && len(recyclableISBSvcs.Items) > 0 {
			numaLogger.WithValues("recyclable isbservices", recyclableISBSvcs).Debug("can't create 'promoted' isbservice yet; need to wait for recyclable isbservices to be deleted")
			requeueDelay = common.DefaultRequeueDelay
		} else {

			// create an object as it doesn't exist
			numaLogger.Debugf("ISBService %s/%s doesn't exist so creating", isbServiceRollout.Namespace, isbServiceRollout.Name)
			isbServiceRollout.Status.MarkPending()

			newISBServiceDef, err := r.makeTargetISBServiceDef(ctx, isbServiceRollout)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("error generating ISBService: %v", err)
			}
			if newISBServiceDef != nil {
				if err = kubernetes.CreateResource(ctx, r.client, newISBServiceDef); err != nil {
					return ctrl.Result{}, fmt.Errorf("error creating ISBService: %v", err)
				}
				if err = r.applyPodDisruptionBudget(ctx, newISBServiceDef); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to apply PodDisruptionBudget for ISBService %s, err: %v", newISBServiceDef.GetName(), err)
				}
				if err := ctlrcommon.CreateRidersForNewChild(ctx, r, isbServiceRollout, newISBServiceDef, r.client); err != nil {
					return ctrl.Result{}, fmt.Errorf("error creating riders: %s", err)
				}

				isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
				r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "create").Observe(time.Since(startTime).Seconds())

				// if there are any PipelineRollouts using this ISBServiceRollout, enqueue them for reconciliation
				err := r.enqueueAllPipelineROsForISBServiceRO(ctx, isbServiceRollout)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("error enqueuing PipelineRollouts following creation of InterstepBufferService %s/%s: %v", newISBServiceDef.GetNamespace(), newISBServiceDef.GetName(), err)
				}
			}
		}

	} else {
		// Object already exists
		// perform logic related to updating
		newISBServiceDef, err := r.makeTargetISBServiceDef(ctx, isbServiceRollout)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error generating ISBService: %v", err)
		}
		existingISBServiceDef, err := kubernetes.GetResource(ctx, r.client, newISBServiceDef.GroupVersionKind(),
			k8stypes.NamespacedName{Namespace: newISBServiceDef.GetNamespace(), Name: newISBServiceDef.GetName()})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error getting ISBService: %v", err)
		}
		if err = r.applyPodDisruptionBudget(ctx, existingISBServiceDef); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to apply PodDisruptionBudget for ISBService %s, UID=%s, err: %v", existingISBServiceDef.GetName(), existingISBServiceDef.GetUID(), err)
		}

		newISBServiceDef = r.merge(existingISBServiceDef, newISBServiceDef)
		requeueDelay, err = r.processExistingISBService(ctx, isbServiceRollout, existingISBServiceDef, newISBServiceDef, syncStartTime)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error processing existing ISBService: %v", err)
		}
	}

	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, isbServiceRollout)

	// clean up recyclable interstepbufferservices
	allDeleted, err := ctlrcommon.GarbageCollectChildren(ctx, isbServiceRollout, r, r.client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error deleting recyclable interstepbufferservices: %s", err.Error())
	}

	// if we still have interstepbufferservices that need deleting, or if we're in the middle of an upgrade strategy, then requeue
	if !allDeleted || inProgressStrategy != apiv1.UpgradeStrategyNoOp {
		if requeueDelay == 0 {
			requeueDelay = common.DefaultRequeueDelay
		} else {
			requeueDelay = min(requeueDelay, common.DefaultRequeueDelay)
		}
	}

	if requeueDelay > 0 {
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}
	return ctrl.Result{}, nil
}

// for the purpose of logging
func (r *ISBServiceRolloutReconciler) GetChildTypeString() string {
	return "interstepbufferservice"
}

// process an existing ISBService
// return:
// - a requeue delay greater than 0 if requeue is needed
// - error if any
func (r *ISBServiceRolloutReconciler) processExistingISBService(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout,
	existingISBServiceDef, newISBServiceDef *unstructured.Unstructured, syncStartTime time.Time) (time.Duration, error) {

	numaLogger := logger.FromContext(ctx)

	// get the list of Riders that we need based on the ISBServiceRollout definition
	currentRiderList, err := r.GetDesiredRiders(isbServiceRollout, existingISBServiceDef.GetName(), newISBServiceDef)
	if err != nil {
		return 0, fmt.Errorf("error getting desired Riders for isbsvc %s: %s", existingISBServiceDef.GetName(), err)
	}
	// get the list of Riders that we have now (for promoted child)
	existingRiderList, err := r.GetExistingRiders(ctx, isbServiceRollout, false)
	if err != nil {
		return 0, fmt.Errorf("error getting existing Riders for isbsvc %s: %s", existingISBServiceDef.GetName(), err)
	}

	// update our Status with the ISBService's Status
	r.processISBServiceStatus(ctx, existingISBServiceDef, isbServiceRollout)

	// determine if we're trying to update the ISBService spec
	// if it's a simple change, direct apply
	// if not, it will require PPND or Progressive
	needsUpdate, upgradeStrategyType, needsRecreate, riderAdditions, riderModifications, riderDeletions, err := usde.ResourceNeedsUpdating(ctx, newISBServiceDef, existingISBServiceDef, currentRiderList, existingRiderList)
	if err != nil {
		return 0, err
	}
	numaLogger.
		WithValues("needsUpdate", needsUpdate, "upgradeStrategyType", upgradeStrategyType).
		Debug("Upgrade decision result")

	// set the Status appropriately to "Pending" or "Deployed"
	// if needsUpdate - this means there's a mismatch between the desired ISBService spec and actual ISBService spec
	// Note that this will be reset to "Deployed" later on if a deployment occurs
	if needsUpdate {
		isbServiceRollout.Status.MarkPending()
	} else {
		isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
	}

	// is there currently an inProgressStrategy for the isbService? (This will override any new decision)
	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, isbServiceRollout)
	inProgressStrategySet := (inProgressStrategy != apiv1.UpgradeStrategyNoOp)

	// if not, should we set one?
	if !inProgressStrategySet {
		if upgradeStrategyType == apiv1.UpgradeStrategyPPND {
			inProgressStrategy = apiv1.UpgradeStrategyPPND
			r.inProgressStrategyMgr.SetStrategy(ctx, isbServiceRollout, inProgressStrategy)
		}
		if upgradeStrategyType == apiv1.UpgradeStrategyProgressive {
			inProgressStrategy = apiv1.UpgradeStrategyProgressive
			r.inProgressStrategyMgr.SetStrategy(ctx, isbServiceRollout, inProgressStrategy)
		}
		if upgradeStrategyType == apiv1.UpgradeStrategyApply {
			inProgressStrategy = apiv1.UpgradeStrategyApply
		}
	}

	// don't risk out-of-date cache while performing PPND or Progressive strategy - get
	// the most current version of the isbsvc just in case
	if inProgressStrategy != apiv1.UpgradeStrategyNoOp {
		existingISBServiceDef, err = kubernetes.GetLiveResource(ctx, newISBServiceDef, "interstepbufferservices")
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.WithValues("isbsvcDefinition", *newISBServiceDef).Warn("InterstepBufferService not found.")
			} else {
				return 0, fmt.Errorf("error getting InterstepBufferService for status processing: %v", err)
			}
		}
		newISBServiceDef = r.merge(existingISBServiceDef, newISBServiceDef)
	}

	// Roll out any changes to Riders in K8S
	// Note for Progressive we have special logic, since the update needs to be for the upgrading child
	if inProgressStrategy != apiv1.UpgradeStrategyProgressive {
		if inProgressStrategy != apiv1.UpgradeStrategyNoOp {
			// update the cluster to reflect the Rider additions, modifications, and deletions
			if err := riders.UpdateRidersInK8S(ctx, newISBServiceDef, riderAdditions, riderModifications, riderDeletions, r.client); err != nil {
				return 0, err
			}
		}

		// update the list of riders in the Status
		r.SetCurrentRiderList(isbServiceRollout, currentRiderList)
	}

	switch inProgressStrategy {
	case apiv1.UpgradeStrategyPPND:

		_, isbServiceIsUpdating, err := r.isISBServiceUpdating(ctx, isbServiceRollout, existingISBServiceDef, true)
		if err != nil {
			return 0, fmt.Errorf("error determining if ISBService is updating: %v", err)
		}

		done, err := ppnd.ProcessChildObjectWithPPND(ctx, r.client, isbServiceRollout, r, needsUpdate, isbServiceIsUpdating, func() error {
			r.recorder.Eventf(isbServiceRollout, corev1.EventTypeNormal, "PipelinesPaused", "All Pipelines have paused for ISBService update")
			err = r.updateISBService(ctx, isbServiceRollout, newISBServiceDef, needsRecreate)
			if err != nil {
				return fmt.Errorf("error updating ISBService, %s: %v", apiv1.UpgradeStrategyPPND, err)
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "update").Observe(time.Since(syncStartTime).Seconds())
			return nil
		},
			pipelinerollout.PipelineROReconciler.EnqueuePipeline)
		if err != nil {
			return 0, err
		}

		if done {
			r.inProgressStrategyMgr.UnsetStrategy(ctx, isbServiceRollout)
		} else {
			// requeue if done with PPND is false
			return common.DefaultRequeueDelay, nil
		}
	case apiv1.UpgradeStrategyProgressive:
		numaLogger.Debug("processing InterstepBufferService with Progressive")

		done, progressiveRequeueDelay, err := progressive.ProcessResource(ctx, isbServiceRollout, existingISBServiceDef, needsUpdate, r, r.client)
		if err != nil {
			return 0, fmt.Errorf("error processing isbsvc with progressive: %s", err.Error())
		}
		if done {
			// update the list of riders in the Status based on our child which was just promoted
			currentRiderList, err := r.GetDesiredRiders(isbServiceRollout, existingISBServiceDef.GetName(), newISBServiceDef)
			if err != nil {
				return 0, fmt.Errorf("error getting desired Riders for pipeline %s: %s", newISBServiceDef.GetName(), err)
			}
			r.SetCurrentRiderList(isbServiceRollout, currentRiderList)

			r.inProgressStrategyMgr.UnsetStrategy(ctx, isbServiceRollout)
		} else {

			// we need to make sure the PipelineRollouts using this isbsvc are reconciled
			pipelineRollouts, err := r.getPipelineRolloutList(ctx, isbServiceRollout.Namespace, isbServiceRollout.Name)
			if err != nil {
				return 0, fmt.Errorf("error getting PipelineRollouts; can't enqueue pipelines: %s", err.Error())
			}
			for _, pipelineRollout := range pipelineRollouts {
				numaLogger.WithValues("pipeline rollout", pipelineRollout.Name).Debugf("Created new upgrading isbsvc; now enqueueing pipeline rollout")
				pipelinerollout.PipelineROReconciler.EnqueuePipeline(k8stypes.NamespacedName{Namespace: pipelineRollout.Namespace, Name: pipelineRollout.Name})
			}

			// requeue using the provided delay
			return progressiveRequeueDelay, nil
		}
	case apiv1.UpgradeStrategyApply:
		// update ISBService
		err = r.updateISBService(ctx, isbServiceRollout, newISBServiceDef, needsRecreate)
		if err != nil {
			return 0, fmt.Errorf("error updating ISBService, %s: %v", inProgressStrategy, err)
		}

		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerISBSVCRollout, "update").Observe(time.Since(syncStartTime).Seconds())
	case apiv1.UpgradeStrategyNoOp:
		break
	default:
		return 0, fmt.Errorf("%v strategy not recognized", inProgressStrategy)
	}

	return 0, nil
}

func (r *ISBServiceRolloutReconciler) updateISBService(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, newISBServiceDef *unstructured.Unstructured, needsRecreate bool) error {
	if needsRecreate {
		// in this case, we need to mark our resource as Recyclable (it will be recreated on a future reconciliation after it's been deleted)
		reasonRecreate := common.LabelValueDeleteRecreateChild
		err := ctlrcommon.UpdateUpgradeState(ctx, r.client, common.LabelValueUpgradeRecyclable, &reasonRecreate, newISBServiceDef)
		if err != nil {
			return err
		}
		isbServiceRollout.Status.MarkPending()
		// enqueue Pipeline Rollouts because they will need to be marked "recyclable" as well
		err = r.enqueueAllPipelineROsForChildISBSvc(ctx, newISBServiceDef)
		if err != nil {
			return fmt.Errorf("failed to enqueue pipelines after marking isbservice as recyclable: %s", err.Error())
		}
	} else {
		if err := kubernetes.UpdateResource(ctx, r.client, newISBServiceDef); err != nil {
			return err
		}
		isbServiceRollout.Status.MarkDeployed(isbServiceRollout.Generation)
	}
	return nil
}

func (r *ISBServiceRolloutReconciler) MarkRolloutPaused(ctx context.Context, rollout client.Object, paused bool) error {

	isbServiceRollout := rollout.(*apiv1.ISBServiceRollout)

	uninitialized := metav1.NewTime(time.Time{})

	if paused {
		// if BeginTime hasn't been set yet, we must have just started pausing - set it
		if isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime == uninitialized || !isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime.After(isbServiceRollout.Status.PauseRequestStatus.LastPauseEndTime.Time) {
			isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime = metav1.NewTime(time.Now())
		}
		r.updatePauseMetric(isbServiceRollout)
		isbServiceRollout.Status.MarkPausingPipelines(isbServiceRollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		if (isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime != uninitialized) && !isbServiceRollout.Status.PauseRequestStatus.LastPauseEndTime.After(isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime.Time) {
			isbServiceRollout.Status.PauseRequestStatus.LastPauseEndTime = metav1.NewTime(time.Now())
			r.updatePauseMetric(isbServiceRollout)
		}
		isbServiceRollout.Status.MarkUnpausingPipelines(isbServiceRollout.Generation)
	}

	return nil
}

func (r *ISBServiceRolloutReconciler) updatePauseMetric(isbServiceRollout *apiv1.ISBServiceRollout) {
	timeElapsed := time.Since(isbServiceRollout.Status.PauseRequestStatus.LastPauseBeginTime.Time)
	r.customMetrics.ISBServicePausedSeconds.WithLabelValues(isbServiceRollout.Name, isbServiceRollout.Namespace).Set(timeElapsed.Seconds())
}

func (r *ISBServiceRolloutReconciler) GetRolloutKey(rolloutNamespace string, rolloutName string) string {
	return ppnd.GetPauseModule().GetISBServiceKey(rolloutNamespace, rolloutName)
}

// return:
// - whether ISBService needs to update
// - whether it's in the process of being updated
// - error if any

// Depending on value "checkLive", either check K8S API directly or go to informer cache
func (r *ISBServiceRolloutReconciler) isISBServiceUpdating(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, existingISBSVCDef *unstructured.Unstructured,
	checkLive bool) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	isbServiceReconciled, msg, err := r.isISBServiceReconciled(ctx, existingISBSVCDef, checkLive)
	if err != nil {
		return false, false, err
	}
	numaLogger.Debugf("isbServiceReconciled=%t, msg=%s", isbServiceReconciled, msg)

	existingSpecAsMap, found, err := unstructured.NestedMap(existingISBSVCDef.Object, "spec")
	if err != nil || !found {
		return false, false, err
	}

	newSpecAsMap := make(map[string]interface{})
	err = util.StructToStruct(&isbServiceRollout.Spec.InterStepBufferService.Spec, &newSpecAsMap)
	if err != nil {
		return false, false, err
	}

	needsUpdate := !util.CompareStructNumTypeAgnostic(existingSpecAsMap, newSpecAsMap)

	return needsUpdate, !isbServiceReconciled, nil
}

func (r *ISBServiceRolloutReconciler) getPipelineRolloutList(ctx context.Context, isbRolloutNamespace string, isbsvcRolloutName string) ([]apiv1.PipelineRollout, error) {
	numaLogger := logger.FromContext(ctx)

	pipelineRolloutsForISBSvc := make([]apiv1.PipelineRollout, 0)
	var pipelineRolloutInNamespace apiv1.PipelineRolloutList
	// get all of the PipelineRollouts on the namespace and filter out any that aren't tied to this ISBServiceRollout
	err := r.client.List(ctx, &pipelineRolloutInNamespace, &client.ListOptions{Namespace: isbRolloutNamespace})
	if err != nil {
		return pipelineRolloutsForISBSvc, err
	}
	for _, pipelineRollout := range pipelineRolloutInNamespace.Items {
		// which ISBServiceRollout is this PipelineRollout using?
		var pipelineSpec numaflowtypes.PipelineSpec
		err = json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec)
		if err != nil {
			return pipelineRolloutsForISBSvc, err
		}
		isbsvcROUsed := "default"
		if pipelineSpec.InterStepBufferServiceName != "" {
			isbsvcROUsed = pipelineSpec.InterStepBufferServiceName
		}
		if isbsvcROUsed == isbsvcRolloutName {
			pipelineRolloutsForISBSvc = append(pipelineRolloutsForISBSvc, pipelineRollout)
		}
	}
	numaLogger.Debugf("found %d PipelineRollouts associated with ISBServiceRollout", len(pipelineRolloutsForISBSvc))
	return pipelineRolloutsForISBSvc, nil
}

// enqueue all of the PipelineRollouts which have a child using this InterstepBufferService
func (r *ISBServiceRolloutReconciler) enqueueAllPipelineROsForChildISBSvc(ctx context.Context, isbsvc *unstructured.Unstructured) error {
	pipelines, err := r.getPipelineListForChildISBSvc(ctx, isbsvc.GetNamespace(), isbsvc.GetName())
	if err != nil {
		return fmt.Errorf("failed to enqueue pipelines - failed to list them: %s", err.Error())
	}
	for _, pipeline := range pipelines.Items {
		pipelineRollout, err := ctlrcommon.GetRolloutParentName(pipeline.GetName())
		if err != nil {
			return fmt.Errorf("failed to enqueue pipelines - error getting PipelineRolloutName: %w", err)
		}
		pipelinerollout.PipelineROReconciler.EnqueuePipeline(k8stypes.NamespacedName{Namespace: pipeline.GetNamespace(), Name: pipelineRollout})
	}
	return nil
}

// enqueue all of the PipelineRollouts which are specified to use this ISBServiceRollout
func (r *ISBServiceRolloutReconciler) enqueueAllPipelineROsForISBServiceRO(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) error {
	pipelineRollouts, err := r.getPipelineRolloutList(ctx, isbServiceRollout.GetNamespace(), isbServiceRollout.GetName())
	if err != nil {
		return fmt.Errorf("failed to enqueue PipelineRollouts for ISBServiceRollout %s/%s: %s", isbServiceRollout.GetNamespace(), isbServiceRollout.GetName(), err.Error())
	}
	for _, pipelineRollout := range pipelineRollouts {
		pipelinerollout.PipelineROReconciler.EnqueuePipeline(k8stypes.NamespacedName{Namespace: pipelineRollout.GetNamespace(), Name: pipelineRollout.GetName()})
	}
	return nil
}

func (r *ISBServiceRolloutReconciler) GetPipelineList(ctx context.Context, rolloutNamespace string, rolloutName string) (*unstructured.UnstructuredList, error) {
	gvk := schema.GroupVersionKind{Group: common.NumaflowAPIGroup, Version: common.NumaflowAPIVersion, Kind: common.NumaflowPipelineKind}
	return kubernetes.ListResources(ctx, r.client, gvk, rolloutNamespace,
		client.MatchingLabels{common.LabelKeyISBServiceRONameForPipeline: rolloutName},
		client.HasLabels{common.LabelKeyParentRollout},
	)
}

func (r *ISBServiceRolloutReconciler) getPipelineListForChildISBSvc(ctx context.Context, namespace string, isbsvcName string) (*unstructured.UnstructuredList, error) {
	gvk := schema.GroupVersionKind{Group: common.NumaflowAPIGroup, Version: common.NumaflowAPIVersion, Kind: common.NumaflowPipelineKind}
	return kubernetes.ListResources(ctx, r.client, gvk, namespace,
		client.MatchingLabels{common.LabelKeyISBServiceChildNameForPipeline: isbsvcName},
		client.HasLabels{common.LabelKeyParentRollout},
	)
}

// Apply pod disruption budget for the ISBService
func (r *ISBServiceRolloutReconciler) applyPodDisruptionBudget(ctx context.Context, isbService *unstructured.Unstructured) error {
	pdb := kubernetes.NewPodDisruptionBudget(isbService.GetName(), isbService.GetNamespace(), 1,
		[]metav1.OwnerReference{*metav1.NewControllerRef(&metav1.ObjectMeta{Name: isbService.GetName(), Namespace: isbService.GetNamespace(), UID: isbService.GetUID()},
			numaflowv1.ISBGroupVersionKind)},
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

// determine if the ISBService, including its underlying StatefulSet, has been reconciled
// so, this requires:
// 1. ISBService.Status.ObservedGeneration == ISBService.Generation
// 2. StatefulSet.Status.ObservedGeneration == StatefulSet.Generation
// 3. StatefulSet.Status.UpdatedReplicas == StatefulSet.Spec.Replicas
// Depending on value "checkLive", either check K8S API directly or go to informer cache
func (r *ISBServiceRolloutReconciler) isISBServiceReconciled(ctx context.Context, isbsvc *unstructured.Unstructured, checkLive bool) (bool, string, error) {
	numaLogger := logger.FromContext(ctx)
	isbsvcStatus, err := kubernetes.ParseStatus(isbsvc)
	if err != nil {
		return false, "", fmt.Errorf("failed to parse Status from InterstepBufferService CR: %+v, %v", isbsvc, err)
	}

	statefulSet, err := numaflowtypes.GetISBSvcStatefulSetFromK8s(ctx, r.client, isbsvc, checkLive)
	if err != nil {
		return false, "", err
	}

	isbsvcReconciled := isbsvc.GetGeneration() <= isbsvcStatus.ObservedGeneration
	numaLogger.Debugf("isbsvc status: %+v, isbsvc.Object[metadata]=%+v, generation=%d, observed generation=%d", isbsvcStatus, isbsvc.Object["metadata"], isbsvc.GetGeneration(), isbsvcStatus.ObservedGeneration)

	if !isbsvcReconciled {
		return false, "Mismatch between ISBService Generation and ObservedGeneration", nil
	}
	if statefulSet == nil {
		return false, "StatefulSet not found, may not have been created", nil
	}
	numaLogger.Debugf("statefulset: generation=%d, observedgen=%d", statefulSet.Generation, statefulSet.Status.ObservedGeneration)

	if statefulSet.Generation != statefulSet.Status.ObservedGeneration {
		return false, "Mismatch between StatefulSet Generation and ObservedGeneration", nil
	}
	specifiedReplicas := int32(1)
	if statefulSet.Spec.Replicas != nil {
		specifiedReplicas = *statefulSet.Spec.Replicas
	}

	numaLogger.Debugf("statefulset: generation=%d, observedgen=%d, replicas=%d, updated replicas=%d",
		statefulSet.Generation, statefulSet.Status.ObservedGeneration, specifiedReplicas, statefulSet.Status.UpdatedReplicas)

	if specifiedReplicas != statefulSet.Status.UpdatedReplicas { // TODO: keep this, or is this a better test?: UpdatedRevision == CurrentRevision
		return false, fmt.Sprintf("StatefulSet UpdatedReplicas (%d) != specified replicas (%d)", statefulSet.Status.UpdatedReplicas, specifiedReplicas), nil
	}
	return true, "", nil
}

func (r *ISBServiceRolloutReconciler) processISBServiceStatus(ctx context.Context, isbsvc *unstructured.Unstructured, rollout *apiv1.ISBServiceRollout) {
	numaLogger := logger.FromContext(ctx)
	isbsvcStatus, err := kubernetes.ParseStatus(isbsvc)
	if err != nil {
		numaLogger.Errorf(err, "failed to parse Status from InterstepBuffer CR: %+v, %v", isbsvc, err)
		return
	}

	numaLogger.Debugf("isbsvc status: %+v", isbsvcStatus)

	isbSvcPhase := numaflowv1.ISBSvcPhase(isbsvcStatus.Phase)
	isbsvcChildResourceStatus, isbsvcChildResourceReason := numaflowtypes.GetISBServiceChildResourceHealth(isbsvcStatus.Conditions)

	if isbsvcChildResourceReason == "Progressing" {
		rollout.Status.MarkChildResourcesUnhealthy("Progressing", "ISBService Progressing", rollout.Generation)
	} else if isbSvcPhase == numaflowv1.ISBSvcPhaseFailed || isbsvcChildResourceStatus == "False" {
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcFailed", "ISBService Failed", rollout.Generation)
	} else if isbSvcPhase == numaflowv1.ISBSvcPhasePending || isbsvcChildResourceStatus == "Unknown" {
		rollout.Status.MarkChildResourcesUnhealthy("ISBSvcPending", "ISBService Pending", rollout.Generation)
	} else if isbSvcPhase == numaflowv1.ISBSvcPhaseUnknown {
		rollout.Status.MarkChildResourcesHealthUnknown("ISBSvcUnknown", "ISBService Phase Unknown", rollout.Generation)
	} else {
		reconciled, nonreconciledMsg, err := r.isISBServiceReconciled(ctx, isbsvc, false)
		if err != nil {
			numaLogger.Errorf(err, "failed while determining if ISBService is fully reconciled: %+v, %v", isbsvc, err)
			return
		}
		if reconciled && isbsvcChildResourceStatus == metav1.ConditionTrue {
			rollout.Status.MarkChildResourcesHealthy(rollout.Generation)
		} else {
			rollout.Status.MarkChildResourcesUnhealthy("Progressing", nonreconciledMsg, rollout.Generation)
		}
	}

	// check if PPND strategy is requesting Pipelines to pause, and set true/false
	// (currently, only PPND is accounted for as far as system pausing, not Progressive)
	_ = r.MarkRolloutPaused(ctx, rollout, ppnd.IsRequestingPause(r, rollout))

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
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.ISBServiceRollout{},
		&handler.TypedEnqueueRequestForObject[*apiv1.ISBServiceRollout]{}, ctlrcommon.TypedGenerationChangedPredicate[*apiv1.ISBServiceRollout]{})); err != nil {
		return fmt.Errorf("failed to watch ISBServiceRollout: %v", err)
	}

	// Watch InterStepBufferServices
	isbServiceUns := &unstructured.Unstructured{}
	isbServiceUns.SetGroupVersionKind(schema.GroupVersionKind{
		Kind:    common.NumaflowISBServiceKind,
		Group:   common.NumaflowAPIGroup,
		Version: common.NumaflowAPIVersion,
	})
	if err := controller.Watch(source.Kind(mgr.GetCache(), isbServiceUns,
		handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.ISBServiceRollout{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{})); err != nil {
		return fmt.Errorf("failed to watch InterStepBufferService: %v", err)
	}

	return nil
}

func (r *ISBServiceRolloutReconciler) updateISBServiceRolloutStatus(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) error {

	numaLogger := logger.FromContext(ctx)

	err := r.client.Status().Update(ctx, isbServiceRollout)

	if err != nil && apierrors.IsConflict(err) {
		// there was a Resource Version conflict error (i.e. an update was made to ISBServiceRollout after the version we retrieved), so retry using the latest Resource Version: get the ISBServiceRollout live resource
		// and attach our Status to it.
		// The reason this is okay is because we are the only ones who write the Status, and because we retrieved the live version of this ISBServiceRollout at the beginning of the reconciliation
		// Therefore, we know that the Status is totally current.
		liveISBServiceRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().ISBServiceRollouts(isbServiceRollout.Namespace).Get(ctx, isbServiceRollout.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.Info("ISBServiceRollout not found, %v", err)
				return nil
			}
			return fmt.Errorf("error getting the live ISBServiceRollout after attempting to update the ISBServiceRollout Status: %w", err)
		}
		status := isbServiceRollout.Status // save off the Status
		*isbServiceRollout = *liveISBServiceRollout
		numaLogger.Debug("resource version conflict error after getting latest ISBServiceRollout Status: try again with latest resource version")
		isbServiceRollout.Status = status
		err = r.client.Status().Update(ctx, isbServiceRollout)
		if err != nil {
			return fmt.Errorf("consecutive errors attempting to update ISBServiceRolloutStatus: %w", err)
		}
		return nil
	}
	return err
}

func (r *ISBServiceRolloutReconciler) updateISBServiceRolloutStatusToFailed(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, err error) error {
	isbServiceRollout.Status.MarkFailed(err.Error())
	return r.updateISBServiceRolloutStatus(ctx, isbServiceRollout)
}

func (r *ISBServiceRolloutReconciler) ErrorHandler(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout, err error, reason, msg string) {
	numaLogger := logger.FromContext(ctx)
	numaLogger.Error(err, "ErrorHandler")
	r.customMetrics.ISBServicesROSyncErrors.WithLabelValues().Inc()
	r.recorder.Eventf(isbServiceRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

// Create an InterstepBufferService definition of "promoted" state
func (r *ISBServiceRolloutReconciler) makeTargetISBServiceDef(
	ctx context.Context,
	isbServiceRollout *apiv1.ISBServiceRollout,
) (*unstructured.Unstructured, error) {
	// if a "promoted" InterstepBufferService exists, gets its name; otherwise create a new name
	isbsvcName, err := ctlrcommon.GetChildName(ctx, isbServiceRollout, r, common.LabelValueUpgradePromoted, nil, r.client, true)
	if err != nil {
		return nil, err
	}

	// set Labels and Annotations
	metadata, err := getBaseISBSVCMetadata(isbServiceRollout)
	if err != nil {
		return nil, err
	}
	metadata.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)

	return r.makeISBServiceDefinition(isbServiceRollout, isbsvcName, metadata)
}

func (r *ISBServiceRolloutReconciler) makeISBServiceDefinition(
	isbServiceRollout *apiv1.ISBServiceRollout,
	isbsvcName string,
	metadata apiv1.Metadata,
) (*unstructured.Unstructured, error) {

	args := map[string]interface{}{
		TemplateISBServiceName:      isbsvcName,
		TemplateISBServiceNamespace: isbServiceRollout.Namespace,
	}

	isbServiceSpec, err := util.ResolveTemplateSpec(isbServiceRollout.Spec.InterStepBufferService.Spec, args)
	if err != nil {
		return nil, err
	}

	metadataResolved, err := util.ResolveTemplateSpec(metadata, args)
	if err != nil {
		return nil, err
	}

	newISBServiceDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	newISBServiceDef.Object["spec"] = isbServiceSpec
	newISBServiceDef.Object["metadata"] = metadataResolved
	newISBServiceDef.SetAPIVersion(common.NumaflowAPIGroup + "/" + common.NumaflowAPIVersion)
	newISBServiceDef.SetKind(common.NumaflowISBServiceKind)
	newISBServiceDef.SetName(isbsvcName)
	newISBServiceDef.SetNamespace(isbServiceRollout.Namespace)
	newISBServiceDef.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(isbServiceRollout.GetObjectMeta(), apiv1.ISBServiceRolloutGroupVersionKind)})

	return newISBServiceDef, nil
}

// take the Metadata (Labels and Annotations) specified in the ISBServiceRollout plus any others that apply to all InterstepBufferServices
func getBaseISBSVCMetadata(isbServiceRollout *apiv1.ISBServiceRollout) (apiv1.Metadata, error) {
	labelMapping := map[string]string{}
	for key, val := range isbServiceRollout.Spec.InterStepBufferService.Labels {
		labelMapping[key] = val
	}
	labelMapping[common.LabelKeyParentRollout] = isbServiceRollout.Name

	return apiv1.Metadata{Labels: labelMapping, Annotations: isbServiceRollout.Spec.InterStepBufferService.Annotations}, nil

}

// take the existing ISBService and merge anything needed from the new ISBService definition
func (r *ISBServiceRolloutReconciler) merge(existingISBService, newISBService *unstructured.Unstructured) *unstructured.Unstructured {
	resultISBService := existingISBService.DeepCopy()
	resultISBService.Object["spec"] = newISBService.Object["spec"]
	resultISBService.SetAnnotations(util.MergeMaps(existingISBService.GetAnnotations(), newISBService.GetAnnotations()))
	resultISBService.SetLabels(util.MergeMaps(existingISBService.GetLabels(), newISBService.GetLabels()))
	return resultISBService
}

// ChildNeedsUpdating() tests for essential equality, with any fields that Numaplane manipulates eliminated from the comparison
// This implements a function of the progressiveController interface
func (r *ISBServiceRolloutReconciler) ChildNeedsUpdating(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	specsEqual := util.CompareStructNumTypeAgnostic(from.Object["spec"], to.Object["spec"])
	numaLogger.Debugf("specsEqual: %t, from=%v, to=%v\n",
		specsEqual, from, to)
	labelsEqual := util.CompareMaps(from.GetLabels(), to.GetLabels())
	numaLogger.Debugf("labelsEqual: %t, from Labels=%v, to Labels=%v", labelsEqual, from.GetLabels(), to.GetLabels())
	annotationsEqual := util.CompareMaps(from.GetAnnotations(), to.GetAnnotations())
	numaLogger.Debugf("annotationsEqual: %t, from Annotations=%v, to Annotations=%v", annotationsEqual, from.GetAnnotations(), to.GetAnnotations())

	return !specsEqual || !labelsEqual || !annotationsEqual, nil
}

func (r *ISBServiceRolloutReconciler) getCurrentChildCount(rolloutObject ctlrcommon.RolloutObject) (int32, bool) {
	isbsvcRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	if isbsvcRollout.Status.NameCount == nil {
		return int32(0), false
	} else {
		return *isbsvcRollout.Status.NameCount, true
	}
}

func (r *ISBServiceRolloutReconciler) updateCurrentChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, nameCount int32) error {
	isbsvcRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	isbsvcRollout.Status.NameCount = &nameCount
	return r.updateISBServiceRolloutStatus(ctx, isbsvcRollout)
}

// IncrementChildCount updates the count of children for the Resource in Kubernetes and returns the index that should be used for the next child
// This implements a function of the RolloutController interface
func (r *ISBServiceRolloutReconciler) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return int32(0), err
		}
	}

	// For readability of the InterstepBufferService name, keep the count from getting too high by rolling around back to 0
	// TODO: consider handling the extremely rare case that user still has a "promoted" child of index 0 running
	nextNameCount := currentNameCount + 1
	if nextNameCount > common.MaxNameCount {
		nextNameCount = 0
	}

	err := r.updateCurrentChildCount(ctx, rolloutObject, nextNameCount)
	if err != nil {
		return int32(0), err
	}
	return currentNameCount, nil
}

// Recycle deletes child; returns true if it was in fact deleted
// This implements a function of the RolloutController interface
func (r *ISBServiceRolloutReconciler) Recycle(ctx context.Context, isbsvc *unstructured.Unstructured, c client.Client) (bool, error) {
	numaLogger := logger.FromContext(ctx).WithValues("isbsvc", fmt.Sprintf("%s/%s", isbsvc.GetNamespace(), isbsvc.GetName()))

	// For InterstepBufferService, the main thing is that we don't want to delete it until we can be sure there are no
	// Pipelines using it

	pipelines, err := r.getPipelineListForChildISBSvc(ctx, isbsvc.GetNamespace(), isbsvc.GetName())
	if err != nil {
		return false, fmt.Errorf("can't recycle isbsvc %s/%s; got error retrieving pipelines using it: %s", isbsvc.GetNamespace(), isbsvc.GetName(), err)
	}
	if pipelines != nil && len(pipelines.Items) > 0 {
		numaLogger.Debugf("can't recycle isbsvc; there are still %d pipelines using it", len(pipelines.Items))
		return false, nil
	}
	// okay to delete now
	numaLogger.Debug("deleting isbsvc")
	err = kubernetes.DeleteResource(ctx, c, isbsvc)
	if err != nil {
		return false, err
	}
	return true, nil
}

func getLiveISBServiceRollout(ctx context.Context, name, namespace string) (*apiv1.ISBServiceRollout, error) {
	isbServiceRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().ISBServiceRollouts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return isbServiceRollout, err
	}
	isbServiceRollout.SetGroupVersionKind(apiv1.ISBServiceRolloutGroupVersionKind)

	return isbServiceRollout, err
}

// Get the list of Riders that we need based on what's defined in the ISBServiceRollout, templated according to the isbsvc child's name
// (isbsvcDef is not used and comes from the RolloutController interface)

func (r *ISBServiceRolloutReconciler) GetDesiredRiders(rolloutObject ctlrcommon.RolloutObject, isbsvcName string, isbsvcDef *unstructured.Unstructured) ([]riders.Rider, error) {
	isbServiceRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	desiredRiders := []riders.Rider{}
	for _, rider := range isbServiceRollout.Spec.Riders {
		var asMap map[string]interface{}
		if err := util.StructToStruct(rider.Definition, &asMap); err != nil {
			return desiredRiders, fmt.Errorf("rider definition could not converted to map: %w", err)
		}
		resolvedMap, err := util.ResolveTemplateSpec(asMap, map[string]interface{}{
			TemplateISBServiceName:      isbsvcName,
			TemplateISBServiceNamespace: isbServiceRollout.Namespace,
		})
		if err != nil {
			return desiredRiders, err
		}
		unstruc := unstructured.Unstructured{}
		unstruc.Object = resolvedMap
		unstruc.SetNamespace(isbServiceRollout.Namespace)
		unstruc.SetName(fmt.Sprintf("%s-%s", unstruc.GetName(), isbsvcName))
		desiredRiders = append(desiredRiders, riders.Rider{Definition: unstruc, RequiresProgressive: rider.Progressive})
	}

	// verify that desiredRiders are all permitted Kinds
	// err := riders.VerifyRiders(desiredRiders)
	if !riders.VerifyRiders(desiredRiders) {
		return desiredRiders, fmt.Errorf("rider definitions contained unpermitted Kind")
	}

	return desiredRiders, nil
}

// Get the Riders that have been deployed
// If "upgrading==true", return those which are associated with the Upgrading isbsvc; otherwise return those which are associated with the Promoted one
func (r *ISBServiceRolloutReconciler) GetExistingRiders(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgrading bool) (unstructured.UnstructuredList, error) {
	isbServiceRollout := rolloutObject.(*apiv1.ISBServiceRollout)

	ridersList := isbServiceRollout.Status.Riders // use the Riders for the promoted isbservice
	if upgrading {
		ridersList = isbServiceRollout.Status.ProgressiveStatus.UpgradingISBServiceStatus.Riders // use the Riders for the upgrading isbservice
	}

	return riders.GetRidersFromK8S(ctx, isbServiceRollout.GetNamespace(), ridersList, r.client)
}

// listAndDeleteChildISBServices lists all child ISBServices and deletes them
// return true if we need to requeue
func (r *ISBServiceRolloutReconciler) listAndDeleteChildISBServices(ctx context.Context, isbServiceRollout *apiv1.ISBServiceRollout) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	isbServiceList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, numaflowv1.ISBGroupVersionResource.Resource,
		isbServiceRollout.Namespace, fmt.Sprintf("%s=%s", common.LabelKeyParentRollout, isbServiceRollout.Name), "")
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Warnf("no child ISBServices found for ISBServiceRollout %s/%s: %v", isbServiceRollout.Namespace, isbServiceRollout.Name, err)
			return false, nil
		}
		return false, err
	}
	if isbServiceList != nil && len(isbServiceList.Items) > 0 {
		// Delete all isbServices that are children of this ISBServiceRollout
		numaLogger.Infof("Deleting ISBService %s/%s", isbServiceRollout.Namespace, isbServiceRollout.Name)
		for _, isbService := range isbServiceList.Items {
			if err := r.client.Delete(ctx, &isbService); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

func (r *ISBServiceRolloutReconciler) SetCurrentRiderList(rolloutObject ctlrcommon.RolloutObject, riders []riders.Rider) {

	isbServiceRollout := rolloutObject.(*apiv1.ISBServiceRollout)
	isbServiceRollout.Status.Riders = make([]apiv1.RiderStatus, len(riders))
	for index, rider := range riders {
		isbServiceRollout.Status.Riders[index] = apiv1.RiderStatus{
			GroupVersionKind: kubernetes.SchemaGVKToMetaGVK(rider.Definition.GroupVersionKind()),
			Name:             rider.Definition.GetName(),
		}
	}
}
