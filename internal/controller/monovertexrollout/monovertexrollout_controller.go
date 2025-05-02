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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	argorolloutsv1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
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
	TemplateMonoVertexName      = ".monovertex-name"
	TemplateMonoVertexNamespace = ".monovertex-namespace"
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

	// Get the live MonoVertexRollout since we need latest Status for Progressive rollout case
	// TODO: consider storing MonoVertexRollout Status in a local cache instead of this
	monoVertexRollout, err := getLiveMonovertexRollout(ctx, req.NamespacedName.Name, req.NamespacedName.Namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Infof("MonoVertxRollout not found, %v", err)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("error getting the live monoVertex rollout: %w", err)
	}

	// store copy of original rollout
	monoVertexRolloutOrig := monoVertexRollout
	monoVertexRollout = monoVertexRolloutOrig.DeepCopy()

	monoVertexRollout.Status.Init(monoVertexRollout.Generation)

	result, err := r.reconcile(ctx, monoVertexRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(ctx, monoVertexRollout, err, "ReconcileFailed", "Failed to reconcile MonoVertexRollout")
		statusUpdateErr := r.updateMonoVertexRolloutStatusToFailed(ctx, monoVertexRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(ctx, monoVertexRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update MonoVertexRollout status")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	// Update the resource definition (everything except the Status subresource)
	if r.needsUpdate(monoVertexRolloutOrig, monoVertexRollout) {
		if err := r.client.Patch(ctx, monoVertexRollout, client.MergeFrom(monoVertexRolloutOrig)); err != nil {
			r.ErrorHandler(ctx, monoVertexRollout, err, "UpdateFailed", "Failed to patch MonoVertexRollout")
			if statusUpdateErr := r.updateMonoVertexRolloutStatusToFailed(ctx, monoVertexRollout, err); statusUpdateErr != nil {
				r.ErrorHandler(ctx, monoVertexRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update MonoVertexRollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
	}

	if monoVertexRollout.DeletionTimestamp.IsZero() {
		statusUpdateErr := r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(ctx, monoVertexRollout, statusUpdateErr, "StatusUpdateFailed", "Failed to update MonoVertexRollout")
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
	requeueDelay := time.Duration(0)

	defer func() {
		r.customMetrics.SetMonoVerticesRolloutHealth(monoVertexRollout.Namespace, monoVertexRollout.Name, string(monoVertexRollout.Status.Phase))
	}()

	// Remove the finalizer if it still exists in the MonoVertexRollout, as finalizer is no longer needed for MonoVertexRollout
	if controllerutil.ContainsFinalizer(monoVertexRollout, common.FinalizerName) {
		controllerutil.RemoveFinalizer(monoVertexRollout, common.FinalizerName)
	}

	// Update metrics if monoVertexRollout is being deleted
	if !monoVertexRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting MonoVertexRollout")
		// generate metrics for MonoVertex deletion
		r.customMetrics.DecMonoVertexRollouts(monoVertexRollout.Name, monoVertexRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "delete").Observe(time.Since(startTime).Seconds())
		r.customMetrics.DeleteMonoVerticesRolloutHealth(monoVertexRollout.Namespace, monoVertexRollout.Name)
		return ctrl.Result{}, nil
	}

	// check if there's a promoted monovertex yet
	promotedMonovertices, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, monoVertexRollout, common.LabelValueUpgradePromoted, nil, false, r.client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error looking for promoted monovertex: %v", err)
	}

	newMonoVertexDef, err := r.makeTargetMonoVertexDefinition(ctx, monoVertexRollout)
	if err != nil {
		return ctrl.Result{}, err
	}

	if newMonoVertexDef != nil {
		if promotedMonovertices == nil || len(promotedMonovertices.Items) == 0 {

			numaLogger.Debugf("MonoVertex %s/%s doesn't exist so creating", monoVertexRollout.Namespace, monoVertexRollout.Name)
			monoVertexRollout.Status.MarkPending()

			if err := kubernetes.CreateResource(ctx, r.client, newMonoVertexDef); err != nil {
				return ctrl.Result{}, err
			}

			if err := ctlrcommon.CreateRidersForNewChild(ctx, r, monoVertexRollout, newMonoVertexDef, r.client); err != nil {
				return ctrl.Result{}, fmt.Errorf("error creating riders: %s", err)
			}

			monoVertexRollout.Status.MarkDeployed(monoVertexRollout.Generation)
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "create").Observe(time.Since(startTime).Seconds())

		} else {
			existingMonoVertexDef, err := kubernetes.GetResource(ctx, r.client, newMonoVertexDef.GroupVersionKind(),
				k8stypes.NamespacedName{Namespace: newMonoVertexDef.GetNamespace(), Name: newMonoVertexDef.GetName()})
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("error getting MonoVertex: %v", err)
			}
			// merge and update
			newMonoVertexDef, err = r.merge(existingMonoVertexDef, newMonoVertexDef)
			if err != nil {
				return ctrl.Result{}, err
			}
			requeueDelay, err = r.processExistingMonoVertex(ctx, monoVertexRollout, existingMonoVertexDef, newMonoVertexDef, syncStartTime)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("error processing existing MonoVertex: %v", err)
			}
			if requeueDelay > 0 {
				return ctrl.Result{RequeueAfter: requeueDelay}, nil
			}
			// process status
			r.processMonoVertexStatus(ctx, existingMonoVertexDef, monoVertexRollout)
		}
	}

	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, monoVertexRollout)

	// clean up recyclable monovertices
	allDeleted, err := ctlrcommon.GarbageCollectChildren(ctx, monoVertexRollout, r, r.client)
	if err != nil {
		return ctrl.Result{}, err
	}

	// if we still have monovertices that need deleting, or if we're in the middle of an upgrade strategy, then requeue
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

// return a requeue delay greater than 0 if requeue is needed, and return error if any (if returning an error, we will requeue anyway)
func (r *MonoVertexRolloutReconciler) processExistingMonoVertex(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout,
	existingMonoVertexDef, newMonoVertexDef *unstructured.Unstructured, syncStartTime time.Time) (time.Duration, error) {

	numaLogger := logger.FromContext(ctx)

	// get the list of Riders that we need based on the MonoVertexRollout definition
	currentRiderList, err := r.GetDesiredRiders(monoVertexRollout, existingMonoVertexDef.GetName(), newMonoVertexDef)
	if err != nil {
		return 0, fmt.Errorf("error getting desired Riders for MonoVertex %s: %s", existingMonoVertexDef.GetName(), err)
	}
	// get the list of Riders that we have now (for promoted child)
	existingRiderList, err := r.GetExistingRiders(ctx, monoVertexRollout, false)
	if err != nil {
		return 0, fmt.Errorf("error getting existing Riders for MonoVertex %s: %s", existingMonoVertexDef.GetName(), err)
	}

	// determine if we're trying to update the MonoVertex spec
	// if it's a simple change, direct apply
	// if not and if user-preferred strategy is "Progressive", it will require Progressive rollout to perform the update with guaranteed no-downtime
	// and capability to rollback an unhealthy one
	needsUpdate, upgradeStrategyType, _, riderAdditions, riderModifications, riderDeletions, err := usde.ResourceNeedsUpdating(ctx, newMonoVertexDef, existingMonoVertexDef, currentRiderList, existingRiderList)
	if err != nil {
		return 0, err
	}
	numaLogger.WithValues(
		"needsUpdate", needsUpdate,
		"upgradeStrategyType", upgradeStrategyType).Debug("Upgrade decision result")

	// set the Status appropriately to "Pending" or "Deployed"
	// if needsUpdate - this means we need to deploy a change
	// Note that this will be reset to "Deployed" later on if a deployment occurs
	if needsUpdate {
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

	requeueDelay := time.Duration(0) // 0 means "no requeue"

	switch inProgressStrategy {
	case apiv1.UpgradeStrategyProgressive:
		numaLogger.Debug("processing MonoVertex with Progressive")

		// don't risk out-of-date cache while performing Progressive strategy - get
		// the most current version of the MonoVertex just in case
		existingMonoVertexDef, err = kubernetes.GetLiveResource(ctx, newMonoVertexDef, "monovertices")
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.WithValues("monoVertexDefinition", *existingMonoVertexDef).Warn("MonoVertex not found.")
				return 0, nil
			} else {
				return 0, fmt.Errorf("error getting MonoVertex for status processing: %v", err)
			}
		}

		done, progressiveRequeueDelay, err := progressive.ProcessResource(ctx, monoVertexRollout, existingMonoVertexDef, needsUpdate, r, r.client)
		if err != nil {
			return 0, err
		}
		if done {

			// update the list of riders in the Status based on our child which was just promoted
			currentRiderList, err := r.GetDesiredRiders(monoVertexRollout, existingMonoVertexDef.GetName(), newMonoVertexDef)
			if err != nil {
				return 0, fmt.Errorf("error getting desired Riders for MonoVertex %s: %s", newMonoVertexDef.GetName(), err)
			}
			r.SetCurrentRiderList(monoVertexRollout, currentRiderList)

			// we need to prevent the possibility that we're done but we fail to update the Progressive Status
			// therefore, we publish Rollout.Status here, so if that fails, then we won't be "done" and so we'll come back in here to try again
			err = r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
			if err != nil {
				return 0, err
			}

			r.inProgressStrategyMgr.UnsetStrategy(ctx, monoVertexRollout)
			monoVertexRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus = nil
		} else {
			requeueDelay = progressiveRequeueDelay
		}

	default:
		if needsUpdate {
			err := r.updateMonoVertex(ctx, monoVertexRollout, newMonoVertexDef)
			if err != nil {
				return 0, err
			}
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerMonoVertexRollout, "update").Observe(time.Since(syncStartTime).Seconds())

			// update the cluster to reflect the Rider additions, modifications, and deletions
			if err := riders.UpdateRidersInK8S(ctx, newMonoVertexDef, riderAdditions, riderModifications, riderDeletions, r.client); err != nil {
				return 0, err
			}
		}
		// update the list of riders in the Status
		r.SetCurrentRiderList(monoVertexRollout, currentRiderList)
	}

	return requeueDelay, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MonoVertexRolloutReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {

	numaLogger := logger.FromContext(ctx)

	controller, err := runtimecontroller.New(ControllerMonoVertexRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch MonoVertexRollouts
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.MonoVertexRollout{},
		&handler.TypedEnqueueRequestForObject[*apiv1.MonoVertexRollout]{}, ctlrcommon.TypedGenerationChangedPredicate[*apiv1.MonoVertexRollout]{})); err != nil {
		return fmt.Errorf("failed to watch MonoVertexRollouts: %w", err)
	}

	// Watch MonoVertices
	monoVertexUns := &unstructured.Unstructured{}
	monoVertexUns.SetGroupVersionKind(schema.GroupVersionKind{
		Kind:    common.NumaflowMonoVertexKind,
		Group:   common.NumaflowAPIGroup,
		Version: common.NumaflowAPIVersion,
	})
	if err := controller.Watch(
		source.Kind(mgr.GetCache(), monoVertexUns,
			handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](mgr.GetScheme(), mgr.GetRESTMapper(), &apiv1.MonoVertexRollout{}, handler.OnlyControllerOwner()),
			predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{})); err != nil {
		return fmt.Errorf("failed to watch MonoVertices: %w", err)
	}

	// Watch AnalysisRuns that are owned by the MonoVertices that MonoVertexRollout owns (this enqueues the MonoVertexRollout)
	if err := controller.Watch(
		source.Kind(mgr.GetCache(), &argorolloutsv1.AnalysisRun{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, analysisRun *argorolloutsv1.AnalysisRun) []reconcile.Request {

				var reqs []reconcile.Request

				// Check if MonoVertex is the owner
				for _, analysisRunOwner := range analysisRun.GetOwnerReferences() {
					// Check if the owner is of Kind 'MonoVertex' and is marked as "Controller"
					if analysisRunOwner.Kind == "MonoVertex" && *analysisRunOwner.Controller {

						// find the MonoVertex so we can enqueue the MonoVertexRollout which owns it (if one does)
						monoVertex, err := kubernetes.GetResource(ctx, r.client, numaflowv1.MonoVertexGroupVersionKind,
							k8stypes.NamespacedName{Namespace: analysisRun.GetNamespace(), Name: analysisRunOwner.Name})
						if err != nil {
							numaLogger.WithValues(
								"AnalysisRun", fmt.Sprintf("%s:%s", analysisRun.Namespace, analysisRun.Name),
								"MonoVertex", fmt.Sprintf("%s:%s", analysisRun.Namespace, analysisRunOwner.Name)).Warnf("Unable to get MonoVertex owner of AnalysisRun")
							continue
						}

						// See if a MonoVertexRollout owns the MonoVertex: if so, enqueue it
						for _, monovertexOwner := range monoVertex.GetOwnerReferences() {
							if monovertexOwner.Kind == "MonoVertexRollout" && *monovertexOwner.Controller {
								reqs = append(reqs, reconcile.Request{
									NamespacedName: types.NamespacedName{
										Name:      monovertexOwner.Name,
										Namespace: analysisRun.GetNamespace(),
									},
								})
							}
						}
					}
				}
				return reqs
			}),
			predicate.TypedResourceVersionChangedPredicate[*argorolloutsv1.AnalysisRun]{})); err != nil {

		return fmt.Errorf("failed to watch AnalysisRuns: %w", err)
	}

	return nil
}

func (r *MonoVertexRolloutReconciler) merge(existingMonoVertex, newMonoVertex *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	resultMonoVertex := existingMonoVertex.DeepCopy()

	var specAsMap map[string]interface{}
	if err := util.StructToStruct(newMonoVertex.Object["spec"], &specAsMap); err != nil {
		return resultMonoVertex, fmt.Errorf("failed to get spec from new MonoVertex: %w", err)
	}
	resultMonoVertex.Object["spec"] = specAsMap

	resultMonoVertex.SetAnnotations(util.MergeMaps(existingMonoVertex.GetAnnotations(), newMonoVertex.GetAnnotations()))
	resultMonoVertex.SetLabels(util.MergeMaps(existingMonoVertex.GetLabels(), newMonoVertex.GetLabels()))

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
		reason := fmt.Sprintf("MonoVertex%s", string(mvtxPhase))
		msg := fmt.Sprintf("MonoVertex %s", strings.ToLower(string(mvtxPhase)))
		rollout.Status.MarkMonoVertexPaused(reason, msg, rollout.Generation)
	} else {
		rollout.Status.MarkMonoVertexUnpaused(rollout.Generation)
	}

}

func (r *MonoVertexRolloutReconciler) updateMonoVertex(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, newMonoVertexDef *unstructured.Unstructured) error {
	err := kubernetes.UpdateResource(ctx, r.client, newMonoVertexDef)
	if err != nil {
		return err
	}

	monoVertexRollout.Status.MarkDeployed(monoVertexRollout.Generation)
	return nil
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

func (r *MonoVertexRolloutReconciler) updateMonoVertexRolloutStatus(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout) error {
	numaLogger := logger.FromContext(ctx)
	err := r.client.Status().Update(ctx, monoVertexRollout)

	if err != nil && apierrors.IsConflict(err) {
		// there was a Resource Version conflict error (i.e. an update was made to MonoVertexRollout after the version we retrieved), so retry using the latest Resource Version: get the MonoVertexRollout live resource
		// and attach our Status to it
		// The reason this is okay is because we are the only ones who write the Status, and because we retrieved the live version of this ISBServiceRollout at the beginning of the reconciliation
		// Therefore, we know that the Status is totally current.
		liveRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().MonoVertexRollouts(monoVertexRollout.Namespace).Get(ctx, monoVertexRollout.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.Infof("MonoVertxRollout not found, %v", err)
				return nil
			}
			return fmt.Errorf("error getting the live MonoVertexRollout after attempting to update the MonoVertexRollout Status: %w", err)
		}
		status := monoVertexRollout.Status // save off the Status
		*monoVertexRollout = *liveRollout
		numaLogger.Debug("resource version conflict error after getting latest MonoVertexRollout Status: try again with latest resource version")
		monoVertexRollout.Status = status
		err = r.client.Status().Update(ctx, monoVertexRollout)
		if err != nil {
			return fmt.Errorf("consecutive errors attempting to update MonoVertexRollout: %w", err)
		}
		return nil
	}
	return err
}

func (r *MonoVertexRolloutReconciler) updateMonoVertexRolloutStatusToFailed(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, err error) error {
	monoVertexRollout.Status.MarkFailed(err.Error())
	return r.updateMonoVertexRolloutStatus(ctx, monoVertexRollout)
}

func (r *MonoVertexRolloutReconciler) ErrorHandler(ctx context.Context, monoVertexRollout *apiv1.MonoVertexRollout, err error, reason, msg string) {
	numaLogger := logger.FromContext(ctx)
	numaLogger.Error(err, "ErrorHandler")
	r.customMetrics.MonoVertexROSyncErrors.WithLabelValues().Inc()
	r.recorder.Eventf(monoVertexRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

func getMonoVertexChildResourceHealth(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, cond := range conditions {
		switch cond.Type {
		case "DaemonHealthy", "PodsHealthy":
			if cond.Status != metav1.ConditionTrue {
				return cond.Status, cond.Reason
			}
		}
	}
	return metav1.ConditionTrue, ""
}

// create the definition for the MonoVertex child of the Rollout which is labeled "promoted"
func (r *MonoVertexRolloutReconciler) makeTargetMonoVertexDefinition(
	ctx context.Context,
	monoVertexRollout *apiv1.MonoVertexRollout,
) (*unstructured.Unstructured, error) {
	monoVertexName, err := ctlrcommon.GetChildName(ctx, monoVertexRollout, r, common.LabelValueUpgradePromoted, nil, r.client, true)
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

	args := map[string]interface{}{
		TemplateMonoVertexName:      monoVertexName,
		TemplateMonoVertexNamespace: monoVertexRollout.Namespace,
	}

	monoVertexSpec, err := util.ResolveTemplateSpec(monoVertexRollout.Spec.MonoVertex.Spec, args)
	if err != nil {
		return nil, err
	}

	metadataResolved, err := util.ResolveTemplateSpec(metadata, args)
	if err != nil {
		return nil, err
	}

	monoVertexDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	monoVertexDef.Object["spec"] = monoVertexSpec
	monoVertexDef.Object["metadata"] = metadataResolved
	monoVertexDef.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)
	monoVertexDef.SetName(monoVertexName)
	monoVertexDef.SetNamespace(monoVertexRollout.Namespace)
	monoVertexDef.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(monoVertexRollout.GetObjectMeta(), apiv1.MonoVertexRolloutGroupVersionKind)})

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

// ChildNeedsUpdating() tests for essential equality, with any fields that Numaplane manipulates eliminated from the comparison
// This implements a function of the progressiveController interface
func (r *MonoVertexRolloutReconciler) ChildNeedsUpdating(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// remove certain fields (which numaplane needs to set) from comparison to test for equality
	removeFunc := func(monoVertex *unstructured.Unstructured) (map[string]interface{}, error) {
		var specAsMap map[string]any

		if err := util.StructToStruct(monoVertex.Object["spec"], &specAsMap); err != nil {
			return nil, err
		}

		excludedPaths := []string{"replicas", "scale.min", "scale.max"}
		util.RemovePaths(specAsMap, excludedPaths, ".")

		// if "scale" is there and empty, remove it
		// (this enables accurate comparison between one monovertex with "scale" empty and one with "scale" not present)
		scaleMap, found := specAsMap["scale"].(map[string]interface{})
		if found && len(scaleMap) == 0 {
			unstructured.RemoveNestedField(specAsMap, "scale")
		}

		return specAsMap, nil
	}

	fromNew, err := removeFunc(from)
	if err != nil {
		return false, err
	}
	toNew, err := removeFunc(to)
	if err != nil {
		return false, err
	}

	specsEqual := util.CompareStructNumTypeAgnostic(fromNew, toNew)
	numaLogger.Debugf("specsEqual: %t, fromNew=%v, toNew=%v\n",
		specsEqual, fromNew, toNew)
	labelsEqual := util.CompareMaps(from.GetLabels(), to.GetLabels())
	numaLogger.Debugf("labelsEqual: %t, from Labels=%v, to Labels=%v", labelsEqual, from.GetLabels(), to.GetLabels())
	annotationsEqual := util.CompareMaps(from.GetAnnotations(), to.GetAnnotations())
	numaLogger.Debugf("annotationsEqual: %t, from Annotations=%v, to Annotations=%v", annotationsEqual, from.GetAnnotations(), to.GetAnnotations())

	return !specsEqual || !labelsEqual || !annotationsEqual, nil

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

// IncrementChildCount increments the child count for the Rollout and returns the count to use
// This implements a function of the RolloutController interface
func (r *MonoVertexRolloutReconciler) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return int32(0), err
		}
	}
	// For readability of the monovertex name, keep the count from getting too high by rolling around back to 0
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
func (r *MonoVertexRolloutReconciler) Recycle(ctx context.Context,
	monoVertexDef *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	err := kubernetes.DeleteResource(ctx, c, monoVertexDef)
	if err != nil {
		return false, err
	}
	return true, nil
}

func getLiveMonovertexRollout(ctx context.Context, name, namespace string) (*apiv1.MonoVertexRollout, error) {
	monoVertexRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().MonoVertexRollouts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return monoVertexRollout, err
	}
	monoVertexRollout.SetGroupVersionKind(apiv1.MonoVertexRolloutGroupVersionKind)

	return monoVertexRollout, err
}

// Get the list of Riders that we need based on what's defined in the MonoVertexRollout, templated according to the monoVertex child's name
// (monoVertexDef is not used and comes from the RolloutController interface)
func (r *MonoVertexRolloutReconciler) GetDesiredRiders(rolloutObject ctlrcommon.RolloutObject, monoVertexName string, monoVertexDef *unstructured.Unstructured) ([]riders.Rider, error) {
	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	desiredRiders := []riders.Rider{}
	for _, rider := range monoVertexRollout.Spec.Riders {
		var asMap map[string]interface{}
		if err := util.StructToStruct(rider.Definition, &asMap); err != nil {
			return desiredRiders, fmt.Errorf("rider definition could not converted to map: %w", err)
		}
		resolvedMap, err := util.ResolveTemplateSpec(asMap, map[string]interface{}{
			TemplateMonoVertexName:      monoVertexName,
			TemplateMonoVertexNamespace: monoVertexRollout.Namespace,
		})
		if err != nil {
			return desiredRiders, err
		}
		unstruc := unstructured.Unstructured{}
		unstruc.Object = resolvedMap
		unstruc.SetNamespace(monoVertexRollout.Namespace)
		unstruc.SetName(fmt.Sprintf("%s-%s", unstruc.GetName(), monoVertexName))
		desiredRiders = append(desiredRiders, riders.Rider{Definition: unstruc, RequiresProgressive: rider.Progressive})
	}

	// verify that desiredRiders are all permitted Kinds
	if !riders.VerifyRidersPermitted(desiredRiders) {
		return desiredRiders, fmt.Errorf("rider definitions contained unpermitted Kind")
	}

	return desiredRiders, nil
}

// Get the Riders that have been deployed
// If "upgrading==true", return those which are associated with the Upgrading MonoVertex; otherwise return those which are associated with the Promoted one
func (r *MonoVertexRolloutReconciler) GetExistingRiders(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgrading bool) (unstructured.UnstructuredList, error) {

	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)

	ridersList := monoVertexRollout.Status.Riders // use the Riders for the promoted monovertex
	if upgrading {
		ridersList = monoVertexRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus.Riders // use the Riders for the upgrading monovertex
	}

	return riders.GetRidersFromK8S(ctx, monoVertexRollout.GetNamespace(), ridersList, r.client)
}

// update Status to reflect the current Riders (for promoted monovertex)
func (r *MonoVertexRolloutReconciler) SetCurrentRiderList(
	rolloutObject ctlrcommon.RolloutObject,
	riders []riders.Rider) {

	monoVertexRollout := rolloutObject.(*apiv1.MonoVertexRollout)
	monoVertexRollout.Status.Riders = make([]apiv1.RiderStatus, len(riders))
	for index, rider := range riders {
		monoVertexRollout.Status.Riders[index] = apiv1.RiderStatus{
			GroupVersionKind: kubernetes.SchemaGVKToMetaGVK(rider.Definition.GroupVersionKind()),
			Name:             rider.Definition.GetName(),
		}
	}

}
