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

package pipelinerollout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sRuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
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
	"github.com/numaproj/numaplane/internal/controller/common/numaflowtypes"
	"github.com/numaproj/numaplane/internal/controller/common/riders"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/controller/progressive"
	"github.com/numaproj/numaplane/internal/usde"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerPipelineRollout = "pipeline-rollout-controller"
	loggerName                = "pipelinerollout-reconciler"
	numWorkers                = 16 // can consider making configurable

	defaultPauseGracePeriodSeconds = 30
)

var (
	PipelineROReconciler *PipelineRolloutReconciler
	initTime             time.Time
)

// PipelineRolloutReconciler reconciles a PipelineRollout object
type PipelineRolloutReconciler struct {
	client client.Client
	scheme *k8sRuntime.Scheme

	// Queue contains the list of PipelineRollouts that currently need to be reconciled
	// both PipelineRolloutReconciler.Reconcile() and other Rollout reconcilers can add PipelineRollouts to this Queue to be processed as needed
	// a set of Workers is used to process this Queue
	Queue workqueue.TypedRateLimitingInterface[interface{}]
	// shutdownWorkerWaitGroup is used when shutting down the workers processing the queue for them to indicate that they're done
	shutdownWorkerWaitGroup *sync.WaitGroup
	// customMetrics is used to generate the custom metrics for the Pipeline
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder

	// maintain inProgressStrategies in memory and in PipelineRollout Status
	inProgressStrategyMgr *ctlrcommon.InProgressStrategyMgr
}

func NewPipelineRolloutReconciler(
	c client.Client,
	s *k8sRuntime.Scheme,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *PipelineRolloutReconciler {

	numaLogger := logger.GetBaseLogger().WithName(loggerName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx := logger.WithLogger(context.Background(), numaLogger)

	// create a queue to process PipelineRollout reconciliations
	// the benefit of the queue is that other reconciliation code can also add PipelineRollouts to it so they'll be processed
	pipelineRolloutQueue := util.NewWorkQueue("pipeline_rollout_queue")

	r := &PipelineRolloutReconciler{
		c,
		s,
		pipelineRolloutQueue,
		&sync.WaitGroup{},
		customMetrics,
		recorder,
		nil, // defined below
	}
	PipelineROReconciler = r

	PipelineROReconciler.inProgressStrategyMgr = ctlrcommon.NewInProgressStrategyMgr(
		// getRolloutStrategy function:
		func(ctx context.Context, rollout client.Object) *apiv1.UpgradeStrategy {
			pipelineRollout := rollout.(*apiv1.PipelineRollout)

			if pipelineRollout.Status.UpgradeInProgress != "" {
				return (*apiv1.UpgradeStrategy)(&pipelineRollout.Status.UpgradeInProgress)
			} else {
				return nil
			}
		},
		// setRolloutStrategy function:
		func(ctx context.Context, rollout client.Object, strategy apiv1.UpgradeStrategy) {
			pipelineRollout := rollout.(*apiv1.PipelineRollout)
			pipelineRollout.Status.SetUpgradeInProgress(strategy)
		},
	)

	r.runWorkers(ctx)

	return r
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=pipelinerollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=pipelinerollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=pipelinerollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *PipelineRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	numaLogger := logger.GetBaseLogger().WithName(loggerName).WithValues("pipelinerollout", req.NamespacedName)
	r.EnqueuePipeline(req.NamespacedName)
	numaLogger.Debugf("PipelineRollout Reconciler added PipelineRollout %v to queue", req.NamespacedName)
	r.customMetrics.PipelineRolloutQueueLength.WithLabelValues().Set(float64(r.Queue.Len()))
	return ctrl.Result{}, nil
}

func (r *PipelineRolloutReconciler) EnqueuePipeline(namespacedName k8stypes.NamespacedName) {
	key := namespacedNameToKey(namespacedName)
	r.Queue.Add(key)
}

func (r *PipelineRolloutReconciler) processPipelineRollout(ctx context.Context, namespacedName k8stypes.NamespacedName) (ctrl.Result, error) {
	syncStartTime := time.Now()
	numaLogger := logger.FromContext(ctx).WithValues("pipelinerollout", namespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)
	r.customMetrics.PipelineROSyncs.WithLabelValues().Inc()

	// Get the live PipelineRollout since we need latest Status for Progressive rollout case
	// TODO: consider storing PipelineRollout Status in a local cache instead of this
	pipelineRollout, err := getLivePipelineRollout(ctx, namespacedName.Name, namespacedName.Namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Infof("PipelineRollout not found, %v", err)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("error getting the live PipelineRollout: %w", err)
	}

	// save off a copy of the original before we modify it
	pipelineRolloutOrig := pipelineRollout
	pipelineRollout = pipelineRolloutOrig.DeepCopy()

	pipelineRollout.Status.Init(pipelineRollout.Generation)

	requeueDelay, existingPipelineDef, err := r.reconcile(ctx, pipelineRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(ctx, pipelineRollout, err, "ReconcileFailed", "Failed to reconcile PipelineRollout")
		statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(ctx, pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// if there's any metadata to add to the pipelines as annotations, do it here
	err = r.annotatePipelines(ctx, pipelineRollout)
	if err != nil {
		return ctrl.Result{}, err
	}

	if existingPipelineDef != nil {

		// Update PipelineRollout Status based on child resource (Pipeline) Status
		err = r.processPipelineStatus(ctx, pipelineRollout, existingPipelineDef)
		if err != nil {
			r.ErrorHandler(ctx, pipelineRollout, err, "ProcessPipelineStatusFailed", "Failed to process Pipeline Status")
			statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(ctx, pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
				return ctrl.Result{}, statusUpdateErr
			}

			return ctrl.Result{}, err
		}
	}

	// Update the resource definition (everything except the Status subresource)
	if r.needsUpdate(pipelineRolloutOrig, pipelineRollout) {
		if err := r.client.Update(ctx, pipelineRollout); err != nil {
			r.ErrorHandler(ctx, pipelineRollout, err, "UpdateFailed", "Failed to update PipelineRollout")
			statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(ctx, pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
	}

	// Update the Status subresource
	if pipelineRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updatePipelineRolloutStatus(ctx, pipelineRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(ctx, pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate the metrics for the Pipeline.
	r.customMetrics.IncPipelineROsRunning(pipelineRollout.Name, pipelineRollout.Namespace)

	r.recorder.Eventf(pipelineRollout, "Normal", "ReconcileSuccess", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

	if requeueDelay > 0 {
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	return ctrl.Result{}, nil
}

func (r *PipelineRolloutReconciler) Shutdown(ctx context.Context) {
	numaLogger := logger.FromContext(ctx)

	numaLogger.Info("shutting down PipelineRollout queue")
	r.Queue.ShutDown()

	// wait for all the workers to have stopped
	r.shutdownWorkerWaitGroup.Wait()
}

// runWorkers starts up the workers processing the queue of PipelineRollouts
func (r *PipelineRolloutReconciler) runWorkers(ctx context.Context) {

	for i := 0; i < numWorkers; i++ {
		r.shutdownWorkerWaitGroup.Add(numWorkers)
		go r.runWorker(ctx)
	}
}

// runWorker starts up one of the workers processing the queue of PipelineRollouts
func (r *PipelineRolloutReconciler) runWorker(ctx context.Context) {
	numaLogger := logger.FromContext(ctx)

	for {
		key, quit := r.Queue.Get()
		if quit {
			numaLogger.Info("PipelineRollout worker done")
			r.shutdownWorkerWaitGroup.Done()
			return
		}
		r.processQueueKey(ctx, key.(string))
		r.Queue.Done(key)
	}

}

func (r *PipelineRolloutReconciler) processQueueKey(ctx context.Context, key string) {
	numaLogger := logger.FromContext(ctx).WithValues("key", key)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)

	// get namespace/name from key
	namespacedName, err := keyToNamespacedName(key)
	if err != nil {
		numaLogger.Fatal(err, "Queue key not derivable")
	}

	numaLogger.Debugf("processing PipelineRollout %v", namespacedName)
	result, err := r.processPipelineRollout(ctx, namespacedName)

	// based on result, may need to add this back to the queue
	if err != nil {
		numaLogger.Errorf(err, "PipelineRollout %v reconcile returned error: %v", namespacedName, err)
		r.Queue.AddRateLimited(key)
	} else {
		if result.Requeue {
			numaLogger.Debugf("PipelineRollout %v reconcile requests requeue", namespacedName)
			r.Queue.AddRateLimited(key)
		} else if result.RequeueAfter > 0 {
			numaLogger.Debugf("PipelineRollout %v reconcile requests requeue after %.0f seconds", namespacedName, result.RequeueAfter.Seconds())
			r.Queue.AddAfter(key, result.RequeueAfter)
		} else {
			numaLogger.Debugf("PipelineRollout %v reconcile complete", namespacedName)
		}
	}
}

func keyToNamespacedName(key string) (k8stypes.NamespacedName, error) {
	index := strings.Index(key, "/")
	if index < 0 {
		return k8stypes.NamespacedName{}, fmt.Errorf("improperly formatted key: %q", key)
	}
	return k8stypes.NamespacedName{Namespace: key[0:index], Name: key[index+1:]}, nil
}

func namespacedNameToKey(namespacedName k8stypes.NamespacedName) string {
	return fmt.Sprintf("%s/%s", namespacedName.Namespace, namespacedName.Name)
}

// reconcile does the real logic, it returns a requeue delay if the event
// needs to be re-queued.
func (r *PipelineRolloutReconciler) reconcile(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
	syncStartTime time.Time,
) (time.Duration, *unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)
	defer func() {
		// Set the health of the pipelineRollout only if it is not being deleted.
		if pipelineRollout.DeletionTimestamp.IsZero() {
			numaLogger.Debugf("Reconcilation finished for pipelineRollout %s/%s, setting phase metrics: %s", pipelineRollout.Namespace, pipelineRollout.Name, pipelineRollout.Status.Phase)
			r.customMetrics.SetPipelineRolloutHealth(pipelineRollout.Namespace, pipelineRollout.Name, string(pipelineRollout.Status.Phase))
		}
	}()

	// is PipelineRollout being deleted? need to remove the finalizer, so it can
	// (OwnerReference will delete the underlying Pipeline through Cascading deletion)
	if !pipelineRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting PipelineRollout")
		if controllerutil.ContainsFinalizer(pipelineRollout, common.FinalizerName) {
			// delete the PipelineRollout child objects once the PipelineRollout is being deleted
			requeue, err := r.listAndDeleteChildPipelines(ctx, pipelineRollout)
			if err != nil {
				return 0, nil, fmt.Errorf("error deleting pipelineRollout child: %v", err)
			}
			// if we have any pipelines that are still in the process of being deleted, requeue
			if requeue {
				return 5 * time.Second, nil, nil
			}
			numaLogger.Info("Removing Finalizer from PipelineRollout")
			controllerutil.RemoveFinalizer(pipelineRollout, common.FinalizerName)
		}
		// generate the metrics for the Pipeline deletion.
		r.customMetrics.DecPipelineROsRunning(pipelineRollout.Name, pipelineRollout.Namespace)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "delete").Observe(time.Since(syncStartTime).Seconds())
		r.customMetrics.DeletePipelineRolloutHealth(pipelineRollout.Namespace, pipelineRollout.Name)
		return 0, nil, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(pipelineRollout, common.FinalizerName) {
		controllerutil.AddFinalizer(pipelineRollout, common.FinalizerName)
	}

	// check if there's a promoted pipeline yet
	promotedPipelines, err := ctlrcommon.FindChildrenOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradePromoted, nil, false, r.client)
	if err != nil {
		return 0, nil, fmt.Errorf("error looking for promoted pipeline: %v", err)
	}
	newPipelineDef, err := r.makeTargetPipelineDefinition(ctx, pipelineRollout)
	if err != nil {
		return 0, nil, err
	}

	requeueDelay := time.Duration(0)

	var existingPipelineDef *unstructured.Unstructured

	if newPipelineDef == nil {
		// we couldn't create the Pipeline definition: we need to check again later
		requeueDelay = common.DefaultRequeueDelay
	} else {

		if len(promotedPipelines.Items) == 0 {

			numaLogger.Debugf("Pipeline %s/%s doesn't exist so creating", pipelineRollout.Namespace, newPipelineDef.GetName())
			pipelineRollout.Status.MarkPending()

			if err = r.createPromotedPipeline(ctx, pipelineRollout, newPipelineDef); err != nil {
				return 0, nil, err
			}

			pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
			r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "create").Observe(time.Since(syncStartTime).Seconds())

		} else {

			// "promoted" object already exists
			existingPipelineDef, err = kubernetes.GetResource(ctx, r.client, newPipelineDef.GroupVersionKind(),
				k8stypes.NamespacedName{Name: newPipelineDef.GetName(), Namespace: newPipelineDef.GetNamespace()})
			if err != nil {
				return 0, existingPipelineDef, fmt.Errorf("error getting Pipeline: %v", err)
			}

			// if Pipeline is not owned by Rollout, fail and return
			if !checkOwnerRef(existingPipelineDef.GetOwnerReferences(), pipelineRollout.UID) {
				errStr := fmt.Sprintf("Pipeline %s already exists in namespace, not owned by a PipelineRollout", existingPipelineDef.GetName())
				numaLogger.Debugf("PipelineRollout %s failed because %s", pipelineRollout.Name, errStr)
				return 0, existingPipelineDef, errors.New(errStr)
			}

			newPipelineDefResult, err := r.merge(existingPipelineDef, newPipelineDef)
			if err != nil {
				return 0, existingPipelineDef, err
			}

			requeueDelay, err = r.processExistingPipeline(ctx, pipelineRollout, existingPipelineDef, newPipelineDefResult, syncStartTime)
			if err != nil {
				return 0, existingPipelineDef, err
			}
		}
	}

	// get current in progress strategy if there is one
	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, pipelineRollout)
	inProgressStrategySet := inProgressStrategy != apiv1.UpgradeStrategyNoOp

	// clean up recyclable pipelines
	allDeleted, err := r.garbageCollectChildren(ctx, pipelineRollout)
	if err != nil {
		return 0, nil, err
	}
	// there are some cases that require re-queueing
	if !allDeleted || inProgressStrategySet {
		if requeueDelay == 0 {
			requeueDelay = common.DefaultRequeueDelay
		} else {
			requeueDelay = min(requeueDelay, common.DefaultRequeueDelay)
		}
	}

	return requeueDelay, existingPipelineDef, err
}

// determine if this Pipeline is owned by this PipelineRollout
func checkOwnerRef(ownerRefs []metav1.OwnerReference, uid k8stypes.UID) bool {
	// no owners
	if len(ownerRefs) == 0 {
		return false
	}
	for _, ref := range ownerRefs {
		if ref.Kind == "PipelineRollout" && ref.UID == uid {
			return true
		}
	}
	return false
}

// take the existing pipeline and merge anything needed from the new pipeline definition
func (r *PipelineRolloutReconciler) merge(existingPipeline, newPipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	resultPipeline := existingPipeline.DeepCopy()

	var specAsMap map[string]interface{}
	if err := util.StructToStruct(newPipeline.Object["spec"], &specAsMap); err != nil {
		return resultPipeline, fmt.Errorf("failed to get spec from new Pipeline: %w", err)
	}
	resultPipeline.Object["spec"] = specAsMap
	resultPipeline.SetAnnotations(util.MergeMaps(existingPipeline.GetAnnotations(), newPipeline.GetAnnotations()))
	resultPipeline.SetLabels(util.MergeMaps(existingPipeline.GetLabels(), newPipeline.GetLabels()))

	return resultPipeline, nil
}

// return a requeue delay greater than 0 if requeue is needed, and return error if any (if returning an error, we will requeue anyway)
func (r *PipelineRolloutReconciler) processExistingPipeline(ctx context.Context, pipelineRollout *apiv1.PipelineRollout,
	existingPipelineDef, newPipelineDef *unstructured.Unstructured, syncStartTime time.Time) (time.Duration, error) {

	numaLogger := logger.FromContext(ctx)

	// get the list of Riders that we need based on the PipelineRollout definition
	currentRiderList, err := r.GetDesiredRiders(pipelineRollout, existingPipelineDef.GetName(), newPipelineDef)
	if err != nil {
		return 0, fmt.Errorf("error getting desired Riders for pipeline %s: %s", existingPipelineDef.GetName(), err)
	}
	// get the list of Riders that we have now (for promoted child)
	existingRiderList, err := r.GetExistingRiders(ctx, pipelineRollout, false)
	if err != nil {
		return 0, fmt.Errorf("error getting existing Riders for pipeline %s: %s", existingPipelineDef.GetName(), err)
	}

	// what is the preferred strategy for this namespace?
	userPreferredStrategy, err := usde.GetUserStrategy(ctx, newPipelineDef.GetNamespace(), existingPipelineDef.GetKind())
	if err != nil {
		return 0, err
	}

	// does the Resource need updating, and if so how?
	// TODO: handle recreate parameter
	needsUpdate, upgradeStrategyType, _, riderAdditions, riderModifications, riderDeletions, err := usde.ResourceNeedsUpdating(ctx, newPipelineDef, existingPipelineDef, currentRiderList, existingRiderList)
	if err != nil {
		return 0, err
	}

	numaLogger.
		WithValues("needsUpdate", needsUpdate, "upgradeStrategyType", upgradeStrategyType).
		Debug("Upgrade decision result")

	// set the Status appropriately to "Pending" or "Deployed" depending on whether rollout needs to update
	if needsUpdate {
		pipelineRollout.Status.MarkPending()
	} else {
		pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
	}

	// is there currently an inProgressStrategy for the pipeline? (This will override any new decision)
	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, pipelineRollout)
	numaLogger.Debugf("current inProgressStrategy=%s", inProgressStrategy)
	inProgressStrategySet := (inProgressStrategy != apiv1.UpgradeStrategyNoOp)

	// if not, should we set one?
	if !inProgressStrategySet {
		if userPreferredStrategy == config.PPNDStrategyID {
			// if the preferred strategy is PPND, do we need to start the process for PPND (if we haven't already)?
			needPPND := false
			ppndRequired, err := r.needPPND(ctx, pipelineRollout, upgradeStrategyType == apiv1.UpgradeStrategyPPND)
			if err != nil {
				return 0, err
			}
			if ppndRequired == nil { // not enough information
				// TODO: mark something in the Status for why we're remaining in "Pending" here
				return common.DefaultRequeueDelay, nil
			}
			needPPND = *ppndRequired
			if needPPND {
				inProgressStrategy = apiv1.UpgradeStrategyPPND
				r.inProgressStrategyMgr.SetStrategy(ctx, pipelineRollout, inProgressStrategy)
			}
		}
		if userPreferredStrategy == config.ProgressiveStrategyID {
			if upgradeStrategyType == apiv1.UpgradeStrategyProgressive {
				inProgressStrategy = apiv1.UpgradeStrategyProgressive
				r.inProgressStrategyMgr.SetStrategy(ctx, pipelineRollout, inProgressStrategy)
			}
		}
	}

	// don't risk out-of-date cache while performing PPND or Progressive strategy - get
	// the most current version of the Pipeline just in case
	if inProgressStrategy != apiv1.UpgradeStrategyNoOp {
		existingPipelineDef, err = kubernetes.GetLiveResource(ctx, newPipelineDef, "pipelines")
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.WithValues("pipelineDefinition", *newPipelineDef).Warn("Pipeline not found.")
				return 0, nil
			} else {
				return 0, fmt.Errorf("error getting Pipeline for status processing: %v", err)
			}
		}
		newPipelineDef, err = r.merge(existingPipelineDef, newPipelineDef)
		if err != nil {
			return 0, err
		}
	}

	requeueDelay := time.Duration(0) // 0 means "no requeue"

	// now do whatever the inProgressStrategy is
	switch inProgressStrategy {
	case apiv1.UpgradeStrategyPPND:
		numaLogger.Debug("processing pipeline with PPND")
		done, err := r.processExistingPipelineWithPPND(ctx, pipelineRollout, existingPipelineDef, newPipelineDef)
		if err != nil {
			return 0, err
		}
		if done {
			r.inProgressStrategyMgr.UnsetStrategy(ctx, pipelineRollout)
		}
		// update the cluster to reflect the Rider additions, modifications, and deletions
		if err := riders.UpdateRidersInK8S(ctx, newPipelineDef, riderAdditions, riderModifications, riderDeletions, r.client); err != nil {
			return 0, err
		}

		// update the list of riders in the Status
		r.SetCurrentRiderList(ctx, pipelineRollout, currentRiderList)

	case apiv1.UpgradeStrategyProgressive:
		numaLogger.Debug("processing pipeline with Progressive")

		done, progressiveRequeueDelay, err := progressive.ProcessResource(ctx, pipelineRollout, existingPipelineDef, needsUpdate, r, r.client)
		if err != nil {
			return 0, err
		}
		if done {
			// update the list of riders in the Status based on our child which was just promoted
			promotedPipeline, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradePromoted, nil, true, r.client)
			if err != nil {
				return 0, err
			}
			currentRiderList, err := r.GetDesiredRiders(pipelineRollout, promotedPipeline.GetName(), promotedPipeline)
			if err != nil {
				return 0, fmt.Errorf("error getting desired Riders for pipeline %s: %s", newPipelineDef.GetName(), err)
			}
			r.SetCurrentRiderList(ctx, pipelineRollout, currentRiderList)

			pipelineRollout.Status.ProgressiveStatus.PromotedPipelineStatus = nil

			// we need to prevent the possibility that we're done but we fail to update the Progressive Status
			// therefore, we publish Rollout.Status here, so if that fails, then we won't be "done" and so we'll come back in here to try again
			err = r.updatePipelineRolloutStatus(ctx, pipelineRollout)
			if err != nil {
				return 0, err
			}
			r.inProgressStrategyMgr.UnsetStrategy(ctx, pipelineRollout)
		} else {
			requeueDelay = progressiveRequeueDelay
		}

		if pipelineRollout.GetUpgradingChildStatus() != nil {
			r.customMetrics.IncPipelineProgressiveResults(pipelineRollout.GetRolloutObjectMeta().GetNamespace(), pipelineRollout.GetRolloutObjectMeta().GetName(),
				pipelineRollout.GetUpgradingChildStatus().Name, string(pipelineRollout.GetUpgradingChildStatus().BasicAssessmentResult),
				progressive.EvaluateSuccessStatusForMetrics(pipelineRollout.GetUpgradingChildStatus().AssessmentResult), pipelineRollout.GetUpgradingChildStatus().ForcedSuccess, true)
		}

	default:
		if needsUpdate && upgradeStrategyType == apiv1.UpgradeStrategyApply {
			if err := updatePipelineSpec(ctx, r.client, pipelineRollout, newPipelineDef, existingPipelineDef); err != nil {
				return 0, err
			}
			pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)

			// update the cluster to reflect the Rider additions, modifications, and deletions
			if err := riders.UpdateRidersInK8S(ctx, newPipelineDef, riderAdditions, riderModifications, riderDeletions, r.client); err != nil {
				return 0, err
			}

			// update the list of riders in the Status
			r.SetCurrentRiderList(ctx, pipelineRollout, currentRiderList)
		}

	}

	if needsUpdate {
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "update").Observe(time.Since(syncStartTime).Seconds())
	}

	return requeueDelay, nil
}

// Create the Promoted Pipeline (as well as any Riders)
func (r *PipelineRolloutReconciler) createPromotedPipeline(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, newPipelineDef *unstructured.Unstructured) error {

	// first need to know if the pipeline needs to be created with "desiredPhase" = "Paused" or not
	// (i.e. if isbsvc or numaflow controller is requesting pause)
	// this can happen during delete/recreate of pipeline
	userPreferredStrategy, err := usde.GetUserStrategy(ctx, newPipelineDef.GetNamespace(), numaflowv1.PipelineGroupVersionKind.Kind)
	if err != nil {
		return err
	}
	if userPreferredStrategy == config.PPNDStrategyID {
		needsPaused, _, err := r.shouldBePaused(ctx, pipelineRollout, nil, newPipelineDef, false)
		if err != nil {
			return err
		}
		if needsPaused != nil && *needsPaused {
			err = numaflowtypes.PipelineWithDesiredPhase(newPipelineDef, "Paused")
			if err != nil {
				return err
			}
		}

	}

	// Create the Pipeline
	err = kubernetes.CreateResource(ctx, r.client, newPipelineDef)
	if err != nil {
		return err
	}
	// Create any Riders if they exist
	if err := ctlrcommon.CreateRidersForNewChild(ctx, r, pipelineRollout, newPipelineDef, r.client); err != nil {
		return fmt.Errorf("error creating riders: %s", err)
	}

	// if user somehow has no Promoted Pipeline and is in the middle of Progressive, this isn't right - user may have deleted the Promoted Pipeline
	// if this happens, we need to stop the Progressive upgrade and remove any Upgrading children
	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, pipelineRollout)

	if inProgressStrategy == apiv1.UpgradeStrategyProgressive {
		r.inProgressStrategyMgr.UnsetStrategy(ctx, pipelineRollout)
		if err = progressive.Discontinue(ctx, pipelineRollout, r, r.client); err != nil {
			return err
		}
	}
	return nil
}

func pipelineObservedGenerationCurrent(generation int64, observedGeneration int64) bool {
	return generation <= observedGeneration
}

// Set the Condition in the Status for child resource health

func (r *PipelineRolloutReconciler) processPipelineStatus(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, existingPipelineDef *unstructured.Unstructured) error {
	numaLogger := logger.FromContext(ctx)

	if existingPipelineDef != nil {
		pipelineStatus, err := kubernetes.ParseStatus(existingPipelineDef)
		if err != nil {
			return fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", existingPipelineDef, err)
		}

		numaLogger.Debugf("pipeline status: %v", pipelineStatus)

		r.setChildResourcesHealthCondition(pipelineRollout, existingPipelineDef, &pipelineStatus)
		r.setChildResourcesPauseCondition(pipelineRollout, &pipelineStatus)
	}
	return nil
}

func (r *PipelineRolloutReconciler) setChildResourcesHealthCondition(pipelineRollout *apiv1.PipelineRollout, pipeline *unstructured.Unstructured, pipelineStatus *kubernetes.GenericStatus) {

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)
	pipelineChildResourceStatus, pipelineChildResourceReason := getPipelineChildResourceHealth(pipelineStatus.Conditions)

	if pipelineChildResourceReason == "Progressing" || !pipelineObservedGenerationCurrent(pipeline.GetGeneration(), pipelineStatus.ObservedGeneration) {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("Progressing", "Pipeline Progressing", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhaseFailed {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Phase=Failed", pipelineRollout.Generation)
	} else if pipelineChildResourceStatus == "False" {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineFailed", "Pipeline Failed, Pipeline Child Resource(s) Unhealthy", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhasePaused || pipelinePhase == numaflowv1.PipelinePhasePausing {
		pipelineRollout.Status.MarkChildResourcesHealthUnknown("PipelineUnknown", "Pipeline Pausing - health unknown", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhaseDeleting {
		pipelineRollout.Status.MarkChildResourcesUnhealthy("PipelineDeleting", "Pipeline Deleting", pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhaseUnknown || pipelineChildResourceStatus == "Unknown" {
		pipelineRollout.Status.MarkChildResourcesHealthUnknown("PipelineUnknown", "Pipeline Phase Unknown", pipelineRollout.Generation)
	} else {
		pipelineRollout.Status.MarkChildResourcesHealthy(pipelineRollout.Generation)
	}

}

func (r *PipelineRolloutReconciler) setChildResourcesPauseCondition(pipelineRollout *apiv1.PipelineRollout, pipelineStatus *kubernetes.GenericStatus) {

	pipelinePhase := numaflowv1.PipelinePhase(pipelineStatus.Phase)

	if pipelinePhase == numaflowv1.PipelinePhasePausing {
		// if BeginTime hasn't been set yet, we must have just started pausing - set it
		if pipelineRollout.Status.PauseStatus.LastPauseBeginTime == metav1.NewTime(initTime) || !pipelineRollout.Status.PauseStatus.LastPauseBeginTime.After(pipelineRollout.Status.PauseStatus.LastPauseEndTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseBeginTime = metav1.NewTime(time.Now())
		}
		reason := fmt.Sprintf("Pipeline%s", string(pipelinePhase))
		msg := fmt.Sprintf("Pipeline %s", strings.ToLower(string(pipelinePhase)))
		// if TransitionTime is before or equal to BeginTime, update Pausing metric
		// if it's not, then something is wrong and we don't want to make a bad calculation
		if !pipelineRollout.Status.PauseStatus.LastPauseTransitionTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			r.updatePauseMetric(pipelineRollout, pipelinePhase)
		}
		pipelineRollout.Status.MarkPipelinePausingOrPaused(reason, msg, pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhasePaused {
		// if LastPauseTransitionTime hasn't been set yet, we are just starting to enter paused phase - set it to denote end of pausing
		if pipelineRollout.Status.PauseStatus.LastPauseTransitionTime == metav1.NewTime(initTime) || !pipelineRollout.Status.PauseStatus.LastPauseTransitionTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseTransitionTime = metav1.NewTime(time.Now())
		}
		reason := fmt.Sprintf("Pipeline%s", string(pipelinePhase))
		msg := fmt.Sprintf("Pipeline %s", strings.ToLower(string(pipelinePhase)))
		// if EndTime is before or equal to BeginTime, update Paused metric
		// if it's not, then something is wrong, and we don't want to make a bad calculation
		if !pipelineRollout.Status.PauseStatus.LastPauseEndTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			r.updatePauseMetric(pipelineRollout, pipelinePhase)
		}
		pipelineRollout.Status.MarkPipelinePausingOrPaused(reason, msg, pipelineRollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		if (pipelineRollout.Status.PauseStatus.LastPauseBeginTime != metav1.NewTime(initTime)) && !pipelineRollout.Status.PauseStatus.LastPauseEndTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseEndTime = metav1.NewTime(time.Now())
		}
		// if phase is not Paused or Pausing, we always reset the metrics to 0 without fail
		r.updatePauseMetric(pipelineRollout, pipelinePhase)
		pipelineRollout.Status.MarkPipelineUnpaused(pipelineRollout.Generation)
	}

}

func (r *PipelineRolloutReconciler) updatePauseMetric(pipelineRollout *apiv1.PipelineRollout, pipelinePhase numaflowv1.PipelinePhase) {

	var pipelineSpec numaflowtypes.PipelineSpec
	_ = json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec)

	// if pause is manual, set metrics back to 0
	if r.isSpecBasedPause(pipelineSpec) {
		r.setPauseMetrics(pipelineRollout.Namespace, pipelineRollout.Name, float64(0), float64(0))
	} else if pipelinePhase == numaflowv1.PipelinePhasePaused {
		// if pipeline is Paused, set Paused metric to time duration since last transition time
		r.setPauseMetrics(pipelineRollout.Namespace, pipelineRollout.Name, time.Since(pipelineRollout.Status.PauseStatus.LastPauseTransitionTime.Time).Seconds(), float64(0))
	} else if pipelinePhase == numaflowv1.PipelinePhasePausing {
		// if pipeline is Pausing, set Pausing metric to time duration since last pause begin time
		r.setPauseMetrics(pipelineRollout.Namespace, pipelineRollout.Name, float64(0), time.Since(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time).Seconds())
	} else {
		// otherwise set both metrics back to 0
		r.setPauseMetrics(pipelineRollout.Namespace, pipelineRollout.Name, float64(0), float64(0))
	}

}

func (r *PipelineRolloutReconciler) setPauseMetrics(namespace, name string, pausedVal, pausingVal float64) {
	r.customMetrics.PipelinePausedSeconds.WithLabelValues(namespace, name).Set(pausedVal)
	r.customMetrics.PipelinePausingSeconds.WithLabelValues(namespace, name).Set(pausingVal)
}

// where needed, add annotations to Pipelines
func (r *PipelineRolloutReconciler) annotatePipelines(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	pipelines, err := kubernetes.ListResources(ctx, r.client, pipelineRollout.GetChildGVK(), pipelineRollout.GetRolloutObjectMeta().GetNamespace())
	if err != nil {
		return err
	}
	for _, pipeline := range pipelines.Items {
		err = r.annotatePipeline(ctx, &pipeline)
		if err != nil {
			return err
		}
	}
	return nil
}

// where needed, add annotations to Pipeline
func (r *PipelineRolloutReconciler) annotatePipeline(ctx context.Context, pipeline *unstructured.Unstructured) error {
	pipelineCanIngestData, err := numaflowtypes.CanPipelineIngestData(ctx, pipeline)
	if err != nil {
		return err
	}

	if pipelineCanIngestData && (pipeline.GetAnnotations() == nil || pipeline.GetAnnotations()[common.AnnotationKeyRequiresDrain] != "true") {
		// Patch the live pipeline to mark that this pipeline requires drain
		patchJson := fmt.Sprintf(`{"metadata": {"annotations": {"%s": "true"}}}`, common.AnnotationKeyRequiresDrain)
		err = kubernetes.PatchResource(ctx, r.client, pipeline, patchJson, k8stypes.MergePatchType)
		if err != nil {
			return fmt.Errorf("failed to patch pipeline annotation %s: %w", common.AnnotationKeyRequiresDrain, err)
		}
	}
	return nil
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
func (r *PipelineRolloutReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {

	numaLogger := logger.FromContext(ctx)
	controller, err := runtimecontroller.New(ControllerPipelineRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch PipelineRollouts
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.PipelineRollout{},
		&handler.TypedEnqueueRequestForObject[*apiv1.PipelineRollout]{}, ctlrcommon.TypedGenerationChangedPredicate[*apiv1.PipelineRollout]{})); err != nil {
		return fmt.Errorf("failed to watch PipelineRollouts: %v", err)
	}

	// Watch Pipelines
	pipelineUns := &unstructured.Unstructured{}
	pipelineUns.SetGroupVersionKind(schema.GroupVersionKind{
		Kind:    common.NumaflowPipelineKind,
		Group:   common.NumaflowAPIGroup,
		Version: common.NumaflowAPIVersion,
	})
	if err := controller.Watch(source.Kind(mgr.GetCache(), pipelineUns,
		handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.PipelineRollout{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{})); err != nil {
		return fmt.Errorf("failed to watch Pipelines: %v", err)
	}

	// Watch AnalysisRuns that are owned by the Pipelines that PipelineRollout owns (this enqueues the PipelineRollout)
	if err := controller.Watch(
		source.Kind(mgr.GetCache(), &argorolloutsv1.AnalysisRun{},
			handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, analysisRun *argorolloutsv1.AnalysisRun) []reconcile.Request {

				var reqs []reconcile.Request

				// Check if Pipeline is the owner
				for _, analysisRunOwner := range analysisRun.GetOwnerReferences() {
					// Check if the owner is of Kind 'Pipeline' and is marked as "Controller"
					if analysisRunOwner.Kind == "Pipeline" && *analysisRunOwner.Controller {

						// find the pipeline so we can enqueue the PipelineRollout which owns it (if one does)
						pipeline, err := kubernetes.GetResource(ctx, r.client, numaflowv1.PipelineGroupVersionKind,
							k8stypes.NamespacedName{Namespace: analysisRun.GetNamespace(), Name: analysisRunOwner.Name})
						if err != nil {
							numaLogger.WithValues(
								"AnalysisRun", fmt.Sprintf("%s:%s", analysisRun.Namespace, analysisRun.Name),
								"Pipeline", fmt.Sprintf("%s:%s", analysisRun.Namespace, analysisRunOwner.Name)).Warnf("Unable to get Pipeline owner of AnalysisRun")
							continue
						}

						// See if a PipelineRollout owns the Pipeline: if so, enqueue it
						for _, pipelineOwner := range pipeline.GetOwnerReferences() {
							if pipelineOwner.Kind == "PipelineRollout" && *pipelineOwner.Controller {

								reqs = append(reqs, reconcile.Request{
									NamespacedName: types.NamespacedName{
										Name:      pipelineOwner.Name,
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

func updatePipelineSpec(
	ctx context.Context,
	c client.Client,
	pipelineRollout *apiv1.PipelineRollout,
	newPipelineDef *unstructured.Unstructured,
	existingPipelineDef *unstructured.Unstructured) error {

	err := performCustomPipelineMods(ctx, c, pipelineRollout, newPipelineDef, existingPipelineDef)
	if err != nil {
		return err
	}

	return kubernetes.UpdateResource(ctx, c, newPipelineDef)
}

func performCustomPipelineMods(
	ctx context.Context,
	c client.Client,
	pipelineRollout *apiv1.PipelineRollout,
	newPipelineDef *unstructured.Unstructured,
	existingPipelineDef *unstructured.Unstructured) error {

	return performCustomResumeMod(ctx, c, pipelineRollout, newPipelineDef, existingPipelineDef)
}

// performCustomResumeMod checks if pipeline's desiredPhase is going from Paused to Running:
// if their strategy says to resume gradually, we need to reset the Vertices' "replicas" value back to nil (i.e. min)
func performCustomResumeMod(
	ctx context.Context,
	c client.Client,
	pipelineRollout *apiv1.PipelineRollout,
	newPipelineDef *unstructured.Unstructured,
	existingPipelineDef *unstructured.Unstructured) error {

	numaLogger := logger.FromContext(ctx).WithValues("pipeline", fmt.Sprintf("%s/%s", newPipelineDef.GetNamespace(), newPipelineDef.GetName()))

	// does user prefer to resume gradually? (note this is the default)
	if pipelineRollout.Spec.Strategy == nil || !pipelineRollout.Spec.Strategy.PauseResumeStrategy.FastResume {
		// if we're in the middle of going from Paused to Running, we need to set vertices' 'replicas' count to nil
		// since user prefers "slow resume": this will cause replicas to reset to "min" and scale up gradually
		desiredPhase, err := numaflowtypes.GetPipelineDesiredPhase(newPipelineDef)
		if err != nil {
			return err
		}
		pausingOrPaused := numaflowtypes.CheckPipelinePhase(ctx, existingPipelineDef, numaflowv1.PipelinePhasePausing) ||
			numaflowtypes.CheckPipelinePhase(ctx, existingPipelineDef, numaflowv1.PipelinePhasePaused)

		if desiredPhase == string(numaflowv1.PipelinePhaseRunning) && pausingOrPaused {
			numaLogger.Debug("resuming Pipeline slow: setting replicas=nil for each Vertex")
			return numaflowtypes.MinimizePipelineVertexReplicas(ctx, c, existingPipelineDef)
		}
		return nil
	}
	return nil
}

// take the Metadata (Labels and Annotations) specified in the PipelineRollout plus any others that apply to all Pipelines
func getBasePipelineMetadata(pipelineRollout *apiv1.PipelineRollout) (apiv1.Metadata, error) {
	labelMapping := map[string]string{}
	for key, val := range pipelineRollout.Spec.Pipeline.Labels {
		labelMapping[key] = val
	}
	var pipelineSpec numaflowtypes.PipelineSpec

	if err := json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec); err != nil {
		return apiv1.Metadata{}, fmt.Errorf("failed to unmarshal pipeline spec: %v", err)
	}

	labelMapping[common.LabelKeyISBServiceRONameForPipeline] = pipelineSpec.GetISBSvcName()
	labelMapping[common.LabelKeyParentRollout] = pipelineRollout.Name

	return apiv1.Metadata{Labels: labelMapping, Annotations: pipelineRollout.Spec.Pipeline.Annotations}, nil

}

func (r *PipelineRolloutReconciler) updatePipelineRolloutStatus(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) error {
	numaLogger := logger.FromContext(ctx)

	err := r.client.Status().Update(ctx, pipelineRollout)

	if err != nil && apierrors.IsConflict(err) {
		// there was a Resource Version conflict error (i.e. an update was made to PipelineRollout after the version we retrieved), so retry using the latest Resource Version: get the PipelineRollout live resource
		// and attach our Status to it.
		// The reason this is okay is because we are the only ones who write the Status, and because we retrieved the live version of this ISBServiceRollout at the beginning of the reconciliation
		// Therefore, we know that the Status is totally current.
		livePipelineRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().PipelineRollouts(pipelineRollout.Namespace).Get(ctx, pipelineRollout.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.Infof("PipelineRollout not found, %v", err)
				return nil
			}
			return fmt.Errorf("error getting the live PipelineRollout after attempting to update the PipelineRollout Status: %w", err)
		}
		status := pipelineRollout.Status // save off the Status
		*pipelineRollout = *livePipelineRollout
		numaLogger.Debug("resource version conflict error after getting latest PipelineRollout Status: try again with latest resource version")
		pipelineRollout.Status = status
		err = r.client.Status().Update(ctx, pipelineRollout)
		if err != nil {
			return fmt.Errorf("consecutive errors attempting to update PipelineRolloutStatus: %w", err)
		}
		return nil
	}
	return err
}

func (r *PipelineRolloutReconciler) updatePipelineRolloutStatusToFailed(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, err error) error {
	pipelineRollout.Status.MarkFailed(err.Error())
	return r.updatePipelineRolloutStatus(ctx, pipelineRollout)
}

// if we did, we can create the pipeline definition and return it
func (r *PipelineRolloutReconciler) makeTargetPipelineDefinition(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) (*unstructured.Unstructured, error) {
	numaLogger := logger.FromContext(ctx)

	// which InterstepBufferServiceName should we use?
	// If there is an upgrading isbsvc, use that
	// Otherwise, use the promoted one
	// TODO: consider case that there's an "upgrading" isbsvc, but the preferred strategy has just changed to something
	// other than progressive - we may need isbsvc's "in-progress-strategy" to inform pipeline's strategy
	isbsvc, err := r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradeInProgress)
	if err != nil {
		return nil, err
	}
	if isbsvc == nil {
		numaLogger.Debug("no Upgrading isbsvc found for Pipeline, will find promoted one")
		isbsvc, err = r.getISBSvc(ctx, pipelineRollout, common.LabelValueUpgradePromoted)
		if err != nil {
			return nil, fmt.Errorf("failed to find isbsvc that's 'promoted': won't be able to reconcile PipelineRollout, err=%v", err)
		}
		if isbsvc == nil {
			numaLogger.Debug("no Upgrading or Promoted isbsvc found for Pipeline")
			return nil, nil
		}
	}

	metadata, err := getBasePipelineMetadata(pipelineRollout)
	if err != nil {
		return nil, err
	}
	metadata.Labels[common.LabelKeyUpgradeState] = string(common.LabelValueUpgradePromoted)
	metadata.Labels[common.LabelKeyISBServiceChildNameForPipeline] = isbsvc.GetName()

	// determine name of the Pipeline
	pipelineName, err := ctlrcommon.GetChildName(ctx, pipelineRollout, r, common.LabelValueUpgradePromoted, nil, r.client, true)
	if err != nil {
		return nil, err
	}

	pipelineDef, err := r.makePipelineDefinition(pipelineRollout, pipelineName, isbsvc.GetName(), metadata)
	return pipelineDef, err
}

func (r *PipelineRolloutReconciler) getISBSvcRollout(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) (*apiv1.ISBServiceRollout, error) {
	// get the ISBServiceRollout name from the PipelineRollout's Pipeline spec
	var pipelineSpec numaflowtypes.PipelineSpec
	if err := json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pipeline spec: %v", err)
	}
	isbsvcRolloutName := pipelineSpec.GetISBSvcName()

	isbServiceRollout := &apiv1.ISBServiceRollout{}
	err := r.client.Get(ctx, k8stypes.NamespacedName{Namespace: pipelineRollout.GetNamespace(), Name: isbsvcRolloutName}, isbServiceRollout)
	return isbServiceRollout, err
}

// templates are used to dynamically evaluate child spec, metadata, as well as Riders
func (r *PipelineRolloutReconciler) GetTemplateArguments(pipeline *unstructured.Unstructured) map[string]interface{} {
	return r.getTemplateArguments(pipeline.GetName(), pipeline.GetNamespace())
}

func (r *PipelineRolloutReconciler) getTemplateArguments(pipelineName string, namespace string) map[string]interface{} {
	return map[string]interface{}{
		common.TemplatePipelineName:      pipelineName,
		common.TemplatePipelineNamespace: namespace,
	}
}

func (r *PipelineRolloutReconciler) makePipelineDefinition(
	pipelineRollout *apiv1.PipelineRollout,
	pipelineName string,
	isbsvcName string,
	metadata apiv1.Metadata,
) (*unstructured.Unstructured, error) {

	args := r.getTemplateArguments(pipelineName, pipelineRollout.Namespace)

	pipelineSpec, err := util.ResolveTemplatedSpec(pipelineRollout.Spec.Pipeline.Spec, args)
	if err != nil {
		return nil, err
	}

	metadataResolved, err := util.ResolveTemplatedSpec(metadata, args)
	if err != nil {
		return nil, err
	}

	pipelineDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	// use the incoming spec from the PipelineRollout after templating, except replace the InterstepBufferServiceName with the one that's dynamically derived
	pipelineDef.Object["spec"] = pipelineSpec
	pipelineDef.Object["metadata"] = metadataResolved
	pipelineDef.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipelineDef.SetName(pipelineName)
	pipelineDef.SetNamespace(pipelineRollout.Namespace)
	pipelineDef.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(pipelineRollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)})

	if err := numaflowtypes.PipelineWithISBServiceName(pipelineDef, isbsvcName); err != nil {
		return nil, err
	}

	return pipelineDef, nil
}

func getPipelineChildResourceHealth(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, cond := range conditions {
		switch cond.Type {
		case "VerticesHealthy", "SideInputsManagersHealthy", "DaemonServiceHealthy":
			// if any child resource unhealthy return status (false/unknown)
			if cond.Status != metav1.ConditionTrue {
				return cond.Status, cond.Reason
			}
		}
	}
	return metav1.ConditionTrue, ""
}

func (r *PipelineRolloutReconciler) getCurrentChildCount(rolloutObject ctlrcommon.RolloutObject) (int32, bool) {
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	if pipelineRollout.Status.NameCount == nil {
		return int32(0), false
	} else {
		return *pipelineRollout.Status.NameCount, true
	}
}

func (r *PipelineRolloutReconciler) updateCurrentChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, nameCount int32) error {
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	pipelineRollout.Status.NameCount = &nameCount
	return r.updatePipelineRolloutStatus(ctx, pipelineRollout)
}

// IncrementChildCount increments the child count for the Rollout and returns the count to use
// This implements a function of the RolloutController interface
func (r *PipelineRolloutReconciler) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	currentNameCount, found := r.getCurrentChildCount(rolloutObject)
	if !found {
		currentNameCount = int32(0)
		err := r.updateCurrentChildCount(ctx, rolloutObject, int32(0))
		if err != nil {
			return int32(0), err
		}
	}

	// For readability of the pipeline name, keep the count from getting too high by rolling around back to 0
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

// get the isbsvc child of ISBServiceRollout with the given upgrading state label
func (r *PipelineRolloutReconciler) getISBSvc(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, upgradeState common.UpgradeState) (*unstructured.Unstructured, error) {
	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil || isbsvcRollout == nil {
		return nil, fmt.Errorf("unable to find ISBServiceRollout, err=%v", err)
	}

	isbsvc, err := ctlrcommon.FindMostCurrentChildOfUpgradeState(ctx, isbsvcRollout, upgradeState, nil, false, r.client)
	if err != nil {
		return nil, err
	}
	return isbsvc, nil
}

// get all isbsvc children of ISBServiceRollout with the given upgrading state label
func (r *PipelineRolloutReconciler) getISBServicesByUpgradeState(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, upgradeState common.UpgradeState) (unstructured.UnstructuredList, error) {
	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil || isbsvcRollout == nil {
		return unstructured.UnstructuredList{}, fmt.Errorf("unable to find ISBServiceRollout, err=%v", err)
	}

	return ctlrcommon.FindChildrenOfUpgradeState(ctx, isbsvcRollout, upgradeState, nil, false, r.client)
}

func (r *PipelineRolloutReconciler) ErrorHandler(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, err error, reason, msg string) {
	numaLogger := logger.FromContext(ctx)
	_, file, line, _ := runtime.Caller(1) // '1' goes back one level in the stack to get the caller of ErrorHandler
	numaLogger.Error(err, "ErrorHandler", "failedAt:", fmt.Sprintf("%s:%d", file, line))
	r.customMetrics.PipelineROSyncErrors.WithLabelValues().Inc()
	r.recorder.Eventf(pipelineRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

// return true if there are still more pipelines that need to be deleted
func (r *PipelineRolloutReconciler) garbageCollectChildren(
	ctx context.Context,
	pipelineRollout *apiv1.PipelineRollout,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	// first check to see if there are any isbservices that are marked "recyclable"
	// our pipelines need to be marked "recyclable" if they are using one of those
	recyclableISBServices, err := r.getISBServicesByUpgradeState(ctx, pipelineRollout, common.LabelValueUpgradeRecyclable)
	if err != nil {
		return false, fmt.Errorf("error getting isbservices of type recyclable: %s", err.Error())
	}
	numaLogger.WithValues("recyclable isbservices", recyclableISBServices).Debug("locating recyclable isbservices")

	allPipelines, err := numaflowtypes.GetPipelinesForRollout(ctx, r.client, pipelineRollout, false)
	if err != nil {
		return false, fmt.Errorf("error getting all pipelines (in order to mark recyclable): %s", err.Error())
	}

	// for each recyclable isbsvc:
	for _, isbsvc := range recyclableISBServices.Items {
		// see if any pipelines are using it: if so, mark them "recyclable"
		for _, pipeline := range allPipelines.Items {
			pipelineISBSvcName, err := numaflowtypes.GetPipelineISBSVCName(&pipeline)
			if err != nil {
				return false, err
			}
			if pipelineISBSvcName == isbsvc.GetName() {
				recyclableReason := isbsvc.GetLabels()[common.LabelKeyUpgradeStateReason]
				numaLogger.WithValues("pipeline", pipeline.GetName(), "isbsvc", pipelineISBSvcName).Debug("marking pipeline 'recyclable' since isbsvc is 'recyclable'")
				upgradeStateReason := common.UpgradeStateReason(recyclableReason)
				err = ctlrcommon.UpdateUpgradeState(ctx, r.client, common.LabelValueUpgradeRecyclable, &upgradeStateReason, &pipeline)
				if err != nil {
					return false, fmt.Errorf("failed to mark pipeline %s 'recyclable': %s/%s", pipeline.GetNamespace(), pipeline.GetName(), err.Error())
				}
			}
		}

	}

	return ctlrcommon.GarbageCollectChildren(ctx, pipelineRollout, r, r.client)
}

func getLivePipelineRollout(ctx context.Context, name, namespace string) (*apiv1.PipelineRollout, error) {
	PipelineRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().PipelineRollouts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	PipelineRollout.SetGroupVersionKind(apiv1.PipelineRolloutGroupVersionKind)

	return PipelineRollout, err
}

// GetDesiredRiders gets the list of Riders as specified in the PipelineRollout, templated for the specific pipeline name and
// based on the pipeline definition.
// Note the pipelineName can be different from pipelineDef.GetName().
// The pipelineName is what's used for templating the Rider definition, while the pipelineDef is really only used in the case of "per-vertex" Riders.
// In this case, it's necessary to use the existing pipeline's name to template in order to effectively compare whether the Rider has changed, but
// use the latest pipeline definition to derive the current list of Vertices that need Riders.
func (r *PipelineRolloutReconciler) GetDesiredRiders(rolloutObject ctlrcommon.RolloutObject, pipelineName string, pipelineDef *unstructured.Unstructured) ([]riders.Rider, error) {
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	desiredRiders := []riders.Rider{}
	for _, rider := range pipelineRollout.Spec.Riders {

		var asMap map[string]interface{}
		if err := util.StructToStruct(rider.Definition, &asMap); err != nil {
			return desiredRiders, fmt.Errorf("rider definition could not converted to map: %w", err)
		}

		if rider.PerVertex {
			// create one Rider per Vertex
			vertices, _, err := unstructured.NestedSlice(pipelineDef.Object, "spec", "vertices")
			if err != nil {
				return desiredRiders, err
			}
			for _, vertex := range vertices {
				vertexName := vertex.(map[string]interface{})["name"]
				resolvedMap, err := util.ResolveTemplatedSpec(asMap, map[string]interface{}{
					common.TemplatePipelineName:      pipelineName,
					common.TemplatePipelineNamespace: pipelineRollout.Namespace,
					common.TemplateVertexName:        vertexName,
				})
				if err != nil {
					return desiredRiders, err
				}
				unstruc := unstructured.Unstructured{}
				unstruc.Object = resolvedMap
				unstruc.SetNamespace(pipelineRollout.Namespace)
				unstruc.SetName(fmt.Sprintf("%s-%s-%s", unstruc.GetName(), pipelineName, vertexName))
				desiredRiders = append(desiredRiders, riders.Rider{Definition: unstruc, RequiresProgressive: rider.Progressive})
			}
		} else {
			// create one Rider for the Pipeline
			resolvedMap, err := util.ResolveTemplatedSpec(asMap, map[string]interface{}{
				common.TemplatePipelineName:      pipelineName,
				common.TemplatePipelineNamespace: pipelineRollout.Namespace,
			})
			if err != nil {
				return desiredRiders, err
			}
			unstruc := unstructured.Unstructured{}
			unstruc.Object = resolvedMap
			unstruc.SetNamespace(pipelineRollout.Namespace)
			unstruc.SetName(fmt.Sprintf("%s-%s", unstruc.GetName(), pipelineName))
			desiredRiders = append(desiredRiders, riders.Rider{Definition: unstruc, RequiresProgressive: rider.Progressive})
		}
	}

	// verify that desiredRiders are all permitted Kinds
	if !riders.VerifyRidersPermitted(desiredRiders) {
		return desiredRiders, fmt.Errorf("rider definitions contained unpermitted Kind")
	}

	return desiredRiders, nil
}

// Get the Riders that have been deployed
// If "upgrading==true", return those which are associated with the Upgrading Pipeline; otherwise return those which are associated with the Promoted one
func (r *PipelineRolloutReconciler) GetExistingRiders(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgrading bool) (unstructured.UnstructuredList, error) {
	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)

	ridersList := pipelineRollout.Status.Riders // use the Riders for the promoted pipeline
	if upgrading {
		ridersList = pipelineRollout.Status.ProgressiveStatus.UpgradingPipelineStatus.Riders // use the Riders for the upgrading pipeline
	}

	return riders.GetRidersFromK8S(ctx, pipelineRollout.GetNamespace(), ridersList, r.client)
}

// listAndDeleteChildPipelines lists all child pipelines and deletes them
// return true if we need to requeue
func (r *PipelineRolloutReconciler) listAndDeleteChildPipelines(ctx context.Context, pipelineRollout *apiv1.PipelineRollout) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	pipelineList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, numaflowv1.PipelineGroupVersionResource.Resource,
		pipelineRollout.Namespace, fmt.Sprintf("%s=%s", common.LabelKeyParentRollout, pipelineRollout.Name), "")
	if err != nil {
		if apierrors.IsNotFound(err) {
			numaLogger.Warnf("no child pipeline found for PipelineRollout %s/%s: %v", pipelineRollout.Namespace, pipelineRollout.Name, err)
			return false, nil
		}
		return false, err
	}
	if len(pipelineList.Items) > 0 {
		// Delete all pipelines that are children of this PipelineRollout
		numaLogger.Infof("Deleting pipeline %s/%s", pipelineRollout.Namespace, pipelineRollout.Name)
		for _, pipeline := range pipelineList.Items {
			if err := r.client.Delete(ctx, &pipeline); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

// update Status to reflect the current Riders (for promoted pipeline)
func (r *PipelineRolloutReconciler) SetCurrentRiderList(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, riders []riders.Rider) {

	numaLogger := logger.FromContext(ctx)

	pipelineRollout := rolloutObject.(*apiv1.PipelineRollout)
	pipelineRollout.Status.Riders = make([]apiv1.RiderStatus, len(riders))
	for index, rider := range riders {
		pipelineRollout.Status.Riders[index] = apiv1.RiderStatus{
			GroupVersionKind: kubernetes.SchemaGVKToMetaGVK(rider.Definition.GroupVersionKind()),
			Name:             rider.Definition.GetName(),
		}
	}
	numaLogger.Debugf("setting PipelineRollout.Status.Riders=%+v", pipelineRollout.Status.Riders)
}
