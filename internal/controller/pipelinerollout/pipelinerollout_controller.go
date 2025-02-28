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
	"strings"
	"sync"
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
	"k8s.io/client-go/util/workqueue"
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
)

var (
	PipelineROReconciler *PipelineRolloutReconciler
	initTime             time.Time
)

// PipelineRolloutReconciler reconciles a PipelineRollout object
type PipelineRolloutReconciler struct {
	client client.Client
	scheme *runtime.Scheme

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
	s *runtime.Scheme,
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

	// Get PipelineRollout CR
	pipelineRollout := &apiv1.PipelineRollout{}
	if err := r.client.Get(ctx, namespacedName, pipelineRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(pipelineRollout, err, "GetPipelineRolloutFailed", "Failed to get PipelineRollout")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	pipelineRolloutOrig := pipelineRollout
	pipelineRollout = pipelineRolloutOrig.DeepCopy()

	pipelineRollout.Status.Init(pipelineRollout.Generation)

	requeueDelay, existingPipelineDef, err := r.reconcile(ctx, pipelineRollout, syncStartTime)
	if err != nil {
		r.ErrorHandler(pipelineRollout, err, "ReconcileFailed", "Failed to reconcile PipelineRollout")
		statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// Update PipelineRollout Status based on child resource (Pipeline) Status
	err = r.processPipelineStatus(ctx, pipelineRollout, existingPipelineDef)
	if err != nil {
		r.ErrorHandler(pipelineRollout, err, "ProcessPipelineStatusFailed", "Failed to process Pipeline Status")
		statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}

		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(pipelineRolloutOrig, pipelineRollout) {
		pipelineRolloutStatus := pipelineRollout.Status
		if err := r.client.Update(ctx, pipelineRollout); err != nil {
			r.ErrorHandler(pipelineRollout, err, "UpdateFailed", "Failed to update PipelineRollout")
			statusUpdateErr := r.updatePipelineRolloutStatusToFailed(ctx, pipelineRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		pipelineRollout.Status = pipelineRolloutStatus
	}

	// Update the Status subresource
	if pipelineRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updatePipelineRolloutStatus(ctx, pipelineRollout)
		if statusUpdateErr != nil {
			r.ErrorHandler(pipelineRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update PipelineRollout status")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate the metrics for the Pipeline.
	r.customMetrics.IncPipelineROsRunning(pipelineRollout.Name, pipelineRollout.Namespace)

	if requeueDelay > 0 {
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	r.recorder.Eventf(pipelineRollout, "Normal", "ReconcileSuccess", "Reconciliation successful")
	numaLogger.Debug("reconciliation successful")

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
		r.customMetrics.SetPipelineRolloutHealth(pipelineRollout.Namespace, pipelineRollout.Name, string(pipelineRollout.Status.Phase))
	}()

	// is PipelineRollout being deleted? need to remove the finalizer, so it can
	// (OwnerReference will delete the underlying Pipeline through Cascading deletion)
	if !pipelineRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting PipelineRollout")
		if controllerutil.ContainsFinalizer(pipelineRollout, common.FinalizerName) {
			// Set the foreground deletion policy so that we will block for children to be cleaned up for any type of deletion action
			foreground := metav1.DeletePropagationForeground
			if err := r.client.Delete(ctx, pipelineRollout, &client.DeleteOptions{PropagationPolicy: &foreground}); err != nil {
				return 0, nil, err
			}
			// Get the PipelineRollout live resource
			livePipelineRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().PipelineRollouts(pipelineRollout.Namespace).Get(ctx, pipelineRollout.Name, metav1.GetOptions{})
			if err != nil {
				return 0, nil, fmt.Errorf("error getting the live PipelineRollout: %w", err)
			}
			*pipelineRollout = *livePipelineRollout
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

		if promotedPipelines == nil || len(promotedPipelines.Items) == 0 {

			numaLogger.Debugf("Pipeline %s/%s doesn't exist so creating", pipelineRollout.Namespace, pipelineRollout.Name)
			pipelineRollout.Status.MarkPending()

			// need to know if the pipeline needs to be created with "desiredPhase" = "Paused" or not
			// (i.e. if isbsvc or numaflow controller is requesting pause)
			userPreferredStrategy, err := usde.GetUserStrategy(ctx, newPipelineDef.GetNamespace())
			if err != nil {
				return 0, nil, err
			}
			if userPreferredStrategy == config.PPNDStrategyID {
				needsPaused, _, err := r.shouldBePaused(ctx, pipelineRollout, nil, newPipelineDef, false)
				if err != nil {
					return 0, nil, err
				}
				if needsPaused != nil && *needsPaused {
					err = numaflowtypes.PipelineWithDesiredPhase(newPipelineDef, "Paused")
					if err != nil {
						return 0, nil, err
					}
				}

			}

			err = kubernetes.CreateResource(ctx, r.client, newPipelineDef)
			if err != nil {
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

	inProgressStrategy := r.inProgressStrategyMgr.GetStrategy(ctx, pipelineRollout)
	inProgressStrategySet := inProgressStrategy != apiv1.UpgradeStrategyNoOp

	// clean up recyclable pipelines
	allDeleted, err := r.garbageCollectChildren(ctx, pipelineRollout)
	if err != nil {
		return 0, nil, err
	}
	// there are some cases that require requeueing
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

	// what is the preferred strategy for this namespace?
	userPreferredStrategy, err := usde.GetUserStrategy(ctx, newPipelineDef.GetNamespace())
	if err != nil {
		return 0, err
	}

	// does the Resource need updating, and if so how?
	// TODO: handle recreate parameter
	pipelineNeedsToUpdate, upgradeStrategyType, _, err := usde.ResourceNeedsUpdating(ctx, newPipelineDef, existingPipelineDef)
	if err != nil {
		return 0, err
	}

	numaLogger.
		WithValues("pipelineNeedsToUpdate", pipelineNeedsToUpdate, "upgradeStrategyType", upgradeStrategyType).
		Debug("Upgrade decision result")

	// set the Status appropriately to "Pending" or "Deployed" depending on whether pipeline needs to update
	if pipelineNeedsToUpdate {
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

	case apiv1.UpgradeStrategyProgressive:
		numaLogger.Debug("processing pipeline with Progressive")

		// Get the PipelineRollout live resource so we can grab the ProgressiveStatus from that for our own local pipelineRollout
		// (Note we don't copy the entire Status in case we've updated something locally)
		livePipelineRollout, err := kubernetes.NumaplaneClient.NumaplaneV1alpha1().PipelineRollouts(pipelineRollout.Namespace).Get(ctx, pipelineRollout.Name, metav1.GetOptions{})
		if err != nil {
			return 0, fmt.Errorf("error getting the live PipelineRollout for assessment processing: %w", err)
		}

		pipelineRollout.Status.ProgressiveStatus = *livePipelineRollout.Status.ProgressiveStatus.DeepCopy()

		done, _, progressiveRequeueDelay, err := progressive.ProcessResource(ctx, pipelineRollout, existingPipelineDef, pipelineNeedsToUpdate, r, r.client)
		if err != nil {
			return 0, err
		}
		if done {
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

	default:
		if pipelineNeedsToUpdate && upgradeStrategyType == apiv1.UpgradeStrategyApply {
			if err := updatePipelineSpec(ctx, r.client, newPipelineDef); err != nil {
				return 0, err
			}
			pipelineRollout.Status.MarkDeployed(pipelineRollout.Generation)
		}
	}

	if pipelineNeedsToUpdate {
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerPipelineRollout, "update").Observe(time.Since(syncStartTime).Seconds())
	}

	return requeueDelay, nil
}
func pipelineObservedGenerationCurrent(generation int64, observedGeneration int64) bool {
	return generation <= observedGeneration
}

// Set the Condition in the Status for child resource health

func (r *PipelineRolloutReconciler) processPipelineStatus(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, existingPipelineDef *unstructured.Unstructured) error {
	numaLogger := logger.FromContext(ctx)

	// Only fetch the latest pipeline object while deleting the pipeline object, i.e. when pipelineRollout.DeletionTimestamp.IsZero() is false
	if existingPipelineDef == nil {
		// determine name of the promoted Pipeline
		pipelineName, err := ctlrcommon.GetChildName(ctx, pipelineRollout, r, common.LabelValueUpgradePromoted, nil, r.client, true)
		if err != nil {
			return fmt.Errorf("Unable to process pipeline status: err=%s", err)
		}
		pipelineDef := &unstructured.Unstructured{}
		pipelineDef.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
		pipelineDef.SetNamespace(pipelineRollout.Namespace)
		pipelineDef.SetName(pipelineName)

		livePipelineDef, err := kubernetes.GetLiveResource(ctx, pipelineDef, "pipelines")
		if err != nil {
			if apierrors.IsNotFound(err) {
				numaLogger.WithValues("pipelineDefinition", *pipelineDef).Warn("Pipeline not found. Unable to process status during this reconciliation.")
				return nil
			} else {
				return fmt.Errorf("error getting Pipeline for status processing: %v", err)
			}
		}
		existingPipelineDef = livePipelineDef
	}

	pipelineStatus, err := kubernetes.ParseStatus(existingPipelineDef)
	if err != nil {
		return fmt.Errorf("failed to parse Pipeline Status from pipeline CR: %+v, %v", existingPipelineDef, err)
	}

	numaLogger.Debugf("pipeline status: %v", pipelineStatus)

	r.setChildResourcesHealthCondition(pipelineRollout, existingPipelineDef, &pipelineStatus)
	r.setChildResourcesPauseCondition(pipelineRollout, &pipelineStatus)

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
		r.updatePauseMetric(pipelineRollout, pipelinePhase)
		pipelineRollout.Status.MarkPipelinePausingOrPaused(reason, msg, pipelineRollout.Generation)
	} else if pipelinePhase == numaflowv1.PipelinePhasePaused {
		// if LastPauseTransitionTime hasn't been set yet, we are just starting to enter paused phase - set it to denote end of pausing
		if pipelineRollout.Status.PauseStatus.LastPauseTransitionTime == metav1.NewTime(initTime) || !pipelineRollout.Status.PauseStatus.LastPauseTransitionTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseTransitionTime = metav1.NewTime(time.Now())
		}
		reason := fmt.Sprintf("Pipeline%s", string(pipelinePhase))
		msg := fmt.Sprintf("Pipeline %s", strings.ToLower(string(pipelinePhase)))
		r.updatePauseMetric(pipelineRollout, pipelinePhase)
		pipelineRollout.Status.MarkPipelinePausingOrPaused(reason, msg, pipelineRollout.Generation)
	} else {
		// only set EndTime if BeginTime has been previously set AND EndTime is before/equal to BeginTime
		// EndTime is either just initialized or the end of a previous pause which is why it will be before the new BeginTime
		if (pipelineRollout.Status.PauseStatus.LastPauseBeginTime != metav1.NewTime(initTime)) && !pipelineRollout.Status.PauseStatus.LastPauseEndTime.After(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time) {
			pipelineRollout.Status.PauseStatus.LastPauseEndTime = metav1.NewTime(time.Now())
			r.updatePauseMetric(pipelineRollout, pipelinePhase)
		}
		pipelineRollout.Status.MarkPipelineUnpaused(pipelineRollout.Generation)
	}

}

func (r *PipelineRolloutReconciler) updatePauseMetric(pipelineRollout *apiv1.PipelineRollout, pipelinePhase numaflowv1.PipelinePhase) {

	var pipelineSpec numaflowtypes.PipelineSpec
	_ = json.Unmarshal(pipelineRollout.Spec.Pipeline.Spec.Raw, &pipelineSpec)

	// if pause is manual, set metrics back to 0
	if r.isSpecBasedPause(pipelineSpec) {
		r.setMetric(pipelineRollout.Namespace, pipelineRollout.Name, float64(0), float64(0))
	} else if pipelinePhase == numaflowv1.PipelinePhasePaused {
		// if pipeline is Paused, set Paused metric to time duration since last transition time
		r.setMetric(pipelineRollout.Namespace, pipelineRollout.Name, time.Since(pipelineRollout.Status.PauseStatus.LastPauseTransitionTime.Time).Seconds(), float64(0))
	} else if pipelinePhase == numaflowv1.PipelinePhasePausing {
		// if pipeline is Pausing, set Pausing metric to time duration since last pause begin time
		r.setMetric(pipelineRollout.Namespace, pipelineRollout.Name, float64(0), time.Since(pipelineRollout.Status.PauseStatus.LastPauseBeginTime.Time).Seconds())
	} else {
		// otherwise set both metrics back to 0
		r.setMetric(pipelineRollout.Namespace, pipelineRollout.Name, float64(0), float64(0))
	}

}

func (r *PipelineRolloutReconciler) setMetric(namespace, name string, pausedVal, pausingVal float64) {
	r.customMetrics.PipelinePausedSeconds.WithLabelValues(namespace, name).Set(pausedVal)
	r.customMetrics.PipelinePausingSeconds.WithLabelValues(namespace, name).Set(pausingVal)
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
func (r *PipelineRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {

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

	return nil
}

func updatePipelineSpec(ctx context.Context, c client.Client, obj *unstructured.Unstructured) error {
	return kubernetes.UpdateResource(ctx, c, obj)
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

func (r *PipelineRolloutReconciler) makePipelineDefinition(
	pipelineRollout *apiv1.PipelineRollout,
	pipelineName string,
	isbsvcName string,
	metadata apiv1.Metadata,
) (*unstructured.Unstructured, error) {
	pipelineDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	pipelineDef.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipelineDef.SetName(pipelineName)
	pipelineDef.SetNamespace(pipelineRollout.Namespace)
	pipelineDef.SetLabels(metadata.Labels)
	pipelineDef.SetAnnotations(metadata.Annotations)
	pipelineDef.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(pipelineRollout.GetObjectMeta(), apiv1.PipelineRolloutGroupVersionKind)})
	var pipelineSpec map[string]interface{}
	if err := util.StructToStruct(pipelineRollout.Spec.Pipeline.Spec, &pipelineSpec); err != nil {
		return nil, err
	}

	// use the imcoming spec from the PipelineRollout as is, except replace the InterstepBufferServiceName with the one that's dynamically derived
	pipelineDef.Object["spec"] = pipelineSpec

	if err := numaflowtypes.PipelineWithISBServiceName(pipelineDef, isbsvcName); err != nil {
		return nil, err
	}

	return pipelineDef, nil
}

func (r *PipelineRolloutReconciler) drain(ctx context.Context, pipeline *unstructured.Unstructured) error {
	patchJson := `{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}`
	return kubernetes.PatchResource(ctx, r.client, pipeline, patchJson, k8stypes.MergePatchType)
}

// ChildNeedsUpdating() tests for essential equality, with any irrelevant fields eliminated from the comparison
// This implements a function of the progressiveController interface
func (r *PipelineRolloutReconciler) ChildNeedsUpdating(ctx context.Context, from, to *unstructured.Unstructured) (bool, error) {
	numaLogger := logger.FromContext(ctx)
	fromCopy := from.DeepCopy()
	toCopy := to.DeepCopy()
	// remove lifecycle.desiredPhase field from comparison to test for equality
	numaflowtypes.PipelineWithoutDesiredPhase(fromCopy)
	numaflowtypes.PipelineWithoutDesiredPhase(toCopy)

	specsEqual := util.CompareStructNumTypeAgnostic(fromCopy.Object["spec"], toCopy.Object["spec"])
	numaLogger.Debugf("specsEqual: %t, from=%v, to=%v\n",
		specsEqual, fromCopy.Object["spec"], toCopy.Object["spec"])
	labelsEqual := util.CompareMaps(from.GetLabels(), to.GetLabels())
	numaLogger.Debugf("labelsEqual: %t, from Labels=%v, to Labels=%v", labelsEqual, from.GetLabels(), to.GetLabels())
	annotationsEqual := util.CompareMaps(from.GetAnnotations(), to.GetAnnotations())
	numaLogger.Debugf("annotationsEqual: %t, from Annotations=%v, to Annotations=%v", annotationsEqual, from.GetAnnotations(), to.GetAnnotations())

	return !specsEqual || !labelsEqual || !annotationsEqual, nil
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

	err := r.updateCurrentChildCount(ctx, rolloutObject, currentNameCount+1)
	if err != nil {
		return int32(0), err
	}
	return currentNameCount, nil
}

// Recycle deletes child; returns true if it was in fact deleted
// This implements a function of the RolloutController interface
func (r *PipelineRolloutReconciler) Recycle(ctx context.Context,
	pipeline *unstructured.Unstructured,
	c client.Client,
) (bool, error) {
	numaLogger := logger.FromContext(ctx)

	pipelineRollout, err := numaflowtypes.GetRolloutForPipeline(ctx, c, pipeline)
	if err != nil {
		return false, err
	}

	// Need to determine how to delete the pipeline
	// Use the "upgrade-strategy-reason" Label to determine how
	// if upgrade-strategy-reason="delete/recreate", then don't pause at all (it will have already paused if we're in PPND)
	// if upgrade-strategy-reason="progressive success", then either just pause, or pause and drain,
	//    depending on PipelineRollout specification (TODO: https://github.com/numaproj/numaplane/issues/512)
	// if upgrade-strategy-reason="progressive failure", then don't pause at all (in this case we are replacing a failed pipeline)
	upgradeState, upgradeStateReason := ctlrcommon.GetUpgradeState(ctx, c, pipeline)
	if upgradeState == nil || *upgradeState != common.LabelValueUpgradeRecyclable {
		numaLogger.Error(errors.New("Should not call Recycle() on a Pipeline which is not in recyclable Upgrade State"), "Recycle() called on pipeline",
			"namespace", pipeline.GetNamespace(), "name", pipeline.GetName(), "labels", pipeline.GetLabels())
	}
	pause := false
	requireDrain := false
	if upgradeStateReason != nil {
		switch *upgradeStateReason {
		case common.LabelValueDeleteRecreateChild:

		case common.LabelValueProgressiveSuccess:
			pause = true
			requireDrain = true // TODO: make configurable (https://github.com/numaproj/numaplane/issues/512)

		case common.LabelValueProgressiveFailure:

		}
	}

	if pause {
		// check if the Pipeline has been paused or if it can't be paused: if so, then delete the pipeline
		pausedOrWontPause, err := numaflowtypes.IsPipelinePausedOrWontPause(ctx, pipeline, pipelineRollout, requireDrain)
		if err != nil {
			return false, err
		}
		if pausedOrWontPause {
			err = kubernetes.DeleteResource(ctx, c, pipeline)
			return true, err
		}
		// make sure we request Paused if we haven't yet
		desiredPhaseSetting, err := numaflowtypes.GetPipelineDesiredPhase(pipeline)
		if err != nil {
			return false, err
		}
		if desiredPhaseSetting != string(numaflowv1.PipelinePhasePaused) {
			_ = r.drain(ctx, pipeline)
			return false, nil
		}
	} else {
		err = kubernetes.DeleteResource(ctx, c, pipeline)
		return true, err
	}

	return false, nil

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
func (r *PipelineRolloutReconciler) getISBServicesByUpgradeState(ctx context.Context, pipelineRollout *apiv1.PipelineRollout, upgradeState common.UpgradeState) (*unstructured.UnstructuredList, error) {
	isbsvcRollout, err := r.getISBSvcRollout(ctx, pipelineRollout)
	if err != nil || isbsvcRollout == nil {
		return nil, fmt.Errorf("unable to find ISBServiceRollout, err=%v", err)
	}

	return ctlrcommon.FindChildrenOfUpgradeState(ctx, isbsvcRollout, upgradeState, nil, false, r.client)
}

func (r *PipelineRolloutReconciler) ErrorHandler(pipelineRollout *apiv1.PipelineRollout, err error, reason, msg string) {
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
