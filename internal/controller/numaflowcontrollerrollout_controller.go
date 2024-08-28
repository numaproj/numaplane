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
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/argoproj/gitops-engine/pkg/diff"
	gitopsSync "github.com/argoproj/gitops-engine/pkg/sync"
	gitopsSyncCommon "github.com/argoproj/gitops-engine/pkg/sync/common"
	kubeUtil "github.com/argoproj/gitops-engine/pkg/utils/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	yamlserializer "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/sync"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	finalizerName = "numaplane.numaproj.io/numaplane-controller"
)

const (
	ControllerNumaflowControllerRollout = "numaflow-controller-rollout-controller"
	NumaflowControllerDeploymentName    = "numaflow-controller"
	DefaultNumaflowControllerImageName  = "numaflow"
)

// NumaflowControllerRolloutReconciler reconciles a NumaflowControllerRollout object
type NumaflowControllerRolloutReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	restConfig    *rest.Config
	rawConfig     *rest.Config
	kubectl       kubeUtil.Kubectl
	stateCache    sync.LiveStateCache
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder
}

func NewNumaflowControllerRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	rawConfig *rest.Config,
	kubectl kubeUtil.Kubectl,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) (*NumaflowControllerRolloutReconciler, error) {
	stateCache := sync.NewLiveStateCache(rawConfig, customMetrics)
	numaLogger := logger.GetBaseLogger().WithName("state cache").WithValues("numaflowcontrollerrollout")
	err := stateCache.Init(numaLogger)
	if err != nil {
		return nil, err
	}

	kubectl.SetOnKubectlRun(func(command string) (kubeUtil.CleanupFunc, error) {
		customMetrics.NumaflowControllerKubectlExecutionCounter.WithLabelValues().Inc()
		return func() {}, nil
	})
	restConfig := rawConfig
	return &NumaflowControllerRolloutReconciler{
		client,
		s,
		restConfig,
		rawConfig,
		kubectl,
		stateCache,
		customMetrics,
		recorder,
	}, nil
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
	r.customMetrics.NumaflowControllersSynced.WithLabelValues().Inc()

	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, numaflowControllerRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(numaflowControllerRollout, err, "GetNumaflowControllerFailed", "Failed to get numaflow controller rollout")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	numaflowControllerRolloutOrig := numaflowControllerRollout
	numaflowControllerRollout = numaflowControllerRolloutOrig.DeepCopy()

	numaflowControllerRollout.Status.Init(numaflowControllerRollout.Generation)

	result, err := r.reconcile(ctx, numaflowControllerRollout, req.Namespace, syncStartTime)
	if err != nil {
		r.ErrorHandler(numaflowControllerRollout, err, "ReconcileFailed", "Failed to reconcile numaflow controller rollout")
		statusUpdateErr := r.updateNumaflowControllerRolloutStatusToFailed(ctx, numaflowControllerRollout, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(numaflowControllerRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of numaflow controller rollout")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	deployment, _, err := r.getNumaflowControllerDeployment(ctx, numaflowControllerRollout)
	if err != nil {
		r.recorder.Eventf(numaflowControllerRollout, corev1.EventTypeWarning, "GetDeploymentFailed", "Failed to get numaflow controller deployment: %v", err.Error())
		return ctrl.Result{}, err
	}

	// update our Status with the Deployment's Status
	err = r.processNumaflowControllerStatus(numaflowControllerRollout, deployment)
	if err != nil {
		r.recorder.Eventf(numaflowControllerRollout, corev1.EventTypeWarning, "ProcessStatusFailed", "Failed to process numaflow controller status: %v", err.Error())
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(numaflowControllerRolloutOrig, numaflowControllerRollout) {
		numaflowControllerRolloutStatus := numaflowControllerRollout.Status
		if err := r.client.Update(ctx, numaflowControllerRollout); err != nil {
			r.ErrorHandler(numaflowControllerRollout, err, "UpdateFailed", "Failed to update numaflow controller rollout")
			statusUpdateErr := r.updateNumaflowControllerRolloutStatusToFailed(ctx, numaflowControllerRollout, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(numaflowControllerRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of numaflow controller rollout")
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
			r.ErrorHandler(numaflowControllerRollout, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of numaflow controller rollout")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	// generate the metrics for the numaflow controller based on a numaflow version.
	r.customMetrics.IncNumaflowControllerMetrics(numaflowControllerRollout.Name, numaflowControllerRollout.Namespace, numaflowControllerRollout.Spec.Controller.Version)

	numaLogger.Debug("reconciliation successful")
	r.recorder.Eventf(numaflowControllerRollout, corev1.EventTypeNormal, "ReconcileSuccess", "Reconciliation successful")
	return result, nil
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

// reconcile does the real logic
func (r *NumaflowControllerRolloutReconciler) reconcile(
	ctx context.Context,
	controllerRollout *apiv1.NumaflowControllerRollout,
	namespace string,
	syncStartTime time.Time,
) (ctrl.Result, error) {
	numaLogger := logger.FromContext(ctx)

	controllerKey := GetPauseModule().getNumaflowControllerKey(namespace)

	if !controllerRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting NumaflowControllerRollout")
		r.recorder.Eventf(controllerRollout, corev1.EventTypeNormal, "Deleting", "Deleting NumaflowControllerRollout")
		if controllerutil.ContainsFinalizer(controllerRollout, finalizerName) {
			GetPauseModule().deletePauseRequest(controllerKey)
			controllerutil.RemoveFinalizer(controllerRollout, finalizerName)
		}
		// generate the metrics for the numaflow controller deletion based on a numaflow version.
		r.customMetrics.DecNumaflowControllerMetrics(controllerRollout.Name, controllerRollout.Namespace, controllerRollout.Spec.Controller.Version)
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "delete").Observe(time.Since(syncStartTime).Seconds())
		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(controllerRollout, finalizerName) {
		controllerutil.AddFinalizer(controllerRollout, finalizerName)
	}

	_, pauseRequestExists := GetPauseModule().getPauseRequest(controllerKey)
	if !pauseRequestExists {
		// this is just creating an entry in the map if it doesn't already exist
		GetPauseModule().newPauseRequest(controllerKey)
	}

	deployment, deploymentExists, err := r.getNumaflowControllerDeployment(ctx, controllerRollout)
	if err != nil {
		return ctrl.Result{}, err
	}

	if deploymentExists && common.DataLossPrevention {
		numaLogger.Debugf("found existing numaflow-controller Deployment")

		// if I need to update or am in the middle of an update of the Controller Deployment, then I need to make sure all the Pipelines are pausing
		controllerDeploymentNeedsUpdating, controllerDeploymentIsUpdating, err := r.isControllerDeploymentUpdating(ctx, controllerRollout, deployment)
		if err != nil {
			return ctrl.Result{}, err
		}

		numaLogger.Debugf("controllerDeploymentNeedsUpdating=%t, controllerDeploymentIsUpdating=%t", controllerDeploymentNeedsUpdating, controllerDeploymentIsUpdating)

		// set the Status appropriately to "Pending" or "Deployed"
		// if controllerDeploymentNeedsUpdating - this means there's a mismatch between the desired NumaflowControllerRollout version and actual
		// if there's a generation mismatch - this means we haven't even observed the current generation
		// we may match the first case and not the second when we've observed the generation change but we're pausing pipelines
		// we may match the second case and not the first if we need to update something other than Numaflow Controller version
		if controllerDeploymentNeedsUpdating || controllerRollout.Status.ObservedGeneration < controllerRollout.Generation {
			controllerRollout.Status.MarkPending()
		} else {
			controllerRollout.Status.MarkDeployed(controllerRollout.Generation)
		}

		needsRequeue, err := processChildObjectWithoutDataLoss(ctx, controllerRollout.Namespace, controllerRollout.Name, r, controllerDeploymentNeedsUpdating,
			controllerDeploymentIsUpdating, func() error {
				r.recorder.Eventf(controllerRollout, corev1.EventTypeNormal, "AllPipelinesPaused", "All Pipelines have paused so Numaflow Controller can safely update")
				phase, err := r.sync(controllerRollout, namespace, numaLogger)
				if err != nil {
					return err
				}

				r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "update").Observe(time.Since(syncStartTime).Seconds())
				if phase != gitopsSyncCommon.OperationSucceeded {
					return fmt.Errorf("sync operation is not successful")
				}
				return nil
			})
		if err != nil {
			return ctrl.Result{}, err
		}
		if needsRequeue {
			return common.DefaultDelayedRequeue, nil
		}

	}

	// apply controller - this handles syncing in the cases in which our Controller Rollout isn't updating
	// (note that the cases above in which it is updating have a 'return' statement):
	// - new ControllerRollout
	// - auto healing
	// - somebody changed the manifest associated with the Controller version (shouldn't happen but could)
	phase, err := r.sync(controllerRollout, namespace, numaLogger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if phase != gitopsSyncCommon.OperationSucceeded {
		return ctrl.Result{}, fmt.Errorf("sync operation is not successful")
	}

	// Generate the creation metrics only if the numaflow controller is newly created
	if !deploymentExists {
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowControllerRollout, "create").Observe(time.Since(syncStartTime).Seconds())
	}

	return ctrl.Result{}, nil
}

// for the purpose of logging
func (r *NumaflowControllerRolloutReconciler) getChildTypeString() string {
	return "Numaflow Controller"
}

func (r *NumaflowControllerRolloutReconciler) getPipelineList(ctx context.Context, rolloutNamespace string, _ string) ([]*kubernetes.GenericObject, error) {
	return kubernetes.ListCR(ctx, r.restConfig, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines", rolloutNamespace, "", "")
}

func (r *NumaflowControllerRolloutReconciler) getRolloutKey(rolloutNamespace string, rolloutName string) string {
	return GetPauseModule().getNumaflowControllerKey(rolloutNamespace)
}

// determine if it needs to update or is already in the middle of an update (waiting for Reconciliation)
// return values:
// - does it need to update?
// - is it already in the middle of an update?
func (r *NumaflowControllerRolloutReconciler) isControllerDeploymentUpdating(ctx context.Context, controllerRollout *apiv1.NumaflowControllerRollout, existingDeployment *appsv1.Deployment) (bool, bool, error) {
	numaLogger := logger.FromContext(ctx)

	_, healthConditionReason, _ := processDeploymentHealth(existingDeployment)
	controllerDeploymentReconciled := healthConditionReason != "Progressing"

	currentVersion, err := getControllerDeploymentVersion(existingDeployment)
	if err != nil {
		return false, false, err
	}
	controllerVersionNeedsToUpdate := (controllerRollout.Spec.Controller.Version != currentVersion)
	if controllerVersionNeedsToUpdate {
		numaLogger.Debugf("current Deployment image tag=%q differs from desired %q", currentVersion, controllerRollout.Spec.Controller.Version)
	}

	return controllerVersionNeedsToUpdate, !controllerDeploymentReconciled, nil
}

// applyOwnershipToManifests Applies NumaflowControllerRollout ownership to
// Kubernetes manifests, returning modified manifests or an error.
func applyOwnershipToManifests(manifests []string, controllerRollout *apiv1.NumaflowControllerRollout) ([]string, error) {
	manifestsWithOwnership := make([]string, 0, len(manifests))
	for _, v := range manifests {
		reference, err := applyOwnership(v, controllerRollout)
		if err != nil {
			return nil, err
		}
		manifestsWithOwnership = append(manifestsWithOwnership, string(reference))
	}
	return manifestsWithOwnership, nil
}

func applyOwnership(manifest string, controllerRollout *apiv1.NumaflowControllerRollout) ([]byte, error) {
	// Decode YAML into an Unstructured object
	decUnstructured := yamlserializer.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	_, _, err := decUnstructured.Decode([]byte(manifest), nil, obj)
	if err != nil {
		return nil, err
	}

	// Construct the new owner reference
	ownerRef := map[string]interface{}{
		"apiVersion":         controllerRollout.APIVersion,
		"kind":               controllerRollout.Kind,
		"name":               controllerRollout.Name,
		"uid":                string(controllerRollout.UID),
		"controller":         true,
		"blockOwnerDeletion": true,
	}

	// Get existing owner references and check if our reference is already there
	existingRefs, found, err := unstructured.NestedSlice(obj.Object, "metadata", "ownerReferences")
	if err != nil {
		return nil, err
	}
	if !found {
		existingRefs = []interface{}{}
	}

	// Check if the owner reference already exists to avoid duplication
	alreadyExists := ownerExists(existingRefs, ownerRef)

	// Add the new owner reference if it does not exist
	if !alreadyExists {
		existingRefs = append(existingRefs, ownerRef)
		err = unstructured.SetNestedSlice(obj.Object, existingRefs, "metadata", "ownerReferences")
		if err != nil {
			return nil, err
		}
	}

	// Marshal the updated object into YAML
	modifiedManifest, err := sigsyaml.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return modifiedManifest, nil
}

// ownerExists checks if an owner reference already exists in the list of owner references.
func ownerExists(existingRefs []interface{}, ownerRef map[string]interface{}) bool {
	var alreadyExists bool
	for _, ref := range existingRefs {
		if refMap, ok := ref.(map[string]interface{}); ok {
			if refMap["uid"] == ownerRef["uid"] {
				alreadyExists = true
				break
			}
		}
	}
	return alreadyExists
}

func (r *NumaflowControllerRolloutReconciler) sync(
	rollout *apiv1.NumaflowControllerRollout,
	namespace string,
	numaLogger *logger.NumaLogger,
) (gitopsSyncCommon.OperationPhase, error) {

	// Get the target manifests based on the version of the controller and throw an error if the definition not for a version.
	version := rollout.Spec.Controller.Version
	definition := config.GetConfigManagerInstance().GetControllerDefinitionsMgr().GetNumaflowControllerDefinitionsConfig()
	manifest := definition[version]
	if len(manifest) == 0 {
		return gitopsSyncCommon.OperationError, fmt.Errorf("no controller definition found for version %s", version)
	}

	// Applying ownership reference
	manifests, err := SplitYAMLToString([]byte(manifest))
	if err != nil {
		return gitopsSyncCommon.OperationError, fmt.Errorf("can not parse file data, err: %v", err)
	}
	manifestsWithOwnership, err := applyOwnershipToManifests(manifests, rollout)
	if err != nil {
		return gitopsSyncCommon.OperationError, fmt.Errorf("failed to apply ownership reference, %w", err)
	}

	targetObjs, err := toUnstructuredAndApplyLabel(manifestsWithOwnership, rollout.Name)
	if err != nil {
		return gitopsSyncCommon.OperationError, fmt.Errorf("failed to parse the manifest, %w", err)
	}
	numaLogger.Debugf("found %d target objects associated with Numaflow Controller version %s; versions defined:%+v", len(targetObjs), version, definition)

	reconciliationResult, diffResults, err := r.compareState(rollout, namespace, targetObjs, numaLogger)
	if err != nil {
		return gitopsSyncCommon.OperationError, err
	}

	opts := []gitopsSync.SyncOpt{
		gitopsSync.WithLogr(*numaLogger.LogrLogger),
		gitopsSync.WithOperationSettings(false, true, false, false),
		gitopsSync.WithManifestValidation(true),
		gitopsSync.WithPruneLast(false),
		gitopsSync.WithResourceModificationChecker(true, diffResults),
		gitopsSync.WithReplace(false),
		gitopsSync.WithServerSideApply(true),
		gitopsSync.WithServerSideApplyManager(common.SSAManager),
	}

	clusterCache, err := r.stateCache.GetClusterCache()
	if err != nil {
		return gitopsSyncCommon.OperationError, err
	}
	openAPISchema := clusterCache.GetOpenAPISchema()

	syncCtx, cleanup, err := gitopsSync.NewSyncContext(
		"",
		reconciliationResult,
		r.restConfig,
		r.rawConfig,
		r.kubectl,
		namespace,
		openAPISchema,
		opts...,
	)
	defer cleanup()
	if err != nil {
		return gitopsSyncCommon.OperationError, err
	}

	syncCtx.Sync()

	rollout.Status.MarkDeployed(rollout.Generation)

	phase, _, _ := syncCtx.GetState()
	return phase, nil
}

// compareState compares with desired state of the objects with the live state in the cluster
// for the target objects.
func (r *NumaflowControllerRolloutReconciler) compareState(
	rollout *apiv1.NumaflowControllerRollout,
	namespace string,
	targetObjs []*unstructured.Unstructured,
	numaLogger *logger.NumaLogger,
) (gitopsSync.ReconciliationResult, *diff.DiffResultList, error) {
	var infoProvider kubeUtil.ResourceInfoProvider
	clusterCache, err := r.stateCache.GetClusterCache()
	if err != nil {
		return gitopsSync.ReconciliationResult{}, nil, err
	}
	infoProvider = clusterCache
	liveObjByKey, err := r.stateCache.GetManagedLiveObjs(rollout.Name, namespace, targetObjs)
	if err != nil {
		return gitopsSync.ReconciliationResult{}, nil, err
	}
	reconciliationResult := gitopsSync.Reconcile(targetObjs, liveObjByKey, namespace, infoProvider)

	// Ignore `status` field for all comparison.
	// TODO: make it configurable
	overrides := map[string]sync.ResourceOverride{
		"*/*": {
			IgnoreDifferences: sync.OverrideIgnoreDiff{JSONPointers: []string{"/status"}}},
	}

	resourceOps, cleanup, err := r.getResourceOperations()
	if err != nil {
		return gitopsSync.ReconciliationResult{}, nil, err
	}
	defer cleanup()

	diffOpts := []diff.Option{
		diff.WithLogr(*numaLogger.LogrLogger),
		diff.WithServerSideDiff(true),
		diff.WithServerSideDryRunner(diff.NewK8sServerSideDryRunner(resourceOps)),
		diff.WithManager(common.SSAManager),
		diff.WithGVKParser(clusterCache.GetGVKParser()),
	}

	diffResults, err := sync.StateDiffs(reconciliationResult.Target, reconciliationResult.Live, overrides, diffOpts)
	if err != nil {
		return reconciliationResult, nil, err
	}

	return reconciliationResult, diffResults, nil
}

// getResourceOperations will return the kubectl implementation of the ResourceOperations
// interface that provides functionality to manage kubernetes resources. Returns a
// cleanup function that must be called to remove the generated kube config for this
// server.
func (r *NumaflowControllerRolloutReconciler) getResourceOperations() (kubeUtil.ResourceOperations, func(), error) {
	clusterCache, err := r.stateCache.GetClusterCache()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting cluster cache: %w", err)
	}

	ops, cleanup, err := r.kubectl.ManageResources(r.restConfig, clusterCache.GetOpenAPISchema())
	if err != nil {
		return nil, nil, fmt.Errorf("error creating kubectl ResourceOperations: %w", err)
	}
	return ops, cleanup, nil
}

func getDeploymentCondition(status appsv1.DeploymentStatus, condType appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for _, cond := range status.Conditions {
		if cond.Type == condType {
			return &cond
		}
	}
	return nil
}

// return:
// - the Deployment, if it exists
// - whether it exists
// - error if any
func (r *NumaflowControllerRolloutReconciler) getNumaflowControllerDeployment(ctx context.Context, controllerRollout *apiv1.NumaflowControllerRollout) (*appsv1.Deployment, bool, error) {
	deployment := &appsv1.Deployment{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Namespace: controllerRollout.Namespace, Name: NumaflowControllerDeploymentName}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		} else {
			return nil, false, err
		}
	}
	return deployment, true, nil
}

// get the tag of the numaflow container
func getControllerDeploymentVersion(deployment *appsv1.Deployment) (string, error) {

	c, err := config.GetConfigManagerInstance().GetConfig()
	if err != nil {
		return "", fmt.Errorf("error getting ConfigMap: %+v", err)
	}
	imageNames := []string{DefaultNumaflowControllerImageName}
	if c.NumaflowControllerImageNames != nil && len(c.NumaflowControllerImageNames) > 0 {
		imageNames = c.NumaflowControllerImageNames
	}

	// in case the Deployment has sidecars, find the container whose image is named "numaflow"
	containers := deployment.Spec.Template.Spec.Containers
	for _, c := range containers {
		imageName := c.Image
		tag := ""
		colon := strings.Index(c.Image, ":")
		if colon != -1 {
			imageName = c.Image[0:colon]
			tag = c.Image[colon+1:]
		}
		finalSlash := strings.LastIndex(imageName, "/")
		if finalSlash != -1 {
			imageName = imageName[finalSlash+1:]

		}
		// is this is the Numaflow Controller itself?
		isNumaflowController := false
		for _, nfControllerImageName := range imageNames {
			if imageName == nfControllerImageName {
				isNumaflowController = true
				break
			}
		}
		if isNumaflowController {
			if tag == "" {
				return "", fmt.Errorf("no tag found in image path %q from Deployment %+v", c.Image, deployment)
			} else {
				// expect first letter to be a "v"
				if tag[0] != 'v' {
					return "", fmt.Errorf("expected numaflow-controller tag to start with 'v': %q", tag)
				}
				return tag[1:], nil
			}
		}
	}
	return "", fmt.Errorf("couldn't find image named %q in Deployment %+v", DefaultNumaflowControllerImageName, deployment)
}

func processDeploymentHealth(deployment *appsv1.Deployment) (bool, string, string) {

	if deployment == nil {
		msg := "Numaflow Controller Deployment not found"
		return false, "Progressing", msg
	}

	deploymentSpec := deployment.Spec
	deploymentStatus := deployment.Status

	// Health Check borrowed from argoproj/gitops-engine/pkg/health/health_deployment.go https://github.com/argoproj/gitops-engine/blob/master/pkg/health/health_deployment.go#L27
	if deployment.Generation <= deploymentStatus.ObservedGeneration {
		cond := getDeploymentCondition(deploymentStatus, appsv1.DeploymentProgressing)
		if cond != nil && cond.Reason == "ProgressDeadlineExceeded" {
			msg := fmt.Sprintf("Deployment %q exceeded its progress deadline", deployment.Name)
			return false, "Degraded", msg
		} else if deploymentSpec.Replicas != nil && deploymentStatus.UpdatedReplicas < *deploymentSpec.Replicas {
			msg := fmt.Sprintf("Waiting for Deployment rollout to finish: %d out of %d new replicas have been updated...", deploymentStatus.UpdatedReplicas, *deploymentSpec.Replicas)
			return false, "Progressing", msg
		} else if deploymentStatus.Replicas > deploymentStatus.UpdatedReplicas {
			msg := fmt.Sprintf("Waiting for Deployment rollout to finish: %d old replicas are pending termination...", deploymentStatus.Replicas-deploymentStatus.UpdatedReplicas)
			return false, "Progressing", msg
		} else if deploymentStatus.AvailableReplicas < deploymentStatus.UpdatedReplicas {
			msg := fmt.Sprintf("Waiting for Deployment rollout to finish: %d of %d updated replicas are available...", deploymentStatus.AvailableReplicas, deploymentStatus.UpdatedReplicas)
			return false, "Progressing", msg
		}
	} else {
		msg := "Waiting for Deployment rollout to finish: observed deployment generation less than desired generation"
		return false, "Progressing", msg
	}

	return true, "", ""
}

// TODO: could pass in the values instead of recalculating them
func (r *NumaflowControllerRolloutReconciler) processNumaflowControllerStatus(controllerRollout *apiv1.NumaflowControllerRollout, deployment *appsv1.Deployment) error {
	healthy, conditionReason, conditionMsg := processDeploymentHealth(deployment)

	if healthy {
		controllerRollout.Status.MarkChildResourcesHealthy(controllerRollout.Generation)
	} else {
		controllerRollout.Status.MarkChildResourcesUnhealthy(conditionReason, conditionMsg, controllerRollout.Generation)
	}

	return nil
}

func (r *NumaflowControllerRolloutReconciler) markRolloutPaused(ctx context.Context, rolloutNamespace string, rolloutName string, paused bool) error {
	rollout := &apiv1.NumaflowControllerRollout{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Namespace: rolloutNamespace, Name: rolloutName}, rollout); err != nil {
		return err
	}

	if paused {
		rollout.Status.MarkPausingPipelines(rollout.Generation)
	} else {
		rollout.Status.MarkUnpausingPipelines(rollout.Generation)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NumaflowControllerRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	controller, err := runtimecontroller.New(ControllerNumaflowControllerRollout, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch NumaflowControllerRollouts
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowControllerRollout{}), &handler.EnqueueRequestForObject{}, predicate.GenerationChangedPredicate{}); err != nil {
		return err
	}

	// Watch NumaflowControllerRollout child resources: numaflow-controller Deployment, ConfigMap, ServiceAccount, Role, RoleBinding
	for _, kind := range []client.Object{&appsv1.Deployment{}, &corev1.ConfigMap{}, &corev1.ServiceAccount{}, &rbacv1.Role{}, &rbacv1.RoleBinding{}} {
		if err := controller.Watch(source.Kind(mgr.GetCache(), kind),
			handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &apiv1.NumaflowControllerRollout{}, handler.OnlyControllerOwner()),
			predicate.ResourceVersionChangedPredicate{}); err != nil {
			return err
		}
	}

	return nil
}

// SplitYAMLToString splits a YAML file into strings. Returns list of yamls
// found in the yaml. If an error occurs, returns objects that have been parsed so far too.
func SplitYAMLToString(yamlData []byte) ([]string, error) {
	d := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlData), 4096)
	var objs []string
	for {
		ext := runtime.RawExtension{}
		if err := d.Decode(&ext); err != nil {
			if err == io.EOF {
				break
			}
			return objs, fmt.Errorf("failed to unmarshal manifest: %v", err)
		}
		ext.Raw = bytes.TrimSpace(ext.Raw)
		if len(ext.Raw) == 0 || bytes.Equal(ext.Raw, []byte("null")) {
			continue
		}
		objs = append(objs, string(ext.Raw))
	}
	return objs, nil
}

func toUnstructuredAndApplyLabel(manifests []string, name string) ([]*unstructured.Unstructured, error) {
	uns := make([]*unstructured.Unstructured, 0)
	for _, m := range manifests {
		obj := make(map[string]interface{})
		err := yaml.Unmarshal([]byte(m), &obj)
		if err != nil {
			return nil, err
		}
		target := &unstructured.Unstructured{Object: obj}
		err = kubernetes.SetLabel(target, common.LabelKeyNumaplaneInstance, name)
		if err != nil {
			return nil, err
		}
		uns = append(uns, target)
	}
	return uns, nil
}

func (r *NumaflowControllerRolloutReconciler) updateNumaflowControllerRolloutStatus(ctx context.Context, controllerRollout *apiv1.NumaflowControllerRollout) error {
	rawSpec := runtime.RawExtension{}
	err := util.StructToStruct(&controllerRollout.Spec, &rawSpec)
	if err != nil {
		return fmt.Errorf("unable to convert NumaflowControllerRollout Spec to GenericObject Spec: %v", err)
	}

	rawStatus := runtime.RawExtension{}
	err = util.StructToStruct(&controllerRollout.Status, &rawStatus)
	if err != nil {
		return fmt.Errorf("unable to convert NumaflowControllerRollout Status to GenericObject Status: %v", err)
	}

	obj := kubernetes.GenericObject{
		TypeMeta:   controllerRollout.TypeMeta,
		ObjectMeta: controllerRollout.ObjectMeta,
		Spec:       rawSpec,
		Status:     rawStatus,
	}

	return kubernetes.UpdateStatus(ctx, r.restConfig, &obj, "numaflowcontrollerrollouts")
}

func (r *NumaflowControllerRolloutReconciler) updateNumaflowControllerRolloutStatusToFailed(ctx context.Context, controllerRollout *apiv1.NumaflowControllerRollout, err error) error {
	controllerRollout.Status.MarkFailed(err.Error())
	return r.updateNumaflowControllerRolloutStatus(ctx, controllerRollout)
}

func (r *NumaflowControllerRolloutReconciler) ErrorHandler(numaflowControllerRollout *apiv1.NumaflowControllerRollout, err error, reason, msg string) {
	r.customMetrics.NumaflowControllersSyncFailed.WithLabelValues().Inc()
	r.recorder.Eventf(numaflowControllerRollout, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}
