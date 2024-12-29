/*
Copyright 2024.

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

package numaflowcontroller

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"strings"
	"time"

	"github.com/argoproj/gitops-engine/pkg/diff"
	gitopsSync "github.com/argoproj/gitops-engine/pkg/sync"
	gitopsSyncCommon "github.com/argoproj/gitops-engine/pkg/sync/common"
	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	kubeUtil "github.com/argoproj/gitops-engine/pkg/utils/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/sync"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerNumaflowController       = "numaflowcontroller-controller"
	NumaflowControllerDeploymentName   = "numaflow-controller"
	DefaultNumaflowControllerImageName = "numaflow"
)

// NumaflowControllerReconciler reconciles a NumaflowController object
type NumaflowControllerReconciler struct {
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

func NewNumaflowControllerReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	rawConfig *rest.Config,
	kubectl kubeUtil.Kubectl,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) (*NumaflowControllerReconciler, error) {
	stateCache := sync.NewLiveStateCache(rawConfig, customMetrics)
	numaLogger := logger.GetBaseLogger().WithValues("numaflowcontroller")
	err := stateCache.Init(numaLogger)
	if err != nil {
		return nil, err
	}

	kubectl.SetOnKubectlRun(func(command string) (kubeUtil.CleanupFunc, error) {
		customMetrics.NumaflowControllerKubectlExecutionCounter.WithLabelValues().Inc()
		return func() {}, nil
	})
	restConfig := rawConfig

	return &NumaflowControllerReconciler{
		client,
		scheme,
		restConfig,
		rawConfig,
		kubectl,
		stateCache,
		customMetrics,
		recorder,
	}, nil
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *NumaflowControllerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	syncStartTime := time.Now()
	numaLogger := logger.GetBaseLogger().WithName("numaflowcontroller-reconciler").WithValues("numaflowcontroller", req.NamespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	ctx = logger.WithLogger(ctx, numaLogger)
	r.customMetrics.NumaflowControllerSyncs.WithLabelValues().Inc()

	numaflowController := &apiv1.NumaflowController{}
	if err := r.client.Get(ctx, req.NamespacedName, numaflowController); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			r.ErrorHandler(numaflowController, err, "GetNumaflowControllerFailed", "Failed to get NumaflowController")
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	numaflowControllerOrig := numaflowController
	numaflowController = numaflowControllerOrig.DeepCopy()

	numaflowController.Status.Init(numaflowController.Generation)

	result, err := r.reconcile(ctx, numaflowController, req.Namespace, syncStartTime)
	if err != nil {
		r.ErrorHandler(numaflowController, err, "ReconcileFailed", "Failed to reconcile NumaflowController")
		statusUpdateErr := r.updateNumaflowControllerStatusToFailed(ctx, numaflowController, err)
		if statusUpdateErr != nil {
			r.ErrorHandler(numaflowController, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of NumaflowController")
			return ctrl.Result{}, statusUpdateErr
		}
		return ctrl.Result{}, err
	}

	deployment, _, err := r.getNumaflowControllerDeployment(ctx, numaflowController)
	if err != nil {
		r.recorder.Eventf(numaflowController, corev1.EventTypeWarning, "GetDeploymentFailed", "Failed to get NumaflowController associated Deployment: %v", err.Error())
		return ctrl.Result{}, err
	}

	// update our Status with the Deployment's Status
	err = r.processNumaflowControllerDeploymentStatus(numaflowController, deployment)
	if err != nil {
		r.recorder.Eventf(numaflowController, corev1.EventTypeWarning, "ProcessStatusFailed", "Failed to process NumaflowController Deployment status: %v", err.Error())
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(numaflowControllerOrig, numaflowController) {
		numaflowControllerStatus := numaflowController.Status
		if err := r.client.Update(ctx, numaflowController); err != nil {
			r.ErrorHandler(numaflowController, err, "UpdateFailed", "Failed to update NumaflowController")
			statusUpdateErr := r.updateNumaflowControllerStatusToFailed(ctx, numaflowController, err)
			if statusUpdateErr != nil {
				r.ErrorHandler(numaflowController, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of NumaflowController")
				return ctrl.Result{}, statusUpdateErr
			}
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		numaflowController.Status = numaflowControllerStatus
	}

	// Update the Status subresource
	if numaflowController.DeletionTimestamp.IsZero() { // would've already been deleted
		statusUpdateErr := r.updateNumaflowControllerStatus(ctx, numaflowController)
		if statusUpdateErr != nil {
			r.ErrorHandler(numaflowController, statusUpdateErr, "UpdateStatusFailed", "Failed to update status of NumaflowController")
			return ctrl.Result{}, statusUpdateErr
		}
	}

	numaLogger.Debug("reconciliation successful")
	r.recorder.Eventf(numaflowController, corev1.EventTypeNormal, "ReconcileSuccess", "Reconciliation successful")
	return result, nil
}

func (r *NumaflowControllerReconciler) needsUpdate(old, new *apiv1.NumaflowController) bool {

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
func (r *NumaflowControllerReconciler) reconcile(
	ctx context.Context,
	controller *apiv1.NumaflowController,
	namespace string,
	syncStartTime time.Time,
) (ctrl.Result, error) {
	numaLogger := logger.FromContext(ctx)

	defer func() {
		if controller.Status.IsHealthy() {
			r.customMetrics.NumaflowControllersHealth.WithLabelValues(controller.Namespace, controller.Name).Set(1)
		} else {
			r.customMetrics.NumaflowControllersHealth.WithLabelValues(controller.Namespace, controller.Name).Set(0)
		}
	}()

	if !controller.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting NumaflowController")
		r.recorder.Eventf(controller, corev1.EventTypeNormal, "Deleting", "Deleting NumaflowController")
		if controllerutil.ContainsFinalizer(controller, common.FinalizerName) {
			// Check if dependent resources are deleted, if not then requeue after 5 seconds
			if !r.areDependentResourcesDeleted(ctx, controller) {
				numaLogger.Warn("Dependent resources are not deleted yet, requeue after 5 seconds")
				return ctrl.Result{Requeue: true}, nil
			}
			err := r.deleteChildren(ctx, controller.Namespace, controller.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(controller, common.FinalizerName)
		}

		// generate the metrics for the numaflow controller deletion
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowController, "delete").Observe(time.Since(syncStartTime).Seconds())
		r.customMetrics.NumaflowControllersHealth.DeleteLabelValues(controller.Namespace, controller.Name)
		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CR is deleted
	if !controllerutil.ContainsFinalizer(controller, common.FinalizerName) {
		controllerutil.AddFinalizer(controller, common.FinalizerName)
	}

	// TODO: we need to be in "Pending" if we haven't deployed yet

	_, deploymentExists, err := r.getNumaflowControllerDeployment(ctx, controller)
	if err != nil {
		return ctrl.Result{}, err
	}

	// apply controller - this handles syncing in the cases in which our Controller  isn't updating
	// (note that the cases above in which it is updating have a 'return' statement):
	// - new Controller
	// - auto healing
	// - somebody changed the manifest associated with the Controller version (shouldn't happen but could)
	phase, needsRequeue, err := r.sync(ctx, controller, namespace, numaLogger)
	if err != nil {
		return ctrl.Result{}, err
	}
	if needsRequeue {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if phase != gitopsSyncCommon.OperationSucceeded {
		return ctrl.Result{}, fmt.Errorf("sync operation is not successful")
	}

	// Generate the creation metrics only if the numaflow controller is newly created
	if !deploymentExists {
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowController, "create").Observe(time.Since(syncStartTime).Seconds())
	}

	return ctrl.Result{}, nil
}

func resolveManifestTemplate(manifest string, controller *apiv1.NumaflowController) ([]byte, error) {
	if controller == nil {
		return []byte(manifest), nil
	}

	tmpl, err := template.New("manifest").Parse(manifest)
	if err != nil {
		return nil, fmt.Errorf("unable to parse manifest: %v", err)
	}

	instanceID := controller.Spec.InstanceID
	instanceSuffix := ""
	if strings.TrimSpace(instanceID) != "" {
		instanceSuffix = fmt.Sprintf("-%s", instanceID)
	}

	data := struct {
		InstanceSuffix string
		InstanceID     string
	}{
		InstanceSuffix: instanceSuffix,
		InstanceID:     instanceID,
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return nil, fmt.Errorf("unable to apply information to manifest: %v", err)
	}

	return buf.Bytes(), nil
}

/*
determineTargetObjects processes the manifests for a given NumaflowController and version
returning a list of target objects representing the NumaflowController's children for the given version.

Parameters:
  - controller: A pointer to the NumaflowController object containing the controller definition.
  - version: A string representing the version of the controller definition to be used.

Returns:
  - A slice of pointers to unstructured.Unstructured objects representing the target manifests.
  - An error if any step in processing the manifests fails.
*/
func determineTargetObjects(
	controller *apiv1.NumaflowController,
	version string,
	namespace string,
) ([]*unstructured.Unstructured, error) {

	// Get the target manifests based on the given version and throw an error if the definition does not have that version
	definition := config.GetConfigManagerInstance().GetControllerDefinitionsMgr().GetNumaflowControllerDefinitionsConfig()
	manifest, manifestExists := definition[version]
	if !manifestExists {
		return nil, fmt.Errorf("no controller definition found for version %s", version)
	}

	// Update templated manifest with information from the NumaflowController definition
	manifestBytes, err := resolveManifestTemplate(manifest, controller)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve manifest: %w", err)
	}

	// Applying ownership reference
	manifests, err := SplitYAMLToString(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("can not parse file data, err: %w", err)
	}
	targetObjs, err := toLabeledUnstructured(manifests, controller.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the manifest, %w", err)
	}
	for _, obj := range targetObjs {
		obj.SetNamespace(namespace)
	}

	return targetObjs, nil
}

func (r *NumaflowControllerReconciler) sync(
	ctx context.Context,
	controller *apiv1.NumaflowController,
	namespace string,
	numaLogger *logger.NumaLogger,
) (gitopsSyncCommon.OperationPhase, bool, error) {

	// get the manifests that should be applied for the current version in the NumaflowController
	newVersion := controller.Spec.Version
	newVersionTargetObjs, err := determineTargetObjects(controller, newVersion, namespace)
	if err != nil {
		return gitopsSyncCommon.OperationError, false, fmt.Errorf("unable to determine the target objects for the new version %s: %w", newVersion, err)
	}

	numaLogger.Debugf("found %d target objects associated with NumaflowController version %s", len(newVersionTargetObjs), newVersion)

	// determine if there's a difference between what should be applied and what is live
	//reconciliationResult, diffResults, liveObjectsMap, err := r.compareState(controller, namespace, newVersionTargetObjs, numaLogger)
	reconciliationResult, diffResults, _, err := r.compareState(controller, namespace, newVersionTargetObjs, numaLogger)
	if err != nil {
		return gitopsSyncCommon.OperationError, false, err
	}

	// delete current resources if any of the specs differ, before applying the new ones (this can take care of issues where "apply" doesn't work)
	/*if diffResults.Modified {
		numaLogger.Debugf("detecting a difference between target and live specs; number of objects remaining to delete first=%d", len(liveObjectsMap))

		// see if we still need to delete some of the resources beforehand
		childResourcesNeedToBeDeleted := len(liveObjectsMap) > 0
		if childResourcesNeedToBeDeleted {
			numaLogger.Debugf("current NumaflowController resources differs from desired")

			err := r.deleteNumaflowControllerChildren(ctx, liveObjectsMap, namespace)
			if err != nil {
				return gitopsSyncCommon.OperationError, false, fmt.Errorf("error deleting NumaflowController child resources: %w", err)
			}

			return gitopsSyncCommon.OperationRunning, true, nil
		}
	}*/

	opts := []gitopsSync.SyncOpt{
		gitopsSync.WithLogr(*numaLogger.LogrLogger),
		gitopsSync.WithOperationSettings(false, true, true, false),
		gitopsSync.WithManifestValidation(true),
		gitopsSync.WithPruneLast(false),
		gitopsSync.WithResourceModificationChecker(true, diffResults),
		gitopsSync.WithReplace(true),
		//gitopsSync.WithServerSideApply(true),
		//gitopsSync.WithServerSideApplyManager(common.SSAManager),
	}

	clusterCache, err := r.stateCache.GetClusterCache()
	if err != nil {
		return gitopsSyncCommon.OperationError, false, err
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
		return gitopsSyncCommon.OperationError, false, err
	}

	// now do an Apply if there's anything that needs to be changed
	syncCtx.Sync()

	controller.Status.MarkDeployed(controller.Generation)

	phase, _, _ := syncCtx.GetState()
	return phase, false, nil
}

// compareState compares with desired state of the objects with the live state in the cluster
// for the target objects.
func (r *NumaflowControllerReconciler) compareState(
	controller *apiv1.NumaflowController,
	namespace string,
	targetObjs []*unstructured.Unstructured,
	numaLogger *logger.NumaLogger,
) (gitopsSync.ReconciliationResult, *diff.DiffResultList, map[kubeUtil.ResourceKey]*unstructured.Unstructured, error) {
	var infoProvider kubeUtil.ResourceInfoProvider
	clusterCache, err := r.stateCache.GetClusterCache()
	if err != nil {
		return gitopsSync.ReconciliationResult{}, nil, nil, err
	}
	infoProvider = clusterCache
	liveObjByKey, err := r.stateCache.GetManagedLiveObjsFromResourceList(controller.Name, namespace, targetObjs)
	if err != nil {
		return gitopsSync.ReconciliationResult{}, nil, nil, err
	}
	// clone liveObjByKey because the call to Reconcile() below will clear it
	liveObjByKeyClone := make(map[kube.ResourceKey]*unstructured.Unstructured)
	for k, v := range liveObjByKey {
		liveObjByKeyClone[k] = v
	}
	reconciliationResult := gitopsSync.Reconcile(targetObjs, liveObjByKey, namespace, infoProvider)
	// Ignore `status` field for all comparison.
	// TODO: make it configurable
	overrides := map[string]sync.ResourceOverride{
		"*/*": {
			IgnoreDifferences: sync.OverrideIgnoreDiff{JSONPointers: []string{"/status"}}},
	}

	diffOpts := []diff.Option{
		diff.WithLogr(*numaLogger.LogrLogger),
		diff.WithServerSideDiff(false),
		diff.WithManager(common.SSAManager),
		diff.WithGVKParser(clusterCache.GetGVKParser()),
	}

	diffResults, err := sync.StateDiffs(reconciliationResult.Target, reconciliationResult.Live, overrides, diffOpts)
	if err != nil {
		return reconciliationResult, nil, liveObjByKeyClone, err
	}

	return reconciliationResult, diffResults, liveObjByKeyClone, nil
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
func (r *NumaflowControllerReconciler) getNumaflowControllerDeployment(ctx context.Context, controller *apiv1.NumaflowController) (*appsv1.Deployment, bool, error) {
	instanceID := controller.Spec.InstanceID
	numaflowControllerDeploymentName := NumaflowControllerDeploymentName
	if strings.TrimSpace(instanceID) != "" {
		numaflowControllerDeploymentName = fmt.Sprintf("%s-%s", NumaflowControllerDeploymentName, instanceID)
	}

	deployment := &appsv1.Deployment{}
	if err := r.client.Get(ctx, k8stypes.NamespacedName{Namespace: controller.Namespace, Name: numaflowControllerDeploymentName}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		} else {
			return nil, false, err
		}
	}
	return deployment, true, nil
}

func processDeploymentHealth(deployment *appsv1.Deployment) (bool, string, string) {

	if deployment == nil {
		msg := "Numaflow Controller Deployment not found"
		return false, apiv1.ProgressingReasonString, msg
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
			msg := fmt.Sprintf("Waiting for Deployment to finish: %d out of %d new replicas have been updated...", deploymentStatus.UpdatedReplicas, *deploymentSpec.Replicas)
			return false, apiv1.ProgressingReasonString, msg
		} else if deploymentStatus.Replicas > deploymentStatus.UpdatedReplicas {
			msg := fmt.Sprintf("Waiting for Deployment to finish: %d old replicas are pending termination...", deploymentStatus.Replicas-deploymentStatus.UpdatedReplicas)
			return false, apiv1.ProgressingReasonString, msg
		} else if deploymentStatus.AvailableReplicas < deploymentStatus.UpdatedReplicas {
			msg := fmt.Sprintf("Waiting for Deployment to finish: %d of %d updated replicas are available...", deploymentStatus.AvailableReplicas, deploymentStatus.UpdatedReplicas)
			return false, apiv1.ProgressingReasonString, msg
		}
	} else {
		msg := "Waiting for Deployment to finish: observed deployment generation less than desired generation"
		return false, apiv1.ProgressingReasonString, msg
	}

	return true, "", ""
}

// TODO: could pass in the values instead of recalculating them
func (r *NumaflowControllerReconciler) processNumaflowControllerDeploymentStatus(controller *apiv1.NumaflowController, deployment *appsv1.Deployment) error {
	healthy, conditionReason, conditionMsg := processDeploymentHealth(deployment)

	if healthy {
		controller.Status.MarkChildResourcesHealthy(controller.Generation)
	} else {
		controller.Status.MarkChildResourcesUnhealthy(conditionReason, conditionMsg, controller.Generation)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NumaflowControllerReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {

	numaLogger := logger.FromContext(ctx).WithName("numaflowcontroller-reconciler")

	controller, err := runtimecontroller.New(ControllerNumaflowController, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	// Watch for changes to primary resource NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowController{},
		&handler.TypedEnqueueRequestForObject[*apiv1.NumaflowController]{}, ctlrcommon.TypedGenerationChangedPredicate[*apiv1.NumaflowController]{})); err != nil {
		return fmt.Errorf("failed to watch NumaflowController: %w", err)
	}

	// Perform Watch on child resources, using the label applied to each child which identifies which NumaflowController needs to be reconciled
	// reference: https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/handler/example_test.go

	// Watch for changes to secondary resources(Deployment) so we can requeue the owner NumaflowController
	err = controller.Watch(source.Kind(mgr.GetCache(), &appsv1.Deployment{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, a *appsv1.Deployment) []reconcile.Request {
			ownerName, found := a.Labels[common.LabelKeyNumaplaneInstance]
			if found && ownerName != "" {
				numaLogger.Debugf("found Deployment labeled by %s/%s", a.Namespace, ownerName)
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      ownerName,
						Namespace: a.Namespace,
					}},
				}
			}
			return nil
		}),
	))

	if err != nil {
		return fmt.Errorf("failed to watch Deployment: %w", err)
	}

	// Watch for changes to secondary resources(ConfigMap) so we can requeue the owner NumaflowController
	err = controller.Watch(source.Kind(mgr.GetCache(), &corev1.ConfigMap{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, a *corev1.ConfigMap) []reconcile.Request {
			ownerName, found := a.Labels[common.LabelKeyNumaplaneInstance]
			if found && ownerName != "" {
				numaLogger.Debugf("found ConfigMap labeled by %s/%s", a.Namespace, ownerName)
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      ownerName,
						Namespace: a.Namespace,
					}},
				}
			}
			return nil
		}),
	))
	if err != nil {
		return fmt.Errorf("failed to watch ConfigMap: %w", err)
	}

	// Watch for changes to secondary resources(ServiceAccount) so we can requeue the owner NumaflowController
	err = controller.Watch(source.Kind(mgr.GetCache(), &corev1.ServiceAccount{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, a *corev1.ServiceAccount) []reconcile.Request {
			ownerName, found := a.Labels[common.LabelKeyNumaplaneInstance]
			if found && ownerName != "" {
				numaLogger.Debugf("found ServiceAccount labeled by %s/%s", a.Namespace, ownerName)
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      ownerName,
						Namespace: a.Namespace,
					}},
				}
			}
			return nil
		}),
	))
	if err != nil {
		return fmt.Errorf("failed to watch ServiceAccount: %w", err)
	}

	// Watch for changes to secondary resources(Role) so we can requeue the owner NumaflowController
	err = controller.Watch(source.Kind(mgr.GetCache(), &rbacv1.Role{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, a *rbacv1.Role) []reconcile.Request {
			ownerName, found := a.Labels[common.LabelKeyNumaplaneInstance]
			if found && ownerName != "" {
				numaLogger.Debugf("found Role labeled by %s/%s", a.Namespace, ownerName)
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      ownerName,
						Namespace: a.Namespace,
					}},
				}
			}
			return nil
		}),
	))
	if err != nil {
		return fmt.Errorf("failed to watch Role: %w", err)
	}

	// Watch for changes to secondary resources(RoleBinding) so we can requeue the owner NumaflowController
	err = controller.Watch(source.Kind(mgr.GetCache(), &rbacv1.RoleBinding{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, a *rbacv1.RoleBinding) []reconcile.Request {
			ownerName, found := a.Labels[common.LabelKeyNumaplaneInstance]
			if found && ownerName != "" {
				numaLogger.Debugf("found RoleBinding labeled by %s/%s", a.Namespace, ownerName)
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      ownerName,
						Namespace: a.Namespace,
					}},
				}
			}
			return nil
		}),
	))
	if err != nil {
		return fmt.Errorf("failed to watch RoleBinding: %w", err)
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

func toLabeledUnstructured(manifests []string, name string) ([]*unstructured.Unstructured, error) {
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

func (r *NumaflowControllerReconciler) updateNumaflowControllerStatus(ctx context.Context, controller *apiv1.NumaflowController) error {
	return r.client.Status().Update(ctx, controller)
}

func (r *NumaflowControllerReconciler) updateNumaflowControllerStatusToFailed(ctx context.Context, controller *apiv1.NumaflowController, err error) error {
	controller.Status.MarkFailed(err.Error())
	return r.updateNumaflowControllerStatus(ctx, controller)
}

func (r *NumaflowControllerReconciler) ErrorHandler(numaflowController *apiv1.NumaflowController, err error, reason, msg string) {
	r.customMetrics.NumaflowControllerSyncErrors.WithLabelValues().Inc()
	r.recorder.Eventf(numaflowController, corev1.EventTypeWarning, reason, msg+" %v", err.Error())
}

// areDependentResourcesDeleted checks if dependent resources are deleted.
func (r *NumaflowControllerReconciler) areDependentResourcesDeleted(ctx context.Context, controller *apiv1.NumaflowController) bool {
	pipelineList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "pipelines",
		controller.Namespace, common.LabelKeyParentRollout, "")
	if err != nil {
		return false
	}
	monoVertexList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "monovertices",
		controller.Namespace, common.LabelKeyParentRollout, "")
	if err != nil {
		return false
	}
	isbServiceList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, "interstepbufferservices",
		controller.Namespace, common.LabelKeyParentRollout, "")
	if err != nil {
		return false
	}
	if len(pipelineList.Items)+len(monoVertexList.Items)+len(isbServiceList.Items) == 0 {
		return true
	}

	return false
}

func (r *NumaflowControllerReconciler) deleteChildren(ctx context.Context, namespace string, name string) error {
	//numaLogger := logger.FromContext(ctx)

	liveObjByKey, err := r.stateCache.GetManagedLiveObjects(name, namespace)
	if err != nil {
		return err
	}
	toUnstructuredMap := make(map[kubeUtil.ResourceKey]*unstructured.Unstructured)
	for key, obj := range liveObjByKey {
		toUnstructuredMap[key] = obj.Resource
	}

	return r.deleteNumaflowControllerChildren(ctx, toUnstructuredMap, namespace)
}

// deleteNumaflowControllerChildren deletes child resources associated with a NumaflowController
// for a specified version and namespace. It determines the target objects to delete and attempts
// to remove them using the Kubernetes client. If a resource is not found, it continues with the
// next one. Returns an error if unable to determine target objects or delete a resource.
func (r *NumaflowControllerReconciler) deleteNumaflowControllerChildren(
	ctx context.Context,
	// controller *apiv1.NumaflowController,
	// currentVersion string,
	liveObjectsMap map[kubeUtil.ResourceKey]*unstructured.Unstructured,
	namespace string,
	// newVersionTargetObjs []*unstructured.Unstructured,
) error {

	numaLogger := logger.FromContext(ctx)

	// TODO: instead of using the client to delete the child resources, try using the gitops-engine if possible
	deletionGracePeriod := int64(0)
	for _, obj := range liveObjectsMap {
		obj.SetNamespace(namespace)
		err := r.client.Delete(ctx, obj, &client.DeleteOptions{
			GracePeriodSeconds: &deletionGracePeriod,
			// PropagationPolicy: v1.DeletePropagationOrphan, // TODO: should we change the default propagation policy?
		})
		if err != nil {
			if apierrors.IsNotFound(err) {

				numaLogger.Debugf("cannot delete %s/%s/%s as it doesn't exist", obj.GetKind(), obj.GetNamespace(), obj.GetName())
				continue
			} else {
				return fmt.Errorf("unable to delete NumaflowController child resource %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
			}
		} else {
			numaLogger.Debugf("successfully deleted %s/%s/%s", obj.GetKind(), obj.GetNamespace(), obj.GetName())
		}
	}

	return nil
}
