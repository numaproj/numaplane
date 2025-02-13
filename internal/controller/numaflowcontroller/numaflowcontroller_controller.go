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
		r.customMetrics.SetNumaflowControllersHealth(controller.Namespace, controller.Name, string(controller.Status.Phase))
	}()

	if !controller.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting NumaflowController")
		r.recorder.Eventf(controller, corev1.EventTypeNormal, "Deleting", "Deleting NumaflowController")
		if controllerutil.ContainsFinalizer(controller, common.FinalizerName) {
			// Set the foreground deletion policy so that we will block for children to be cleaned up for any type of deletion action
			// foreground := metav1.DeletePropagationForeground
			// if err := r.client.Delete(ctx, controller, &client.DeleteOptions{PropagationPolicy: &foreground}); err != nil {
			// 	return ctrl.Result{}, err
			// }
			// Check if dependent resources are deleted, if not then requeue
			if ok, err := r.areDependentResourcesDeleted(ctx, controller); !ok || err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to delete dependent resources: %v", err)
			}
			controllerutil.RemoveFinalizer(controller, common.FinalizerName)
		}

		// generate the metrics for the numaflow controller deletion
		r.customMetrics.ReconciliationDuration.WithLabelValues(ControllerNumaflowController, "delete").Observe(time.Since(syncStartTime).Seconds())
		r.customMetrics.DeleteNumaflowControllersHealth(controller.Namespace, controller.Name)
		return ctrl.Result{}, nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(controller, common.FinalizerName) {
		controllerutil.AddFinalizer(controller, common.FinalizerName)
	}

	_, deploymentExists, err := r.getNumaflowControllerDeployment(ctx, controller)
	if err != nil {
		return ctrl.Result{}, err
	}

	newVersion := controller.Spec.Version
	newVersionTargetObjs, err := determineTargetObjects(controller, newVersion, namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("unable to determine the target objects for the new version %s: %w", newVersion, err)
	}

	numaLogger.Debugf("found %d target objects associated with NumaflowController version %s", len(newVersionTargetObjs), newVersion)

	// apply controller - this handles syncing in the cases in which our Controller  isn't updating
	// (note that the cases above in which it is updating have a 'return' statement):
	// - new Controller
	// - auto healing
	// - somebody changed the manifest associated with the Controller version (shouldn't happen but could)
	phase, err := r.sync(controller, namespace, numaLogger, newVersionTargetObjs)
	if err != nil {
		return ctrl.Result{}, err
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

// applyOwnershipToManifests Applies NumaflowController ownership to
// Kubernetes manifests, returning modified manifests or an error.
func applyOwnershipToManifests(manifests []string, controller *apiv1.NumaflowController) ([]string, error) {
	manifestsWithOwnership := make([]string, 0, len(manifests))
	for _, v := range manifests {
		reference, err := applyOwnership(v, controller)
		if err != nil {
			return nil, err
		}
		manifestsWithOwnership = append(manifestsWithOwnership, string(reference))
	}
	return manifestsWithOwnership, nil
}

func applyOwnership(manifest string, controller *apiv1.NumaflowController) ([]byte, error) {
	// Decode YAML into an Unstructured object
	decUnstructured := yamlserializer.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	_, _, err := decUnstructured.Decode([]byte(manifest), nil, obj)
	if err != nil {
		return nil, err
	}

	// Construct the new owner reference
	ownerRef := map[string]interface{}{
		"apiVersion":         controller.APIVersion,
		"kind":               controller.Kind,
		"name":               controller.Name,
		"uid":                string(controller.UID),
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
	manifest, err := config.GetConfigManagerInstance().GetControllerDefinitionsMgr().GetNumaflowControllerDefinitionsConfig(namespace, version)
	if err != nil {
		return nil, fmt.Errorf("unable to get controller definition: %w", err)
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
	manifestsWithOwnership, err := applyOwnershipToManifests(manifests, controller)
	if err != nil {
		return nil, fmt.Errorf("failed to apply ownership reference, %w", err)
	}

	targetObjs, err := toUnstructuredAndApplyLabel(manifestsWithOwnership, controller.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the manifest, %w", err)
	}

	for _, obj := range targetObjs {
		obj.SetNamespace(namespace)
	}

	return targetObjs, nil
}

func (r *NumaflowControllerReconciler) sync(
	controller *apiv1.NumaflowController,
	namespace string,
	numaLogger *logger.NumaLogger,
	targetObjs []*unstructured.Unstructured,
) (gitopsSyncCommon.OperationPhase, error) {

	reconciliationResult, diffResults, err := r.compareState(controller, namespace, targetObjs, numaLogger)
	if err != nil {
		return gitopsSyncCommon.OperationError, err
	}

	opts := []gitopsSync.SyncOpt{
		gitopsSync.WithLogr(*numaLogger.LogrLogger),
		gitopsSync.WithOperationSettings(false, true, true, false),
		gitopsSync.WithManifestValidation(true),
		gitopsSync.WithPruneLast(false),
		gitopsSync.WithResourceModificationChecker(true, diffResults),
		gitopsSync.WithReplace(true),
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

	controller.Status.MarkDeployed(controller.Generation)

	phase, _, _ := syncCtx.GetState()
	return phase, nil
}

// compareState compares with desired state of the objects with the live state in the cluster
// for the target objects.
func (r *NumaflowControllerReconciler) compareState(
	controller *apiv1.NumaflowController,
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
	liveObjByKey, err := r.stateCache.GetManagedLiveObjs(controller.Name, namespace, targetObjs)
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

	diffOpts := []diff.Option{
		diff.WithLogr(*numaLogger.LogrLogger),
		diff.WithServerSideDiff(false),
		diff.WithGVKParser(clusterCache.GetGVKParser()),
	}

	diffResults, err := sync.StateDiffs(reconciliationResult.Target, reconciliationResult.Live, overrides, diffOpts)
	if err != nil {
		return reconciliationResult, nil, err
	}

	return reconciliationResult, diffResults, nil
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
func (r *NumaflowControllerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	controller, err := runtimecontroller.New(ControllerNumaflowController, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	// Watch for changes to primary resource NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowController{},
		&handler.TypedEnqueueRequestForObject[*apiv1.NumaflowController]{}, ctlrcommon.TypedGenerationChangedPredicate[*apiv1.NumaflowController]{})); err != nil {
		return fmt.Errorf("failed to watch NumaflowController: %w", err)
	}

	// Watch for changes to secondary resources(Deployment) so we can requeue the owner NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &appsv1.Deployment{},
		handler.TypedEnqueueRequestForOwner[*appsv1.Deployment](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowController{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*appsv1.Deployment]{})); err != nil {
		return fmt.Errorf("failed to watch Deployment: %w", err)
	}

	// Watch for changes to secondary resources(ConfigMap) so we can requeue the owner NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &corev1.ConfigMap{},
		handler.TypedEnqueueRequestForOwner[*corev1.ConfigMap](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowController{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*corev1.ConfigMap]{})); err != nil {
		return fmt.Errorf("failed to watch ConfigMap: %w", err)
	}

	// Watch for changes to secondary resources(ServiceAccount) so we can requeue the owner NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &corev1.ServiceAccount{},
		handler.TypedEnqueueRequestForOwner[*corev1.ServiceAccount](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowController{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*corev1.ServiceAccount]{})); err != nil {
		return fmt.Errorf("failed to watch ServiceAccount: %w", err)
	}

	// Watch for changes to secondary resources(Role) so we can requeue the owner NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &rbacv1.Role{},
		handler.TypedEnqueueRequestForOwner[*rbacv1.Role](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowController{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*rbacv1.Role]{})); err != nil {
		return fmt.Errorf("failed to watch Role: %w", err)
	}

	// Watch for changes to secondary resources(RoleBinding) so we can requeue the owner NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &rbacv1.RoleBinding{},
		handler.TypedEnqueueRequestForOwner[*rbacv1.RoleBinding](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowController{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*rbacv1.RoleBinding]{})); err != nil {
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
func (r *NumaflowControllerReconciler) areDependentResourcesDeleted(ctx context.Context, controller *apiv1.NumaflowController) (bool, error) {
	pipelineList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, common.NumaflowPipelineKind,
		controller.Namespace, common.LabelKeyParentRollout, "")
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		pipelineList = &unstructured.UnstructuredList{} // Assume it as an empty list
	}
	monoVertexList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, common.NumaflowMonoVertexKind,
		controller.Namespace, common.LabelKeyParentRollout, "")
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		monoVertexList = &unstructured.UnstructuredList{} // Assume it as an empty list
	}
	isbServiceList, err := kubernetes.ListLiveResource(ctx, common.NumaflowAPIGroup, common.NumaflowAPIVersion, common.NumaflowISBServiceKind,
		controller.Namespace, common.LabelKeyParentRollout, "")
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		isbServiceList = &unstructured.UnstructuredList{} // Assume it as an empty list
	}
	if len(pipelineList.Items)+len(monoVertexList.Items)+len(isbServiceList.Items) == 0 {
		return true, nil
	}

	return false, fmt.Errorf("dependent resources are not deleted")
}
