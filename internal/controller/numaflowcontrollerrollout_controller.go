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
	"encoding/json"
	"fmt"
	"io"

	"github.com/argoproj/gitops-engine/pkg/diff"
	gitopsSync "github.com/argoproj/gitops-engine/pkg/sync"
	gitopsSyncCommon "github.com/argoproj/gitops-engine/pkg/sync/common"
	kubeUtil "github.com/argoproj/gitops-engine/pkg/utils/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	yamlserializer "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
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
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	finalizerName = "numaplane.numaproj.io/numaplane-controller"
)

const (
	ControllerNumaflowControllerRollout = "numaflow-controller-rollout-controller"
	NumaflowControllerDeploymentName    = "numaflow-controller"
)

// NumaflowControllerRolloutReconciler reconciles a NumaflowControllerRollout object
type NumaflowControllerRolloutReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	restConfig *rest.Config
	rawConfig  *rest.Config
	kubectl    kubeUtil.Kubectl
	stateCache sync.LiveStateCache
}

func NewNumaflowControllerRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	rawConfig *rest.Config,
	kubectl kubeUtil.Kubectl,
) (*NumaflowControllerRolloutReconciler, error) {
	stateCache := sync.NewLiveStateCache(rawConfig)
	numaLogger := logger.GetBaseLogger().WithName("state cache").WithValues("numaflowcontrollerrollout")
	err := stateCache.Init(numaLogger)
	if err != nil {
		return nil, err
	}

	restConfig := rawConfig
	return &NumaflowControllerRolloutReconciler{
		client,
		s,
		restConfig,
		rawConfig,
		kubectl,
		stateCache,
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
	numaLogger := logger.GetBaseLogger().WithName("numaflowcontrollerrollout-reconciler").WithValues("numaflowcontrollerrollout", req.NamespacedName)

	// TODO: only allow one controllerRollout per namespace.
	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, numaflowControllerRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			numaLogger.Error(err, "Unable to get NumaflowControllerRollout", "request", req)
			return ctrl.Result{}, err
		}
	}

	// save off a copy of the original before we modify it
	numaflowControllerRolloutOrig := numaflowControllerRollout
	numaflowControllerRollout = numaflowControllerRolloutOrig.DeepCopy()

	numaflowControllerRollout.Status.Init(numaflowControllerRollout.Generation)

	err := r.reconcile(ctx, numaflowControllerRollout, req.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Update the Spec if needed
	if r.needsUpdate(numaflowControllerRolloutOrig, numaflowControllerRollout) {
		numaflowControllerRolloutStatus := numaflowControllerRollout.Status
		if err := r.client.Update(ctx, numaflowControllerRollout); err != nil {
			numaLogger.Error(err, "Error Updating NumaflowControllerRollout", "NumaflowControllerRollout", numaflowControllerRollout)
			return ctrl.Result{}, err
		}
		// restore the original status, which would've been wiped in the previous call to Update()
		numaflowControllerRollout.Status = numaflowControllerRolloutStatus
	}

	// Update the Status subresource
	if numaflowControllerRollout.DeletionTimestamp.IsZero() { // would've already been deleted
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latestNumaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
			if err := r.client.Get(ctx, req.NamespacedName, latestNumaflowControllerRollout); err != nil {
				return err
			}

			latestNumaflowControllerRollout.Status = numaflowControllerRollout.Status
			return r.client.Status().Update(ctx, latestNumaflowControllerRollout)
		})
		if err != nil {
			numaLogger.Error(err, "Error Updating NumaflowControllerRollout Status", "NumaflowControllerRollout", numaflowControllerRollout)
			return ctrl.Result{}, err
		}
	}

	numaLogger.Debug("reconciliation successful")

	return ctrl.Result{}, nil
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
) error {
	numaLogger := logger.FromContext(ctx)

	if !controllerRollout.DeletionTimestamp.IsZero() {
		numaLogger.Info("Deleting NumaflowControllerRollout")
		if controllerutil.ContainsFinalizer(controllerRollout, finalizerName) {
			controllerutil.RemoveFinalizer(controllerRollout, finalizerName)
		}
		return nil
	}

	// add Finalizer so we can ensure that we take appropriate action when CRD is deleted
	if !controllerutil.ContainsFinalizer(controllerRollout, finalizerName) {
		controllerutil.AddFinalizer(controllerRollout, finalizerName)
	}

	// apply controller
	phase, err := r.sync(controllerRollout, namespace, numaLogger)
	if err != nil {
		return err
	}

	if phase != gitopsSyncCommon.OperationSucceeded {
		return fmt.Errorf("sync operation is not successful")
	}

	err = r.processNumaflowControllerStatus(ctx, controllerRollout)
	if err != nil {
		return err
	}

	controllerRollout.Status.MarkDeployed(controllerRollout.Generation)

	return nil
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
	definition := config.GetConfigManagerInstance().GetControllerDefinitionsMgr().GetControllerDefinitionsConfig()
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
		numaLogger.Error(err, "Error on comparing live state")
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
		numaLogger.Error(err, "Error on getting the cluster cache")
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
		numaLogger.Error(err, "Error on creating syncing context")
		return gitopsSyncCommon.OperationError, err
	}

	syncCtx.Sync()

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

func (r *NumaflowControllerRolloutReconciler) processNumaflowControllerStatus(ctx context.Context, controllerRollout *apiv1.NumaflowControllerRollout) error {
	numaLogger := logger.FromContext(ctx)

	// Try to get the Numaflow Controller Deployment
	deplObj := kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      NumaflowControllerDeploymentName,
			Namespace: controllerRollout.Namespace,
		},
	}
	deployment, err := kubernetes.GetCR(ctx, r.restConfig, &deplObj, "deployments")
	if err != nil {
		if apierrors.IsNotFound(err) {
			// This could just mean that the deployment has not yet been created.
			// Log and return to wait for next reconcile call once the deployment is created triggering the reconciliation.
			numaLogger.Warnf("Numaflow Controller Deployment not found. It may still have to be created.")
			return nil
		}

		return fmt.Errorf("error getting the Numaflow Controller Deployment for NumaflowControllerRollout %s/%s: %v", controllerRollout.Namespace, controllerRollout.Name, err)
	}

	// Parse the Deployment spec of the existing Numaflow Controller Deployment
	var deploymentSpec appsv1.DeploymentSpec
	if len(deployment.Spec.Raw) > 0 {
		err = json.Unmarshal(deployment.Spec.Raw, &deploymentSpec)
		if err != nil {
			return fmt.Errorf("unable to parse spec for existing Numaflow Controller Deployment %s/%s: %v", deployment.Namespace, deployment.Name, err)
		}
	}

	// Parse the Deployment status of the existing Numaflow Controller Deployment
	var deploymentStatus appsv1.DeploymentStatus
	if len(deployment.Status.Raw) > 0 {
		err = json.Unmarshal(deployment.Status.Raw, &deploymentStatus)
		if err != nil {
			return fmt.Errorf("unable to parse status for existing Numaflow Controller Deployment %s/%s: %v", deployment.Namespace, deployment.Name, err)
		}
	}

	// Health Check borrowed from argoproj/gitops-engine/pkg/health/health_deployment.go https://github.com/argoproj/gitops-engine/blob/master/pkg/health/health_deployment.go#L27
	if deployment.Generation <= deploymentStatus.ObservedGeneration {
		cond := getDeploymentCondition(deploymentStatus, appsv1.DeploymentProgressing)
		if cond != nil && cond.Reason == "ProgressDeadlineExceeded" {
			msg := fmt.Sprintf("Deployment %q exceeded its progress deadline", deployment.Name)
			controllerRollout.Status.MarkChildResourcesUnhealthy("Degraded", msg, controllerRollout.Generation)
			return nil
		} else if deploymentSpec.Replicas != nil && deploymentStatus.UpdatedReplicas < *deploymentSpec.Replicas {
			msg := fmt.Sprintf("Waiting for Deployment rollout to finish: %d out of %d new replicas have been updated...", deploymentStatus.UpdatedReplicas, *deploymentSpec.Replicas)
			controllerRollout.Status.MarkChildResourcesUnhealthy("Progressing", msg, controllerRollout.Generation)
			return nil
		} else if deploymentStatus.Replicas > deploymentStatus.UpdatedReplicas {
			msg := fmt.Sprintf("Waiting for Deployment rollout to finish: %d old replicas are pending termination...", deploymentStatus.Replicas-deploymentStatus.UpdatedReplicas)
			controllerRollout.Status.MarkChildResourcesUnhealthy("Progressing", msg, controllerRollout.Generation)
			return nil
		} else if deploymentStatus.AvailableReplicas < deploymentStatus.UpdatedReplicas {
			msg := fmt.Sprintf("Waiting for Deployment rollout to finish: %d of %d updated replicas are available...", deploymentStatus.AvailableReplicas, deploymentStatus.UpdatedReplicas)
			controllerRollout.Status.MarkChildResourcesUnhealthy("Progressing", msg, controllerRollout.Generation)
			return nil
		}
	} else {
		msg := "Waiting for Deployment rollout to finish: observed deployment generation less than desired generation"
		controllerRollout.Status.MarkChildResourcesUnhealthy("Progressing", msg, controllerRollout.Generation)
		return nil
	}

	controllerRollout.Status.MarkChildResourcesHealthy(controllerRollout.Generation)

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
