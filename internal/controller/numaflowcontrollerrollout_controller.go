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

	"github.com/argoproj/gitops-engine/pkg/diff"
	gitopsSync "github.com/argoproj/gitops-engine/pkg/sync"
	gitopsSyncCommon "github.com/argoproj/gitops-engine/pkg/sync/common"
	kubeUtil "github.com/argoproj/gitops-engine/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/sync"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	finalizerName = "numaplane-controller"
)

const (
	ControllerNumaflowControllerRollout = "numaflow-controller-rollout-controller"
)

// NumaflowControllerRolloutReconciler reconciles a NumaflowControllerRollout object
type NumaflowControllerRolloutReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	restConfig *rest.Config
	rawConfig  *rest.Config
	kubectl    kubeUtil.Kubectl

	definitions map[string]string
	stateCache  sync.LiveStateCache
}

func NewNumaflowControllerRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	rawConfig *rest.Config,
	kubectl kubeUtil.Kubectl,
) (*NumaflowControllerRolloutReconciler, error) {
	stateCache := sync.NewLiveStateCache(rawConfig)
	logger.RefreshBaseLoggerLevel()
	numaLogger := logger.GetBaseLogger().WithName("state cache").WithValues("numaflowcontrollerrollout")
	err := stateCache.Init(numaLogger)
	if err != nil {
		return nil, err
	}

	restConfig := rawConfig
	definitions, err := loadDefinitions()
	if err != nil {
		return nil, err
	}
	return &NumaflowControllerRolloutReconciler{
		client,
		s,
		restConfig,
		rawConfig,
		kubectl,
		definitions,
		stateCache,
	}, nil
}

func loadDefinitions() (map[string]string, error) {
	definitionsConfig, err := config.GetConfigManagerInstance().GetControllerDefinitionsConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get the controller definitions config, %v", err)
	}

	definitions := make(map[string]string)
	for _, definition := range definitionsConfig.ControllerDefinitions {
		definitions[definition.Version] = definition.FullSpec
	}
	return definitions, nil
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
	// update the Base Logger's level according to the Numaplane Config
	logger.RefreshBaseLoggerLevel()
	numaLogger := logger.GetBaseLogger().WithName("reconciler").WithValues("numaflowcontrollerrollout", req.NamespacedName)

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
		if err := r.client.Status().Update(ctx, numaflowControllerRollout); err != nil {
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

	controllerRollout.Status.MarkRunning()
	return nil
}

// applyOwnershipToManifests Applies GitSync ownership to Kubernetes manifests, returning modified manifests or an error.
func applyOwnershipToManifests(manifests []string, controllerRollout *apiv1.NumaflowControllerRollout) ([]string, error) {
	manifestsWithOwnership := make([]string, 0, len(manifests))
	for _, v := range manifests {
		reference, err := kubernetes.ApplyOwnership(v, controllerRollout)
		if err != nil {
			return nil, err
		}
		manifestsWithOwnership = append(manifestsWithOwnership, string(reference))
	}
	return manifestsWithOwnership, nil
}

func (r *NumaflowControllerRolloutReconciler) sync(
	rollout *apiv1.NumaflowControllerRollout,
	namespace string,
	numaLogger *logger.NumaLogger,
) (gitopsSyncCommon.OperationPhase, error) {

	// Get the target manifests
	version := rollout.Spec.Controller.Version
	manifest := r.definitions[version]

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
	numaLogger.Debugf("found %d target objects associated with Numaflow Controller version %s; versions defined:%+v", len(targetObjs), version, r.definitions)

	var infoProvider kubeUtil.ResourceInfoProvider
	clusterCache, err := r.stateCache.GetClusterCache()
	infoProvider = clusterCache
	if err != nil {
		return gitopsSyncCommon.OperationError, err
	}
	liveObjByKey, err := r.stateCache.GetManagedLiveObjs(rollout.Name, rollout.Namespace, targetObjs)
	if err != nil {
		return gitopsSyncCommon.OperationError, err
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
		return gitopsSyncCommon.OperationError, err
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
		numaLogger.Error(err, "Error on comparing git sync state")
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

	if err != nil {
		numaLogger.Error(err, "Error on getting the cluster cache")
		//return gitopsSyncCommon.OperationError, err.Error()
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

// SetupWithManager sets up the controller with the Manager.
func (r *NumaflowControllerRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {

	return ctrl.NewControllerManagedBy(mgr).
		// Reconcile NumaflowControllerRollouts when there's been a Generation changed (i.e. Spec change)
		For(&apiv1.NumaflowControllerRollout{}).WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)

	/*
	   controller, err := runtimecontroller.New(ControllerNumaflowControllerRollout, mgr, runtimecontroller.Options{Reconciler: r})

	   	if err != nil {
	   		return err
	   	}

	   // Watch NumaflowControllerRollouts

	   	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowControllerRollout{}), &handler.EnqueueRequestForObject{}, predicate.GenerationChangedPredicate{}); err != nil {
	   		return err
	   	}

	   // Watch Deployments of numaflow-controller
	   // Can add other resources as well
	   numaflowControllerDeployments := appv1.Deployment{}
	   numaflowControllerDeployments.Name = "numaflow-controller" // not sure if this would work or not
	   if err := controller.Watch(source.Kind(mgr.GetCache(), &numaflowControllerDeployments),

	   		handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &apiv1.NumaflowControllerRollout{}, handler.OnlyControllerOwner()),
	   		predicate.GenerationChangedPredicate{}); err != nil {
	   		return err
	   	}

	   return nil
	*/
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
		err = kubernetes.SetNumaplaneInstanceLabel(target, common.LabelKeyNumaplaneInstance, name)
		if err != nil {
			return nil, err
		}
		uns = append(uns, target)
	}
	return uns, nil
}
