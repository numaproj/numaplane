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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	runtimecontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/numaproj/numaplane/internal/util/logger"
	"github.com/numaproj/numaplane/internal/util/metrics"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	ControllerNumaflowController = "numaflowcontroller-controller"
)

// NumaflowControllerReconciler reconciles a NumaflowController object
type NumaflowControllerReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	customMetrics *metrics.CustomMetrics
	// the recorder is used to record events
	recorder record.EventRecorder
}

func NewNumaflowControllerReconciler(
	c client.Client,
	s *runtime.Scheme,
	customMetrics *metrics.CustomMetrics,
	recorder record.EventRecorder,
) *NumaflowControllerReconciler {

	r := &NumaflowControllerReconciler{
		c,
		s,
		customMetrics,
		recorder,
	}

	return r
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
	numaLogger := logger.GetBaseLogger().WithName("numaflowcontroller-reconciler").WithValues("numaflowcontroller", req.NamespacedName)
	// update the context with this Logger so downstream users can incorporate these values in the logs
	// ctx = logger.WithLogger(ctx, numaLogger)

	// TODO: add reconciliation logic here
	numaLogger.Info("TODO: reconcile NumaflowController resource")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NumaflowControllerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// return ctrl.NewControllerManagedBy(mgr).
	// 	For(&numaplanenumaprojiov1alpha1.NumaflowController{}).
	// 	Complete(r)

	controller, err := runtimecontroller.New(ControllerNumaflowController, mgr, runtimecontroller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch NumaflowController
	if err := controller.Watch(source.Kind(mgr.GetCache(), &apiv1.NumaflowController{},
		&handler.TypedEnqueueRequestForObject[*apiv1.NumaflowController]{}, predicate.TypedGenerationChangedPredicate[*apiv1.NumaflowController]{})); err != nil {
		return fmt.Errorf("failed to watch NumaflowController: %v", err)
	}

	// Watch NumaflowController
	numaflowControllerUns := &unstructured.Unstructured{}
	numaflowControllerUns.SetGroupVersionKind(schema.GroupVersionKind{
		Kind:    apiv1.NumaflowControllerGroupVersionKind.Kind,
		Group:   apiv1.NumaflowControllerGroupVersionKind.Group,
		Version: apiv1.NumaflowControllerGroupVersionKind.Version,
	})
	if err := controller.Watch(source.Kind(mgr.GetCache(), numaflowControllerUns,
		handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](mgr.GetScheme(), mgr.GetRESTMapper(),
			&apiv1.NumaflowController{}, handler.OnlyControllerOwner()), predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{})); err != nil {
		return fmt.Errorf("failed to watch NumaflowController: %v", err)
	}

	return nil
}
