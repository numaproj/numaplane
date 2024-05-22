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
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	finalizerName = "numaplane-controller"
)

// NumaflowControllerRolloutReconciler reconciles a NumaflowControllerRollout object
type NumaflowControllerRolloutReconciler struct {
	client     client.Client
	scheme     *runtime.Scheme
	restConfig *rest.Config
}

func NewNumaflowControllerRolloutReconciler(
	client client.Client,
	s *runtime.Scheme,
	restConfig *rest.Config,
) *NumaflowControllerRolloutReconciler {
	return &NumaflowControllerRolloutReconciler{
		client,
		s,
		restConfig,
	}
}

//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollerrollouts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollerrollouts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=numaplane.numaproj.io,resources=numaflowcontrollerrollouts/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NumaflowControllerRollout object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile
func (r *NumaflowControllerRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// update the Base Logger's level according to the Numaplane Config
	logger.RefreshBaseLoggerLevel()
	numaLogger := logger.GetBaseLogger().WithName("reconciler").WithValues("numaflowcontrollerrollout", req.NamespacedName)

	numaflowControllerRollout := &apiv1.NumaflowControllerRollout{}
	if err := r.client.Get(ctx, req.NamespacedName, numaflowControllerRollout); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		} else {
			numaLogger.Error(err, "Unable to get NumaflowControllerRollout", "request", req)
			return ctrl.Result{}, err
		}
	}

	// TODO: reconciliation logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NumaflowControllerRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.NumaflowControllerRollout{}).
		Complete(r)
}
