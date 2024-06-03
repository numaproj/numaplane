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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	sigsReconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("PipelineRollout Controller", func() {
	Context("When reconciling a resource", func() {
		const namespace = "default"
		const resourceName = "pipelinerollout-test"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: namespace,
		}

		// pipelinerollout := &apiv1.PipelineRollout{}

		var controllerReconciler *PipelineRolloutReconciler

		BeforeEach(func() {
			By("creating the custom resource for the Kind PipelineRollout")
			// err := k8sClient.Get(ctx, typeNamespacedName, pipelinerollout)
			// if err != nil && errors.IsNotFound(err) {
			resource := &apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: namespace,
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: runtime.RawExtension{
						Raw: []byte(`{"field":"val"}`),
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			// }

			controllerReconciler = &PipelineRolloutReconciler{
				client:     k8sClient,
				scheme:     k8sClient.Scheme(),
				restConfig: cfg,
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &apiv1.PipelineRollout{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			// By("Cleanup the specific resource instance PipelineRollout")
			// Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, sigsReconcile.Request{
				NamespacedName: typeNamespacedName,
			})

			Expect(err).NotTo(HaveOccurred())

			// Get the reconciled resource.
			err = k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			// Check phase is Running.
			Expect(resource.Status.Phase).To(Equal(apiv1.PhaseRunning))

			// TODO: check that the pipeline contains the expected `{"field":"val"}`
			// Expect(resource.Spec.Pipeline.Raw)

			// =========================

			// update
			// resource.Spec.Pipeline = runtime.RawExtension{
			// 	Raw: []byte(`{"field":"val2"}`),
			// }

			// _, err = controllerReconciler.Reconcile(ctx, sigsReconcile.Request{
			// 	NamespacedName: typeNamespacedName,
			// })

			// Expect(err).NotTo(HaveOccurred())

			// // Get the reconciled resource.
			// err = k8sClient.Get(ctx, typeNamespacedName, resource)
			// Expect(err).NotTo(HaveOccurred())

			// // Check phase is Running.
			// Expect(resource.Status.Phase).To(Equal(apiv1.PhaseRunning))

		})

		// It("should successfully reconcile the resource", func() {
		// 	By("Reconciling the created resource")
		// 	controllerReconciler := &PipelineRolloutReconciler{
		// 		client:     k8sClient,
		// 		scheme:     k8sClient.Scheme(),
		// 		restConfig: cfg,
		// 	}

		// 	_, err := controllerReconciler.Reconcile(ctx, sigsReconcile.Request{
		// 		NamespacedName: typeNamespacedName,
		// 	})

		// 	Expect(err).NotTo(HaveOccurred())
		// 	// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
		// 	// Example: If you expect a certain status condition after reconciliation, verify it here.
		// })
	})
})

// func reconcile(t *testing.T, r *GitSyncReconciler, gitSync *apiv1.GitSync) {
// 	err := r.reconcile(context.Background(), gitSync)
// 	assert.NoError(t, err)
// }
