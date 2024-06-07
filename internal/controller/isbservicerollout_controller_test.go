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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("ISBServiceRollout Controller", func() {
	const (
		namespace             = "default"
		isbServiceRolloutName = "isbservicerollout-test"
		timeout               = 10 * time.Second
		interval              = 250 * time.Millisecond
	)

	ctx := context.Background()

	isbServiceRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      isbServiceRolloutName,
		},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: runtime.RawExtension{
				Raw: []byte(`{
									"apiVersion": "v1",
									"kind": "Service",
									"metadata": {
											"name": "my-service"
									},
									"spec": {
											"selector": {
													"app": "MyApp"
											},
											"ports": [{
													"protocol": "TCP",
													"port": 80,
													"targetPort": 9376
											}]
									}
							}`),
			},
		},
	}

	resourceLookupKey := types.NamespacedName{Name: isbServiceRolloutName, Namespace: namespace}

	Context("When applying a ISBServiceRollout spec", func() {
		It("Should create the ISBServiceRollout if it does not exist", func() {
			Expect(k8sClient.Create(ctx, isbServiceRollout)).Should(Succeed())

			createdResource := &apiv1.ISBServiceRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Verifying the content of the ISBServiceRollout spec field")
			Expect(createdResource.Spec).Should(Equal(isbServiceRollout.Spec))
		})

		It("Should update the ISBServiceRollout", func() {
			By("updating the ISBServiceRollout")

			currentISBServiceRollout := &apiv1.ISBServiceRollout{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentISBServiceRollout)).ToNot(HaveOccurred())

			// Update the spec here
			currentISBServiceRollout.Spec.InterStepBufferService = runtime.RawExtension{
				Raw: []byte(`{
									"apiVersion": "v1",
									"kind": "Service",
									"metadata": {
											"name": "my-updated-service"
									},
									"spec": {
											"selector": {
													"app": "MyUpdatedApp"
											},
											"ports": [{
													"protocol": "TCP",
													"port": 8080,
													"targetPort": 9377
											}]
									}
							}`),
			}

			Expect(k8sClient.Update(ctx, currentISBServiceRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the ISBServiceRollout spec field")
			Eventually(func() apiv1.ISBServiceRolloutSpec {
				updatedResource := &apiv1.ISBServiceRollout{}
				_ = k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				return updatedResource.Spec
			}, timeout, interval).Should(Equal(currentISBServiceRollout.Spec))
		})

		It("Should delete the ISBServiceRollout", func() {
			Expect(k8sClient.Delete(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.ISBServiceRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			// TODO: use this on real cluster for e2e tests
			// NOTE: it's necessary to run on existing cluster to allow for deletion of child resources.
			// See https://book.kubebuilder.io/reference/envtest#testing-considerations for more details.
			// Could also reuse the env var used to set useExistingCluster to skip or perform the deletion based on CI settings.
			// Eventually(func() bool {
			// 	deletedChildResource := &apiv1.ISBServiceRollout{}
			// 	err := k8sClient.Get(ctx, resourceLookupKey, deletedChildResource)
			// 	return errors.IsNotFound(err)
			// }, timeout, interval).Should(BeTrue())
		})
	})

	Context("When applying an invalid ISBServiceRollout spec", func() {
		It("Should not create the ISBServiceRollout", func() {
			Expect(k8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				Spec: isbServiceRollout.Spec,
			})).To(HaveOccurred())

			Expect(k8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
			})).To(HaveOccurred())

			Expect(k8sClient.Create(ctx, &apiv1.ISBServiceRollout{
				ObjectMeta: isbServiceRollout.ObjectMeta,
				Spec:       apiv1.ISBServiceRolloutSpec{},
			})).To(HaveOccurred())
		})
	})
})
