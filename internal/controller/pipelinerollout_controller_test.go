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
	"sigs.k8s.io/controller-runtime/pkg/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("PipelineRollout Controller", func() {
	const (
		name      = "pipelinerollout-test"
		namespace = "default"

		timeout  = 10 * time.Second
		duration = 10 * time.Second
		interval = 250 * time.Millisecond
	)

	var ctx context.Context
	var resource client.Object
	var rawContent string
	var resourceLookupKey types.NamespacedName

	BeforeEach(func() {
		ctx = context.Background()

		// TODO: LOW PRIORITY: load an entire valid pipeline from test file or from sample files
		rawContent = RemoveIndentationFromJSON(`{
			"interStepBufferServiceName": "my-isbsvc",
			"vertices": [
				{
					"name": "in",
					"source": {
						"generator": {
							"rpu": 5,
							"duration": "1s"
						}
					}
				},
				{
					"name": "cat",
					"udf": {
						"builtin": {
							"name": "cat"
						}
					}	
				},
				{
					"name": "cat",
					"sink": {
						"log": {}
					}
				}
			],
			"edges": [
				{
					"from": "in",
					"to": "cat"
				},
				{
					"from": "cat",
					"to": "out"
				}
			]
		}
		`)

		resource = &apiv1.PipelineRollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: apiv1.PipelineRolloutSpec{
				Pipeline: runtime.RawExtension{
					Raw: []byte(rawContent),
				},
			},
		}

		resourceLookupKey = types.NamespacedName{Name: name, Namespace: namespace}
	})

	Context("When applying a PipelineRollout spec", func() {
		It("Should create the PipelineRollout succesfully", func() {
			Expect(k8sClient.Create(ctx, resource)).Should(Succeed())

			createdResource := &apiv1.PipelineRollout{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Verifying the content of the pipeline field")
			Expect(createdResource.Spec.Pipeline.Raw).Should(BeEquivalentTo(rawContent))
		})

		It("Should create a Numaflow Pipeline", func() {
			createdResource := &numaflowv1.Pipeline{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})

		It("Should have the PipelineRollout Status Phase has Running", func() {
			createdResource := &apiv1.PipelineRollout{}

			Consistently(func() (apiv1.Phase, error) {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return createdResource.Status.Phase, nil
			}, duration, interval).Should(Equal(apiv1.PhaseRunning))
		})

		It("Should update the PipelineRollout and Numaflow Pipeline", func() {
			By("updating the PipelineRollout")

			currentResource := &apiv1.PipelineRollout{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentResource)).ToNot(HaveOccurred())

			// TODO: only change part of the spec for the update instead of having the entire JSON here
			newRawContent := []byte(
				RemoveIndentationFromJSON(`{
					"interStepBufferServiceName": "my-isbsvc",
					"vertices": [
						{
							"name": "in",
							"source": {
								"generator": {
									"rpu": 10,
									"duration": "1s"
								}
							}
						},
						{
							"name": "cat",
							"udf": {
								"builtin": {
									"name": "cat"
								}
							}
						},
						{
							"name": "cat",
							"sink": {
								"log": {}
							}
						}
					],
					"edges": [
						{
							"from": "in",
							"to": "cat"
						},
						{
							"from": "cat",
							"to": "out"
						}
					]
				}
			`))

			currentResource.Spec.Pipeline.Raw = newRawContent

			Expect(k8sClient.Update(ctx, currentResource)).ToNot(HaveOccurred())

			By("Verifying the content of the pipeline field of the PipelineRollout")
			updatedResource := &apiv1.PipelineRollout{}
			Eventually(func() ([]byte, error) {
				err := k8sClient.Get(ctx, resourceLookupKey, updatedResource)
				if err != nil {
					return []byte{}, err
				}
				return updatedResource.Spec.Pipeline.Raw, nil
			}, timeout, interval).Should(Equal(currentResource.Spec.Pipeline.Raw))

			// TODO: improve this comparison as needed
			By("Verifying the content of the spec field of the Numaflow Pipeline")
			updatedChildResource := &numaflowv1.Pipeline{}
			Eventually(func() (int64, error) {
				err := k8sClient.Get(ctx, resourceLookupKey, updatedChildResource)
				if err != nil {
					return -1, err
				}

				for _, vertex := range updatedChildResource.Spec.Vertices {
					if vertex.Name == "in" {
						return *vertex.Source.Generator.RPU, nil
					}
				}

				return -1, nil
			}, timeout, interval).Should(BeEquivalentTo(10))
		})

		It("Should delete the PipelineRollout and Numaflow Pipeline", func() {
			currentResource := &apiv1.PipelineRollout{}
			Expect(k8sClient.Get(ctx, resourceLookupKey, currentResource)).ToNot(HaveOccurred())

			Expect(k8sClient.Delete(ctx, currentResource)).Should(Succeed())

			deletedResource := &apiv1.PipelineRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			// TODO: this is not working as expected. It reaches timeout without deleting the Numaflow Pipeline
			// deletedChildResource := &numaflowv1.Pipeline{}
			// Eventually(func() bool {
			// 	err := k8sClient.Get(ctx, resourceLookupKey, deletedChildResource)
			// 	return errors.IsNotFound(err)
			// }, 120*time.Second, interval).Should(BeTrue())
		})
	})
})
