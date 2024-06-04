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

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
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

	Context("When creating a PipelineRollout", func() {
		It("Should create the PipelineRollout succesfully and as expected", func() {
			Expect(k8sClient.Create(ctx, resource)).Should(Succeed())

			createdResource := &apiv1.PipelineRollout{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(createdResource.Spec.Pipeline.Raw).Should(BeEquivalentTo(rawContent))
		})

		It("Should create a Pipeline", func() {
			createdResource := &numaflowv1.Pipeline{}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})

		It("Should have the PipelineRollout Phase has Running", func() {
			createdResource := &apiv1.PipelineRollout{}

			Consistently(func() (apiv1.Phase, error) {
				err := k8sClient.Get(ctx, resourceLookupKey, createdResource)
				if err != nil {
					return apiv1.Phase(""), err
				}
				return createdResource.Status.Phase, nil
			}, duration, interval).Should(Equal(apiv1.PhaseRunning))
		})
	})
})
