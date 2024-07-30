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

package e2e

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var pipelineSpecSourceRPU = int64(5)
var pipelineSpecSourceDuration = metav1.Duration{
	Duration: time.Second,
}
var pipelineSpec = numaflowv1.PipelineSpec{
	InterStepBufferServiceName: "my-isbsvc",
	Vertices: []numaflowv1.AbstractVertex{
		{
			Name: "in",
			Source: &numaflowv1.Source{
				Generator: &numaflowv1.GeneratorSource{
					RPU:      &pipelineSpecSourceRPU,
					Duration: &pipelineSpecSourceDuration,
				},
			},
		},
		{
			Name: "out",
			Sink: &numaflowv1.Sink{
				AbstractSink: numaflowv1.AbstractSink{
					Log: &numaflowv1.Log{},
				},
			},
		},
	},
	Edges: []numaflowv1.Edge{
		{
			From: "in",
			To:   "out",
		},
	},
}

var _ = Describe("PipelineRollout e2e", func() {

	const (
		pipelineRolloutName = "e2e-pipeline-rollout"
	)

	pipelinegvr := getGVRForPipeline()

	It("Should create the PipelineRollout if it does not exist", func() {

		pipelineRolloutSpec := CreatePipelineRolloutSpec(pipelineRolloutName, Namespace)
		_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying that the PipelineRollout was created")
		Eventually(func() error {
			_, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err
		}).WithTimeout(testTimeout).Should(Succeed())

		By("Verifying that the Pipeline was created")
		createdPipeline := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdPipeline = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())

		By("Verifying the content of the pipeline spec")
		createdPipelineSpec := numaflowv1.PipelineSpec{}
		rawPipelineSpec := createdPipeline.Object["spec"].(map[string]interface{})
		rawPipelineSpecBytes, err := json.Marshal(rawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(rawPipelineSpecBytes, &createdPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(createdPipelineSpec).Should(Equal(pipelineSpec))

	})

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		// get child Pipeline
		createdPipeline := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdPipeline = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())

		// modify spec to have different isbsvc name
		createdPipeline.Object["spec"].(map[string]interface{})["interStepBufferServiceName"] = "new-isbsvc"

		// update child Pipeline
		_, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Update(ctx, createdPipeline, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdPipeline = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())
		createdPipelineSpec := numaflowv1.PipelineSpec{}
		rawPipelineSpec := createdPipeline.Object["spec"].(map[string]interface{})
		rawPipelineSpecBytes, err := json.Marshal(rawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(rawPipelineSpecBytes, &createdPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying that the child Pipeline spec has been restored to the original")
		Expect(createdPipelineSpec).Should(Equal(pipelineSpec))

	})

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {

		// isbsvc/controller must be up to update pipeline as of PR #125
		isbsvcrollout := CreateISBServiceRolloutSpec("my-isbsvc", Namespace)
		_, err := isbServiceRolloutClient.Create(ctx, isbsvcrollout, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		numaflowcontrollerrollout := CreateNumaflowControllerRolloutSpec("numaflow-controller", Namespace)
		_, err = numaflowControllerRolloutClient.Create(ctx, numaflowcontrollerrollout, metav1.CreateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// new Pipeline spec
		updatedPipelineSpec := pipelineSpec
		updatedPipelineSpec.InterStepBufferServiceName = "updated-isbsvc"

		rawSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// get current PipelineRollout
		rollout := &apiv1.PipelineRollout{}
		Eventually(func() bool {
			rollout, err = pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err == nil
		}).WithTimeout(testTimeout).Should(BeTrue())

		// update the PipelineRollout
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		_, err = pipelineRolloutClient.Update(ctx, rollout, metav1.UpdateOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// wait for update to reconcile
		time.Sleep(5 * time.Second)

		// get Pipeline to check that spec has been updated
		createdPipeline := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdPipeline = unstruct
			return true
		}).WithTimeout(testTimeout).Should(BeTrue())
		createdPipelineSpec := numaflowv1.PipelineSpec{}
		rawPipelineSpec := createdPipeline.Object["spec"].(map[string]interface{})
		rawPipelineSpecBytes, err := json.Marshal(rawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(rawPipelineSpecBytes, &createdPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying pipeline spec is equal to updated spec")
		Expect(createdPipelineSpec).Should(Equal(updatedPipelineSpec))

		// delete isbsvc/controller made for this case
		err = isbServiceRolloutClient.Delete(ctx, "my-isbsvc", metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		err = numaflowControllerRolloutClient.Delete(ctx, "numaflow-controller", metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

	})

	It("Should delete the PipelineRollout and child Pipeline", func() {

		err := pipelineRolloutClient.Delete(ctx, pipelineRolloutName, metav1.DeleteOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		Eventually(func() bool {
			_, err := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the PipelineRollout: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The PipelineRollout should have been deleted but it was found.")

		Eventually(func() bool {
			_, err := dynamicClient.Resource(pipelinegvr).Namespace(Namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the Pipeline: " + err.Error())
				}
				return false
			}
			return true
		}).WithTimeout(testTimeout).Should(BeFalse(), "The Pipeline should have been deleted but it was found.")

	})

})

func CreatePipelineRolloutSpec(name, namespace string) *apiv1.PipelineRollout {

	pipelineSpecRaw, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())

	pipelineRollout := &apiv1.PipelineRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "PipelineRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{
					Raw: pipelineSpecRaw,
				},
			},
		},
	}

	return pipelineRollout

}

func getGVRForPipeline() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaflow.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelines",
	}
}
