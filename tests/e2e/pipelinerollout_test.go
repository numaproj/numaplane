package e2e

import (
	"context"
	"encoding/json"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var rpuVal = int64(5)
var pipelineSpec = numaflowv1.PipelineSpec{
	InterStepBufferServiceName: "test-isbsvc",
	Vertices: []numaflowv1.AbstractVertex{
		{
			Name: "in",
			Source: &numaflowv1.Source{
				Generator: &numaflowv1.GeneratorSource{
					RPU:      &rpuVal,
					Duration: &metav1.Duration{Duration: time.Second},
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

var _ = Describe("PipelineRollout E2E", func() {
	const (
		namespace           = "numaplane-system"
		pipelineRolloutName = "test-pipeline-rollout"
	)
	gvr := getGVR()

	It("Should create the PipelineRollout if it does not exist or it should update existing PipelineRollout and Numaflow Pipeline", func() {
		// create PipelineRolloutSpec
		pipelineRolloutSpec := createPipelineRolloutSpec(pipelineRolloutName, namespace)

		// create the PipelineRollout
		err := createPipelineRollout(ctx, pipelineRolloutSpec)
		Expect(err).ShouldNot(HaveOccurred())

		createdResource := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdResource = unstruct
			return true
		}, timeout, duration).Should(BeTrue())

		// unmarshal the Pipeline from the PipelineRollout
		createdPipelineRolloutPipelineSpec := numaflowv1.PipelineSpec{}
		rawPipelineSpec := createdResource.Object["spec"].(map[string]interface{})["pipeline"].(map[string]interface{})
		rawPipelineSpecBytes, err := json.Marshal(rawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(rawPipelineSpecBytes, &createdPipelineRolloutPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying the content of the pipeline spec field")
		Expect(createdPipelineRolloutPipelineSpec).Should(Equal(pipelineSpec))
	})

	It("Should create a Numaflow Pipeline", func() {
		// Correct GVR for Numaflow Pipeline
		pipelineGVR := schema.GroupVersionResource{
			Group:    "numaflow.numaproj.io",
			Version:  "v1alpha1",
			Resource: "pipelines",
		}

		createdPipeline := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(pipelineGVR).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			createdPipeline = unstruct
			return true
		}, timeout, duration).Should(BeTrue())

		// unmarshal the spec from the created Pipeline
		createdPipelineSpec := numaflowv1.PipelineSpec{}
		rawPipelineSpec := createdPipeline.Object["spec"].(map[string]interface{})
		rawPipelineSpecBytes, err := json.Marshal(rawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		err = json.Unmarshal(rawPipelineSpecBytes, &createdPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		By("Verifying the content of the pipeline spec")
		Expect(createdPipelineSpec).Should(Equal(pipelineSpec))
	})

	It("Should update the entire PipelineRollout and Numaflow Pipeline", func() {
		// Fetch existing PipelineRollout
		unstruct, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		Expect(err).ShouldNot(HaveOccurred())

		// Marshal spec map to RawExtension bytes, unmarshal to update with PipelineRollout
		rawPipelineSpec := unstruct.Object["spec"].(map[string]interface{})["pipeline"].(map[string]interface{})
		rawPipelineSpecBytes, err := json.Marshal(rawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		currentPipelineSpec := numaflowv1.PipelineSpec{}
		err = json.Unmarshal(rawPipelineSpecBytes, &currentPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// Update fields in the fetched PipelineRollout's PipelineSpec
		currentPipelineSpec.InterStepBufferServiceName = "my-isbsvc-updated"

		// Convert the updated PipelineSpec back to RawExtension bytes
		updatedPipelineSpecBytes, err := json.Marshal(currentPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// Convert the updated PipelineSpec back to map[string]interface{} so you can put it directly back into the unstructured object.
		updatedPipelineSpecAsMap := make(map[string]interface{})
		err = json.Unmarshal(updatedPipelineSpecBytes, &updatedPipelineSpecAsMap)
		Expect(err).ShouldNot(HaveOccurred())

		// Save updated spec back into PipelineRollout
		unstruct.Object["spec"].(map[string]interface{})["pipeline"] = updatedPipelineSpecAsMap

		// Update PipelineRollout in the cluster
		err = updatePipelineRollout(ctx, unstruct)
		Expect(err).ShouldNot(HaveOccurred())

		// Fetch the updated PipelineRollout
		updatedResource := &unstructured.Unstructured{}
		Eventually(func() bool {
			unstruct, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			updatedResource = unstruct
			return true
		}, timeout, duration).Should(BeTrue())

		// Unmarshal the updated PipelineRollout spec into a numaflowv1.PipelineSpec
		updatedRawPipelineSpec := updatedResource.Object["spec"].(map[string]interface{})["pipeline"].(map[string]interface{})
		updatedRawPipelineSpecBytes, err := json.Marshal(updatedRawPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		updatedPipelineSpec := numaflowv1.PipelineSpec{}
		err = json.Unmarshal(updatedRawPipelineSpecBytes, &updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		// Validate that the updated PipelineRollout has the expected content
		Expect(updatedPipelineSpec).To(Equal(currentPipelineSpec))
	})

	It("Should delete the PipelineRollout and Numaflow Pipeline", func() {
		// Delete PipelineRollout
		err := deletePipelineRollout(ctx, namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())

		// Refetch the PipelineRollout to ensure it was deleted
		Eventually(func() bool {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			// If an error occurred that wasn't a not found error, fail the test
			if err != nil {
				if !errors.IsNotFound(err) {
					Fail("An unexpected error occurred when fetching the PipelineRollout: " + err.Error())
				}
				// Resource not found, hence it has been deleted
				return false
			}
			// Resource is found, which means it's not yet deleted.
			return true
		}, timeout, duration).Should(BeFalse(), "The PipelineRollout should have been deleted but it was found.")
	})

})

func createPipelineRolloutSpec(name, namespace string) *unstructured.Unstructured {

	pipelineSpecRaw, _ := json.Marshal(pipelineSpec)

	PipelineRollout := &apiv1.PipelineRollout{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "numaplane.numaproj.io/v1alpha1",
			Kind:       "PipelineRollout",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: runtime.RawExtension{
				Raw: pipelineSpecRaw,
			},
		},
	}

	unstructuredObj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(PipelineRollout)
	return &unstructured.Unstructured{Object: unstructuredObj}
}

func createPipelineRollout(ctx context.Context, pipelineRollout *unstructured.Unstructured) error {
	gvr := getGVR()
	_, err := dynamicClient.Resource(gvr).Namespace(pipelineRollout.GetNamespace()).Create(ctx, pipelineRollout, metav1.CreateOptions{})
	return err
}

func deletePipelineRollout(ctx context.Context, namespace, name string) error {
	gvr := getGVR()
	return dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func updatePipelineRollout(ctx context.Context, pipelineRolloutUpdate *unstructured.Unstructured) error {
	gvr := getGVR()
	_, err := dynamicClient.Resource(gvr).Namespace(pipelineRolloutUpdate.GetNamespace()).Update(ctx, pipelineRolloutUpdate, metav1.UpdateOptions{})
	return err
}

func getGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}
}
