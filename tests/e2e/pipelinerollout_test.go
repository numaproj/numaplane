package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("PipelineRollout E2E", func() {
	const (
		namespace           = "numaplane-system"
		pipelineRolloutName = "test-pipeline-rollout"
	)
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}
	BeforeEach(func() {
		err := dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, pipelineRolloutName, metav1.DeleteOptions{})
		if err != nil {
			fmt.Printf("Failed to delete pipelineRollout: %v\n", err)
		}
	})

	It("Should create, update, and delete a PipelineRollout", func() {
		By("Creating a new PipelineRollout")
		pipelineRollout := createPipelineRolloutSpec(pipelineRolloutName, namespace)

		err := createPipelineRollout(ctx, pipelineRollout)
		Expect(err).To(Succeed())
		fmt.Printf("PipelineRollout created, error: %v\n", err)

		By("Checking if the PipelineRollout was successfully created")

		Eventually(func() error {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err
		}, time.Second*30, time.Second).Should(Succeed())

		By("Deleting the PipelineRollout")
		err = deletePipelineRollout(ctx, namespace, pipelineRolloutName)
		Expect(err).To(Succeed())

		By("Checking if the PipelineRollout was successfully deleted")
		Eventually(func() error {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err
		}, time.Second*30, time.Second).ShouldNot(Succeed())
	})

	It("should ", func() {
		By("Creating a new PipelineRollout for the update test")
		pipelineRollout := createPipelineRolloutSpec(pipelineRolloutName, namespace)
		err := createPipelineRollout(ctx, pipelineRollout)
		Expect(err).To(Succeed())

		// After creating the pipelineRollout...
		pipelineRollout, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
		Expect(err).Should(Succeed())

		// Now pipelineRollout has the `resourceVersion` field, and after making modifications you can update it
		pipelineRolloutUpdate := pipelineRollout.DeepCopy()

		By("Updating the PipelineRollout")

		updatedPipeline := map[string]interface{}{
			"InterStepBufferServiceName": "new-test-isbsvc",
			// Add more fields if necessary, matching the structure of your PipelineSpec
			// ...
		}
		pipelineRolloutUpdate.Object["spec"].(map[string]interface{})["pipeline"] = updatedPipeline

		err = updatePipelineRollout(ctx, pipelineRolloutUpdate)
		Expect(err).To(Succeed())

		By("Checking if the PipelineRollout was successfully updated")
		Eventually(func() error {
			updatedRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			updatedFieldFromServer := updatedRollout.Object["spec"].(map[string]interface{})["pipeline"].(map[string]interface{})["InterStepBufferServiceName"].(string)
			if updatedFieldFromServer != updatedPipeline["InterStepBufferServiceName"] {
				return fmt.Errorf("field of updated rollout does not match expected value")
			}
			return nil
		}, time.Second*30, time.Second).Should(Succeed())
	})

})

func createPipelineRolloutSpec(name, namespace string) *unstructured.Unstructured {
	rpuVal := int64(5)
	pipelineSpec := numaflowv1.PipelineSpec{
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
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}
	_, err := dynamicClient.Resource(gvr).Namespace(pipelineRollout.GetNamespace()).Create(ctx, pipelineRollout, metav1.CreateOptions{})
	return err
}

func deletePipelineRollout(ctx context.Context, namespace, name string) error {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}
	return dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func updatePipelineRollout(ctx context.Context, pipelineRolloutUpdate *unstructured.Unstructured) error {
	gvr := schema.GroupVersionResource{
		Group:    "numaplane.numaproj.io",
		Version:  "v1alpha1",
		Resource: "pipelinerollouts",
	}
	_, err := dynamicClient.Resource(gvr).Namespace(pipelineRolloutUpdate.GetNamespace()).Update(ctx, pipelineRolloutUpdate, metav1.UpdateOptions{})
	return err
}
