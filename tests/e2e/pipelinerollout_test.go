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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("PipelineRollout E2E", func() {
	const (
		namespace           = "numaplane-system"
		pipelineRolloutName = "test-pipeline-rollout"
		timeout             = 30 * time.Second
		duration            = time.Second
	)
	gvr := getGVR()

	BeforeEach(func() {
		err := dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, pipelineRolloutName, metav1.DeleteOptions{})
		// If the error is not that it's 'not found', let's fail the test.
		if err != nil && !errors.IsNotFound(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("Should create, update, and delete a PipelineRollout", func() {
		By("Creating a new PipelineRollout")
		pipelineRollout := createPipelineRolloutSpec(pipelineRolloutName, namespace)

		err := createPipelineRollout(ctx, pipelineRollout)
		Expect(err).NotTo(HaveOccurred())

		By("Checking if the PipelineRollout was successfully created")

		Eventually(func() error {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return err
		}, time.Second*30, time.Second).Should(Succeed())

		By("Deleting the PipelineRollout")
		err = deletePipelineRollout(ctx, namespace, pipelineRolloutName)
		Expect(err).NotTo(HaveOccurred())

		By("Checking if the PipelineRollout was successfully deleted")
		Eventually(func() bool {
			_, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			return errors.IsNotFound(err)
		}, timeout, duration).Should(BeTrue())
	})

	It("should correctly update the PipelineRollout", func() {
		By("Creating a new PipelineRollout for the update test")
		pipelineRollout := createPipelineRolloutSpec(pipelineRolloutName, namespace)
		err := createPipelineRollout(ctx, pipelineRollout)
		Expect(err).NotTo(HaveOccurred())

		// After creating the pipelineRollout...
		Eventually(func() error {
			pipelineRollout, err = dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			Expect(err).Should(Succeed())
			return err
		}, time.Second*30, time.Second).Should(Succeed())

		// Now pipelineRollout has the `resourceVersion` field, and after making modifications you can update it
		pipelineRolloutUpdate := pipelineRollout.DeepCopy()

		By("Updating the PipelineRollout")

		updatedPipeline := map[string]any{
			"InterStepBufferServiceName": "new-test-isbsvc",
		}
		Eventually(func() error {
			err = updateFields(pipelineRolloutUpdate)
			Expect(err).Should(Succeed())
			return err
		}, timeout, duration).Should(Succeed())

		pipelineRolloutUpdate.Object["spec"].(map[string]any)["pipeline"] = updatedPipeline

		err = updatePipelineRollout(ctx, pipelineRolloutUpdate)
		Expect(err).NotTo(HaveOccurred())

		By("Checking if the PipelineRollout was successfully updated")
		Eventually(func() error {
			updatedRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			updatedFieldFromServer := updatedRollout.Object["spec"].(map[string]any)["pipeline"].(map[string]any)["InterStepBufferServiceName"].(string)
			if updatedFieldFromServer != updatedPipeline["InterStepBufferServiceName"] {
				return fmt.Errorf("field of updated rollout does not match expected value")
			}
			return nil
		}, timeout, duration).Should(Succeed())
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

func updateFields(pipelineRolloutUpdate *unstructured.Unstructured) error {

	if spec, exists := pipelineRolloutUpdate.Object["spec"].(map[string]interface{}); exists {
		if pipe, hasPipe := spec["pipeline"].(map[string]interface{}); hasPipe {
			updatedPipeline := map[string]interface{}{
				"InterStepBufferServiceName": "new-test-isbsvc",
			}
			pipe["spec"] = updatedPipeline
		} else {
			return fmt.Errorf("field 'pipeline' not found in the object spec")
		}
	} else {
		return fmt.Errorf("field 'spec' not found in the object")
	}
	return nil
}
