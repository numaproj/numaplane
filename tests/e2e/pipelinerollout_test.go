package e2e

import (
	"encoding/json"
	"fmt"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
)

var _ = Describe("PipelineRollout E2E", func() {
	const (
		timeout             = time.Second * 10
		interval            = time.Millisecond * 250
		namespace           = "default"
		pipelineRolloutName = "test-pipeline-rollout"
	)

	It("Should create, update, and delete a PipelineRollout", func() {
		givenHelper := NewGivenHelper(k8sClient)
		whenHelper := NewWhenHelper(k8sClient)
		expectHelper := NewExpectHelper(k8sClient)

		By("Creating a new PipelineRollout")
		pipelineRollout := givenHelper.APipelineRollout(pipelineRolloutName, namespace)
		fmt.Printf("Creating PipelineRollout: %+v\n", pipelineRollout)

		err := whenHelper.CreatePipelineRollout(ctx, pipelineRollout)
		Expect(err).To(Succeed())
		fmt.Printf("PipelineRollout created, error: %v\n", err)

		By("Checking if the PipelineRollout was successfully created")
		Eventually(func() error {
			createdPipelineRollout := &apiv1.PipelineRollout{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRolloutName, Namespace: namespace}, createdPipelineRollout)
			if err != nil {
				fmt.Printf("Error getting PipelineRollout: %v\n", err)
				return err
			}
			fmt.Printf("Retrieved PipelineRollout: %+v\n", createdPipelineRollout)
			return nil
		}, time.Second*30, time.Second).Should(Succeed())

		By("Updating the PipelineRollout")
		updatedPipelineSpec := createUpdatedPipelineSpec()
		pipelineRollout.Spec.Pipeline.Raw = updatedPipelineSpec
		Expect(whenHelper.UpdatePipelineRollout(ctx, pipelineRollout)).To(Succeed())

		By("Checking if the PipelineRollout was successfully updated")
		expectHelper.PipelineRolloutToBeUpdated(ctx, namespace, pipelineRolloutName, updatedPipelineSpec)

		By("Deleting the PipelineRollout")
		Expect(whenHelper.DeletePipelineRollout(ctx, pipelineRollout)).To(Succeed())

		By("Checking if the PipelineRollout was successfully deleted")
		expectHelper.PipelineRolloutToBeDeleted(ctx, namespace, pipelineRolloutName)
	})
})

// createSamplePipelineRollout and createUpdatedPipelineSpec functions remain the same

func createSamplePipelineRollout(name, namespace string) *apiv1.PipelineRollout {
	pipelineSpec := numaflowv1.PipelineSpec{
		InterStepBufferServiceName: "test-isbsvc",
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      pointer.Int64(5),
						Duration: &metav1.Duration{Duration: time.Second},
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

	return &apiv1.PipelineRollout{
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
}

func createUpdatedPipelineSpec() []byte {
	updatedPipelineSpec := numaflowv1.PipelineSpec{
		InterStepBufferServiceName: "updated-isbsvc",
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      pointer.Int64(10),
						Duration: &metav1.Duration{Duration: 2 * time.Second},
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

	updatedPipelineSpecRaw, _ := json.Marshal(updatedPipelineSpec)
	return updatedPipelineSpecRaw
}
