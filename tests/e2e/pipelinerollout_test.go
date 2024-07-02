package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("PipelineRollout E2E", func() {
	const (
		timeout             = time.Second * 10
		interval            = time.Millisecond * 250
		namespace           = "numaplane-system"
		pipelineRolloutName = "test-pipeline-rollout"
		updatedRolloutName  = "updated-pipeline-rollout" // Add this
	)

	It("Should create, update, and delete a PipelineRollout", func() {
		givenHelper := NewGivenHelper(dynamicClient)
		whenHelper := NewWhenHelper(dynamicClient)
		expectHelper := NewExpectHelper(dynamicClient)

		By("Creating a new PipelineRollout")
		pipelineRollout := givenHelper.APipelineRollout(pipelineRolloutName, namespace)
		fmt.Printf("Creating PipelineRollout: %+v\n", pipelineRollout)

		err := whenHelper.CreatePipelineRollout(ctx, pipelineRollout)
		Expect(err).To(Succeed())
		fmt.Printf("PipelineRollout created, error: %v\n", err)

		By("Checking if the PipelineRollout was successfully created")
		gvr := schema.GroupVersionResource{
			Group:    "numaplane.numaproj.io",
			Version:  "v1alpha1",
			Resource: "pipelinerollouts",
		}

		Eventually(func() error {
			// createdPipelineRollout := &apiv1.PipelineRollout{}
			createdPipelineRollout, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			if err != nil {
				fmt.Printf("Error getting PipelineRollout: %v\n", err)
				return err
			}
			fmt.Printf("Retrieved PipelineRollout: %+v\n", createdPipelineRollout)
			return nil
		}, time.Second*30, time.Second).Should(Succeed())

		expectHelper.PipelineRolloutToExist(ctx, namespace, pipelineRolloutName)

		By("Deleting the PipelineRollout")
		Expect(whenHelper.DeletePipelineRollout(ctx, namespace, pipelineRolloutName)).To(Succeed())

		By("Checking if the PipelineRollout was successfully deleted")
		expectHelper.PipelineRolloutToBeDeleted(ctx, namespace, pipelineRolloutName)
	})

})
