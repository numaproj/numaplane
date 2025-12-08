/*
Copyright 2025.

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
	"fmt"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
)

var (
	volSize, _            = apiresource.ParseQuantity("10Mi")
	initialISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: InitialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}

	pipelineSpecSourceRPU      = int64(1000)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	pullPolicyAlways    = corev1.PullAlways
	validImagePath      = "quay.io/numaio/numaflow-go/map-cat:stable"
	onePod              = int32(1)
	twoPods             = int32(2)
	threePods           = int32(3)
	fourPods            = int32(4)
	fivePods            = int32(5)
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	initialPipelineSpec = numaflowv1.PipelineSpec{
		Templates: &numaflowv1.Templates{
			VertexTemplate: &numaflowv1.VertexTemplate{
				ContainerTemplate: &numaflowv1.ContainerTemplate{
					Env: []corev1.EnvVar{
						{Name: "NUMAFLOW_DEBUG", Value: "true"},
					},
				},
			},
		},
		InterStepBufferServiceName: isbServiceRolloutName,
		Lifecycle: numaflowv1.Lifecycle{
			PauseGracePeriodSeconds: ptr.To(int64(60)),
		},
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &twoPods, Max: &threePods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           validImagePath,
						ImagePullPolicy: &pullPolicyAlways,
					},
				},
				Scale: numaflowv1.Scale{Min: &fourPods, Max: &fivePods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &onePod, Max: &onePod, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "cat",
			},
			{
				From: "cat",
				To:   "out",
			},
		},
	}

	updatedPipelineSpec *numaflowv1.PipelineSpec
)

func init() {
	updatedPipelineSpec = initialPipelineSpec.DeepCopy()
	updatedPipelineSpec.Vertices[2] = numaflowv1.AbstractVertex{
		Name: "out",
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}
}

func TestForceDrainE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Force Drain - E2E Suite")
}

var _ = Describe("Force Drain e2e", Serial, func() {

	It("Should create NumaflowControllerRollout, ISBServiceRollout, PipelineRollout", func() {
		CreateNumaflowControllerRollout(UpdatedNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)

		// this will be the original successful Pipeline to drain
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil, apiv1.Metadata{})
	})

	It("Should create 2 failed Pipelines which will need to be drained and deleted and update back to original Pipeline", func() {

		// update twice
		// first this creates "test-pipeline-rollout-1", then "test-pipeline-rollout-2"
		updateFailedPipelinesBackToBack(1)

		// restore PipelineRollout back to original spec
		updatePipeline(&initialPipelineSpec)

		verifyPipelinesPausingWithValidSpecAndDeleted([]int{1, 2})

		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 1), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 2), "Normal")
	})

	It("Should create 3 failed Pipelines which attempt to drain with an unhealthy Pipeline spec", func() {

		// update twice
		// first this creates "test-pipeline-rollout-3", then "test-pipeline-rollout-4"
		updateFailedPipelinesBackToBack(3)

		// Force promote test-pipeline-rollout-4 (which is unhealthy)
		// This enables us to see that test-pipeline-rollout-0 and test-pipeline-rollout-3 start trying to drain
		forcePromote(pipelineRolloutName, 4)

		// test-pipeline-rollout-0 should be able to drain with its original spec
		// Verify that test-pipeline-rollout-0 and test-pipeline-rollout-3 have the spec from test-pipeline-rollout-4
		// and are pausing
		verifyPipelineHasImage(0, validImagePath)
		verifyPipelineHasImage(3, "badpath2")
		VerifyPipelineDesiredPhase(GetInstanceName(pipelineRolloutName, 0), numaflowv1.PipelinePhasePaused)
		VerifyPipelineDesiredPhase(GetInstanceName(pipelineRolloutName, 3), numaflowv1.PipelinePhasePaused)
		// get the annotation numaflow.numaproj.io/pause-timestamp for test-pipeline-rollout-3

		// verify test-pipeline-rollout-0 and test-pipeline-rollout-3 hav status.phase==Pausing
		VerifyPipelinePhase(Namespace, GetInstanceName(pipelineRolloutName, 0), []numaflowv1.PipelinePhase{numaflowv1.PipelinePhasePausing, numaflowv1.PipelinePhasePaused})
		VerifyPipelinePhase(Namespace, GetInstanceName(pipelineRolloutName, 3), []numaflowv1.PipelinePhase{numaflowv1.PipelinePhasePausing, numaflowv1.PipelinePhasePaused})

		pauseTimestamp3Orig, err := GetAnnotation(Namespace, GetInstanceName(pipelineRolloutName, 3), "numaflow.numaproj.io/pause-timestamp")
		Expect(err).ShouldNot(HaveOccurred())
		fmt.Printf("pauseTimestamp3Orig: %s\n", pauseTimestamp3Orig)

		// create test-pipeline-rollout-5 which is also unhealthy
		updatePipelineImage("badpath3")
		// check that test-pipeline-rollout-5 has been created
		verifyPipelinesUpgrading(5)

		// force promote it so that test-pipeline-rollout-3 and test-pipeline-rollout-4 start trying to drain with this spec
		// once they are done with the previous drain attempt
		forcePromote(pipelineRolloutName, 5)
		verifyPipelineHasImage(3, "badpath3")
		verifyPipelineHasImage(4, "badpath3")
		VerifyPipelineDesiredPhase(GetInstanceName(pipelineRolloutName, 3), numaflowv1.PipelinePhasePaused)
		VerifyPipelineDesiredPhase(GetInstanceName(pipelineRolloutName, 4), numaflowv1.PipelinePhasePaused)
		VerifyPipelinePhase(Namespace, GetInstanceName(pipelineRolloutName, 3), []numaflowv1.PipelinePhase{numaflowv1.PipelinePhasePausing, numaflowv1.PipelinePhasePaused})
		VerifyPipelinePhase(Namespace, GetInstanceName(pipelineRolloutName, 4), []numaflowv1.PipelinePhase{numaflowv1.PipelinePhasePausing, numaflowv1.PipelinePhasePaused})

		// get the annotation numaflow.numaproj.io/pause-timestamp for test-pipeline-rollout-3 to make sure that it's
		// different from the previous pause timestamp, indicating that a new pause has started
		pauseTimestamp3, err := GetAnnotation(Namespace, GetInstanceName(pipelineRolloutName, 3), "numaflow.numaproj.io/pause-timestamp")
		Expect(err).ShouldNot(HaveOccurred())
		fmt.Printf("pauseTimestamp3: %s\n", pauseTimestamp3)
		Expect(pauseTimestamp3).ShouldNot(Equal(pauseTimestamp3Orig))

		// verify that test-pipeline-rollout-3, and test-pipeline-rollout-4 have finished trying to drain
		VerifyPipelineSpecStatus(Namespace, GetInstanceName(pipelineRolloutName, 3), func(spec numaflowv1.PipelineSpec, status numaflowv1.PipelineStatus) bool {
			return status.Phase == numaflowv1.PipelinePhasePaused && !status.DrainedOnPause
		})
		VerifyPipelineSpecStatus(Namespace, GetInstanceName(pipelineRolloutName, 4), func(spec numaflowv1.PipelineSpec, status numaflowv1.PipelineStatus) bool {
			return status.Phase == numaflowv1.PipelinePhasePaused && !status.DrainedOnPause
		})

		// verify that test-pipeline-rollout-0, test-pipeline-rollout-3, and test-pipeline-rollout-4 scale to 0 replicas, waiting for the next "promoted" pipeline spec
		verifyPipelineScaledToZero(3)
		verifyPipelineScaledToZero(4)

		// verify that test-pipeline-rollout-0 was able to be deleted because it drained fully
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 0))
	})

	It("Should successfully drain all previous Pipelines once there's a Pipeline with a healthy spec", func() {

		// updated Pipeline updates the sink ("test-pipeline-rollout-6")
		updatePipeline(updatedPipelineSpec)

		verifyPipelinesPausingWithValidSpecAndDeleted([]int{3, 4, 5})

		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 0), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 3), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 4), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 5), "Normal")

	})

	It("Should reach max recyclable duration and delete the Pipeline", func() {

		// set numaplane controller config to have max recyclable duration of 2 minutes
		UpdateNumaplaneControllerConfig(map[string]string{
			"pipeline.maxRecyclableDurationMinutes": "2",
		})

		// create test-pipeline-rollout-7 and test-pipeline-rollout-8 and force promote 8
		updateFailedPipelinesBackToBack(7)
		forcePromote(pipelineRolloutName, 8)

		// now let's make sure that test-pipeline-rollout-7 gets deleted even though it can't drain successfully
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 7))
	})

	It("Should Delete Rollouts", func() {
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

// update twice:
// the first pipeline will be failed but it will be replaced before it's assessed
// the second pipeline will actually be assessed as Failed
// We test both since they take slightly different paths in the recycle code
// nextIndex is the expected index of the first pipeline that will be created (second will be nextIndex+1)
func updateFailedPipelinesBackToBack(nextIndex int) {
	// this will be a failed Pipeline which will be replaced before it has a chance to be assessed
	time.Sleep(10 * time.Second)
	updatePipelineImage("badpath1")

	// verify the first pipeline was created
	verifyPipelinesUpgrading(nextIndex)

	// this will be a failed Pipeline which will be assessed as Failed
	time.Sleep(45 * time.Second)
	updatePipelineImage("badpath2")

	// verify the second pipeline was created
	verifyPipelinesUpgrading(nextIndex + 1)

	// verify it was assessed as failed
	VerifyPipelineRolloutProgressiveCondition(pipelineRolloutName, metav1.ConditionFalse)
}

func verifyPipelinesPausingWithValidSpecAndDeleted(pipelineIndices []int) {

	pausingWithCorrectSpec := map[int]bool{}

	for _, pipelineIndex := range pipelineIndices {
		pausingWithCorrectSpec[pipelineIndex] = false
	}

	CheckEventually(fmt.Sprintf("Verifying that the failed Pipeline(s) (%v) have spec overridden", pipelineIndices), func() bool {
		// if at any point the pipeline is pausing with its spec overridden, update the value in the forceAppliedSpecPausing array
		for _, pipelineIndex := range pipelineIndices {
			pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
			pipeline, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(Namespace, pipelineName)
			if err != nil {
				continue
			}

			annotations, found, err := unstructured.NestedMap(pipeline.Object, "metadata", "annotations")
			if !found || err != nil || annotations == nil {
				return false
			}

			if !pausingWithCorrectSpec[pipelineIndex] &&
				retrievedPipelineSpec.Vertices[1].UDF != nil && retrievedPipelineSpec.Vertices[1].UDF.Container != nil &&
				retrievedPipelineSpec.Vertices[1].UDF.Container.Image == validImagePath &&
				retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused &&
				// we check for either Pausing or Paused w/ drainedOnPause
				// just the latter would be a better check, but sometimes the test isn't quick enough to catch it before the pipeline is deleted
				(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing ||
					(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause)) {
				pausingWithCorrectSpec[pipelineIndex] = true
				By(fmt.Sprintf("setting pausingWithCorrectSpec for index %d\n", pipelineIndex))
			}

		}

		// check if all Pipelines have met the criteria
		for _, pipelineIndex := range pipelineIndices {
			if !pausingWithCorrectSpec[pipelineIndex] {
				return false
			}

		}
		return true

	}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), fmt.Sprintf("Pipelines weren't both drainedOnPause=true: %v", pausingWithCorrectSpec))

	// verify that pipelines are deleted
	for _, pipelineIndex := range pipelineIndices {
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, pipelineIndex))
	}

}

func updatePipeline(pipelineSpec *numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})
}

func forcePromote(pipelineRolloutName string, pipelineIndex int) {
	pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	By(fmt.Sprintf("Force promoting Pipeline %s", pipelineName))
	UpdatePipelineInK8S(pipelineName, func(pipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		labels := pipeline.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[common.LabelKeyForcePromote] = "true"
		pipeline.SetLabels(labels)
		return pipeline, nil
	})
}

func verifyPipelineHasImage(pipelineIndex int, expectedImage string) {
	pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	By(fmt.Sprintf("Verifying that Pipeline %s has image %s", pipelineName, expectedImage))
	VerifyPipelineSpecStatus(Namespace, pipelineName, func(spec numaflowv1.PipelineSpec, status numaflowv1.PipelineStatus) bool {
		// Check the "cat" vertex (index 1) for the expected image
		return spec.Vertices[1].UDF != nil &&
			spec.Vertices[1].UDF.Container != nil &&
			spec.Vertices[1].UDF.Container.Image == expectedImage
	})
}

func updatePipelineImage(imagePath string) *numaflowv1.PipelineSpec {
	By(fmt.Sprintf("Updating PipelineRollout with image %s", imagePath))
	pipelineSpec := initialPipelineSpec.DeepCopy()
	pipelineSpec.Vertices[1] = numaflowv1.AbstractVertex{
		Name: "cat",
		UDF: &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image: imagePath,
			},
		},
	}
	updatePipeline(pipelineSpec)
	return pipelineSpec
}

func verifyPipelinesUpgrading(pipelineIndex int) {
	expectedPipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	By(fmt.Sprintf("Verifying that Pipeline %s is in the upgrading pipelines list", expectedPipelineName))
	CheckEventually(fmt.Sprintf("Verifying that Pipeline %s is in the upgrading pipelines list", expectedPipelineName), func() bool {
		upgradingPipelines, err := GetUpgradingPipelines(Namespace, pipelineRolloutName)
		if err != nil {
			return false
		}
		for _, pipeline := range upgradingPipelines.Items {
			if pipeline.GetName() == expectedPipelineName {
				return true
			}
		}
		return false
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue())
}

func verifyPipelineScaledToZero(pipelineIndex int) {
	pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	By(fmt.Sprintf("Verifying that Pipeline %s is scaled to 0 replicas", pipelineName))
	VerifyPipelineSpecStatus(Namespace, pipelineName, func(spec numaflowv1.PipelineSpec, status numaflowv1.PipelineStatus) bool {
		// Check that all vertices have scale.min and scale.max equal to 0
		for _, vertex := range spec.Vertices {
			if vertex.Scale.Min == nil || *vertex.Scale.Min != 0 {
				return false
			}
			if vertex.Scale.Max == nil || *vertex.Scale.Max != 0 {
				return false
			}
		}
		return true
	})
}
