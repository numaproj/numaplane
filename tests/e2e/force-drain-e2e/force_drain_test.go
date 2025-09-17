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
)

func TestForceDrainE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Force Drain - E2E Suite")
}

var _ = Describe("Force Drain e2e", Serial, func() {

	It("Should create NumaflowControllerRollout and ISBServiceRollout", func() {
		CreateNumaflowControllerRollout(UpdatedNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)

		// this will be the original successful Pipeline to drain
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil)
	})

	It("Should create 2 failed Pipelines which will need to be drained and deleted and update back to original Pipeline", func() {

		updateToFailedPipelines()

		// restore PipelineRollout back to original spec
		updatePipeline(&initialPipelineSpec)

		verifyPipelinesPausingWithValidSpecAndDeleted([]int{1, 2})
	})

	It("Should create 2 failed Pipelines which will need to be drained and deleted and update to new Pipeline", func() {

		updateToFailedPipelines()

		// updated Pipeline inserts another "cat" vertex in the middle
		updatedPipelineSpec := initialPipelineSpec
		/*catVertex := initialPipelineSpec.Vertices[1].DeepCopy()
		catVertex.Name = "cat2"
		outVertex := initialPipelineSpec.Vertices[2].DeepCopy()
		updatedPipelineSpec.Vertices = append(updatedPipelineSpec.Vertices, *outVertex)
		updatedPipelineSpec.Vertices[2] = *catVertex
		updatedPipelineSpec.Edges = []numaflowv1.Edge{
			{From: "in", To: "cat"},
			{From: "cat", To: "cat2"},
			{From: "cat2", To: "out"},
		}*/
		updatedPipelineSpec.Vertices[2] = numaflowv1.AbstractVertex{
			Name: "out",
			Sink: &numaflowv1.Sink{
				AbstractSink: numaflowv1.AbstractSink{
					Blackhole: &numaflowv1.Blackhole{},
				},
			},
		}

		// restore PipelineRollout back to original spec
		updatePipeline(&updatedPipelineSpec)

		verifyPipelinesPausingWithValidSpecAndDeleted([]int{0, 3, 4})
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
func updateToFailedPipelines() {
	// this will be a failed Pipeline which will be replaced before it has a chance to be assessed
	failedPipelineSpec1 := initialPipelineSpec.DeepCopy()
	failedPipelineSpec1.Vertices[1] = numaflowv1.AbstractVertex{
		Name: "cat",
		UDF: &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image: "badpath1",
			},
		},
	}

	time.Sleep(5 * time.Second)
	updatePipeline(failedPipelineSpec1)

	// this will be a failed Pipeline which will be assessed as Failed
	failedPipelineSpec2 := initialPipelineSpec.DeepCopy()
	failedPipelineSpec2.Vertices[1] = numaflowv1.AbstractVertex{
		Name: "cat",
		UDF: &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image: "badpath2",
			},
		},
	}
	time.Sleep(30 * time.Second)
	updatePipeline(failedPipelineSpec2)

	// verify it was assessed as failed
	VerifyPipelineRolloutProgressiveCondition(pipelineRolloutName, metav1.ConditionFalse)
}

func verifyPipelinesPausingWithValidSpecAndDeleted(pipelineIndices []int) {

	forceAppliedSpecPausing := map[int]bool{}

	for _, pipelineIndex := range pipelineIndices {
		forceAppliedSpecPausing[pipelineIndex] = false
	}

	CheckEventually("Verifying that the failed Pipelines have spec overridden", func() bool {
		// if at any point the pipeline is pausing with its spec overridden, update the value in the forceAppliedSpecPausing array
		for _, pipelineIndex := range pipelineIndices {
			pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
			pipeline, err := GetPipelineByName(Namespace, pipelineName)
			if err != nil {
				continue
			}

			var retrievedPipelineSpec numaflowv1.PipelineSpec
			if retrievedPipelineSpec, err = GetPipelineSpec(pipeline); err != nil {
				return false
			}

			var retrievedPipelineStatus numaflowv1.PipelineStatus
			if retrievedPipelineStatus, err = GetPipelineStatus(pipeline); err != nil {
				return false
			}

			annotations, found, err := unstructured.NestedMap(pipeline.Object, "metadata", "annotations")
			if !found || err != nil || annotations == nil {
				return false
			}

			if !forceAppliedSpecPausing[pipelineIndex] && /*annotations[common.AnnotationKeyOverriddenSpec] == "true"*/
				retrievedPipelineSpec.Vertices[1].UDF != nil && retrievedPipelineSpec.Vertices[1].UDF.Container != nil &&
				retrievedPipelineSpec.Vertices[1].UDF.Container.Image == validImagePath &&
				retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused &&
				//(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing ||
				(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause) {
				forceAppliedSpecPausing[pipelineIndex] = true
				By(fmt.Sprintf("setting forceAppliedSpecPausing for index %d\n", pipelineIndex))
			}

		}

		// check if all Pipelines have met the criteria
		for _, pipelineIndex := range pipelineIndices {
			if !forceAppliedSpecPausing[pipelineIndex] {
				return false
			}
		}
		return true

	}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), fmt.Sprintf("Pipelines weren't both drainedOnPause=true: %v", forceAppliedSpecPausing))

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
