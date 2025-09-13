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
	fourPods            = int32(4)
	fivePods            = int32(5)
	onePod              = int32(1)
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	initialPipelineSpec = numaflowv1.PipelineSpec{
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
				Scale: numaflowv1.Scale{Min: &fourPods, Max: &fivePods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
						ImagePullPolicy: &pullPolicyAlways,
					},
				},
				Scale: numaflowv1.Scale{Min: &onePod, Max: &onePod, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)

	})

	It("Should create various Pipelines which will need to be drained and deleted", func() {
		// this will be the original successful Pipeline to drain
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil)

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
		time.Sleep(15 * time.Second)
		updatePipeline(failedPipelineSpec2)

		// verify it was assessed as failed
		VerifyPipelineRolloutProgressiveCondition(pipelineRolloutName, metav1.ConditionFalse)

		// restore PipelineRollout back to original spec
		updatePipeline(&initialPipelineSpec)

		/*pipelineDrained := map[int]bool{
			1: false,
			2: false,
		}

		// verify that pipelines 1-2 are drainedOnPause
		CheckEventually("Verifying that the failed Pipelines have been drained", func() bool {
			// if at any point the pipeline is drained, update the value in pipelineDrained array
			for index := 1; index <= 2; index++ {
				pipelineName := GetInstanceName(pipelineRolloutName, index)
				pipeline, err := GetPipelineByName(Namespace, pipelineName)
				if err != nil {
					continue
				}

				var retrievedPipelineStatus numaflowv1.PipelineStatus
				if retrievedPipelineStatus, err = GetPipelineStatus(pipeline); err != nil {
					return false
				}
				By("checking drainedOnPaused")
				if !pipelineDrained[index] && retrievedPipelineStatus.DrainedOnPause {
					pipelineDrained[index] = true
					fmt.Printf("setting pipeline drained for index %d\n", index)
					By(fmt.Sprintf("setting pipeline drained for index %d\n", index))
				}
			}

			return pipelineDrained[1] && pipelineDrained[2]

		}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), fmt.Sprintf("Pipelines weren't both drainedOnPause=true: %v", pipelineDrained))*/

		forceAppliedSpecPausing := map[int]bool{
			1: false,
			2: false,
		}

		CheckEventually("Verifying that the failed Pipelines have spec overridden", func() bool {
			// if at any point the pipeline is drained, update the value in pipelineDrained array
			for index := 1; index <= 2; index++ {
				pipelineName := GetInstanceName(pipelineRolloutName, index)
				pipeline, err := GetPipelineByName(Namespace, pipelineName)
				if err != nil {
					continue
				}

				var retrievedPipelineSpec numaflowv1.PipelineSpec
				if retrievedPipelineSpec, err = GetPipelineSpec(pipeline); err != nil {
					return false
				}

				annotations, found, err := unstructured.NestedMap(pipeline.Object, "metadata", "annotations")
				if !found || err != nil || annotations == nil {
					return false
				}
				// TODO: what if we try to check for phase==Pausing?
				if !forceAppliedSpecPausing[index] && annotations[common.AnnotationKeyOverriddenSpec] == "true" && retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused {
					forceAppliedSpecPausing[index] = true
					By(fmt.Sprintf("setting forceAppliedSpecPausing for index %d\n", index))
				}

			}

			return forceAppliedSpecPausing[1] && forceAppliedSpecPausing[2]

		}).WithTimeout(DefaultTestTimeout).Should(BeTrue(), fmt.Sprintf("Pipelines weren't both drainedOnPause=true: %v", forceAppliedSpecPausing))

		// verify that pipelines are deleted
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 1))
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 2))
	})

	// TODO: do one more test of upgrading to a good pipeline and make sure our original is drained

})

func updatePipeline(pipelineSpec *numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})
}
