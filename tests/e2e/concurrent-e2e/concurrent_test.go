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
	"strings"
	"testing"
	"time"

	. "github.com/numaproj/numaplane/tests/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	isbServiceRolloutName   = "test-isbservice-rollout"
	pipelineRolloutName     = "test-pipeline-rollout"
	monoVertexRolloutName   = "test-monovertex-rollout"
	initialJetstreamVersion = "2.10.17"
	updatedJetstreamVersion = "2.10.11"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	sourceVertexName    = "in"
	fourPods            = int32(4)
	fivePods            = int32(5)
	onePod              = int32(1)
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	initialPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: sourceVertexName,
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
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
				Scale: numaflowv1.Scale{ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: sourceVertexName,
				To:   "out",
			},
		},
	}

	dataLossPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Lifecycle: numaflowv1.Lifecycle{
			PauseGracePeriodSeconds: ptr.To(int64(120)),
		},
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: sourceVertexName,
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
					Builtin: &numaflowv1.Function{
						Name: "cat",
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
				From: sourceVertexName,
				To:   "cat",
			},
			{
				From: "cat",
				To:   "out",
			},
		},
	}

	monoVertexScaleMin = int32(4)
	monoVertexScaleMax = int32(5)
	monoVertexScaleTo  = int64(2)

	initialMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{
			Min: &monoVertexScaleMin,
			Max: &monoVertexScaleMax,
		},
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-go/source-simple-source:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}

	volSize, _     = apiresource.ParseQuantity("10Mi")
	isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
	updatedMemLimit, _            = apiresource.ParseQuantity("2Gi")
	ISBServiceSpecNoDataLossField = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: updatedJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
			ContainerTemplate: &numaflowv1.ContainerTemplate{
				Resources: v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceMemory: updatedMemLimit},
				},
			},
		},
	}

	revisedVolSize, _           = apiresource.ParseQuantity("20Mi")
	ISBServiceSpecRecreateField = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: updatedJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &revisedVolSize,
			},
			ContainerTemplate: &numaflowv1.ContainerTemplate{
				Resources: v1.ResourceRequirements{
					Limits: v1.ResourceList{v1.ResourceMemory: updatedMemLimit},
				},
			},
		},
	}
)

// TODO: test some of these scenarios of a concurrent update - should run for all 3 strategies
// PipelineRollout data loss, ISBService data loss, Numaflow Controller data loss (version), MonoVertex image
// PipelineRollout data loss, ISBService recreate, Numaflow Controller data loss (version), MonoVertex image
// PipelineRollout data loss, ISBService direct apply, Numaflow Controller data loss (version), MonoVertex image
// PipelineRollout direct apply, ISBService recreate, Numaflow Controller data loss (version), MonoVertex image
// PipelineRollout direct apply, ISBService data loss, Numaflow Controller data loss (version), MonoVertex image

func TestConcurrentE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Concurrent updates E2E Suite")
}

var _ = Describe("Pause and drain e2e", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false)
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil)
	})

	// current: testing first scenario
	// goal: loop this logic to test each scenario
	It("Should update all rollouts concurrently with data loss", func() {

		// TODO: consolidate in each individual file
		// update each rollout

		By("Updating PipelineRollout - data loss")
		rawSpec, err := json.Marshal(dataLossPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		// update the PipelineRollout
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawSpec
			return rollout, nil
		})

		By("Updating MonoVertexRollout - image")
		updatedMonoVertexSpec := initialMonoVertexSpec
		updatedMonoVertexSpec.Source.UDSource.Container.Image = "quay.io/numaio/numaflow-java/source-simple-source:stable"
		rawSpec, err = json.Marshal(updatedMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())
		// update the MonoVertexRollout
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawSpec
			return rollout, nil
		})

		By("Updating ISBServiceRollout - data loss")
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = updatedJetstreamVersion
		rawSpec, err = json.Marshal(updatedISBServiceSpec)
		Expect(err).ShouldNot(HaveOccurred())
		// update the ISBServiceRollout
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return rollout, nil
		})

		By("Updating NumaflowControllerRollout - data loss")
		UpdateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
			rollout.Spec = apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{Version: UpdatedNumaflowControllerVersion},
			}
			return rollout, nil
		})

		time.Sleep(10 * time.Second)

		// TODO: verify that each resource eventually has correct spec and is deployed/running
		// TODO: verify in progress strategy is no-strategy

		By("Verifying NumaflowController got updated")
		VerifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
			colon := strings.Index(d.Spec.Template.Spec.Containers[0].Image, ":")
			return colon != -1 && d.Spec.Template.Spec.Containers[0].Image[colon+1:] == "v"+UpdatedNumaflowControllerVersion
		})
		VerifyNumaflowControllerRolloutReady()

		By("Verifying ISBService got updated")
		VerifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
		})
		VerifyISBSvcRolloutReady(isbServiceRolloutName)

		By("Verifying Pipeline got updated")
		// get Pipeline to check that spec has been updated to correct spec
		VerifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == 3
		})
		VerifyPipelineRolloutDeployed(pipelineRolloutName)

		By("Verifying MonoVertexRollout got updated")
		VerifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return retrievedMonoVertexSpec.Source.UDSource.Container.Image == "quay.io/numaio/numaflow-java/source-simple-source:stable"
		})
		VerifyMonoVertexRolloutDeployed(monoVertexRolloutName)

		// case cleanup
		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()

	})

	// It("Should delete all rollout objects", func() {
	// 	DeleteMonoVertexRollout(monoVertexRolloutName)
	// 	DeletePipelineRollout(pipelineRolloutName)
	// 	DeleteISBServiceRollout(isbServiceRolloutName)
	// 	DeleteNumaflowControllerRollout()
	// })

})
