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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	isbServiceRolloutName            = "test-isbservice-rollout"
	pipelineRolloutName              = "test-pipeline-rollout"
	monoVertexRolloutName            = "test-monovertex-rollout"
	initialNumaflowControllerVersion = "1.4.1"
	updatedNumaflowControllerVersion = "1.4.2"
	invalidNumaflowControllerVersion = "99.99.99"
	initialJetstreamVersion          = "2.10.17"
	updatedJetstreamVersion          = "2.10.11"

	// anotherPipelineRolloutName = "another-pipeline-rollout"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	numVertices         = int32(GetVerticesScaleValue())
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	currentPipelineSpec numaflowv1.PipelineSpec
	initialPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: PipelineSourceVertexName,
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: PipelineSourceVertexName,
				To:   "out",
			},
		},
	}

	updatedPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: PipelineSourceVertexName,
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Builtin: &numaflowv1.Function{
						Name: "cat",
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: PipelineSourceVertexName,
				To:   "cat",
			},
			{
				From: "cat",
				To:   "out",
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

	currentMonoVertexSpec numaflowv1.MonoVertexSpec
	initialMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-go/source-simple-source:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				UDSink: &numaflowv1.UDSink{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-go/sink-log:stable",
					},
				},
			},
		},
	}
)

func TestFunctionalE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Functional E2E Suite")
}

var _ = Describe("Functional e2e", Serial, func() {

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {
		CreateNumaflowControllerRollout(initialNumaflowControllerVersion)
	})

	It("Should create the ISBServiceRollout if it doesn't exist", func() {
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should create the PipelineRollout if it does not exist", func() {
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false)
	})

	// extra PipelineRollout to verify that multiple Pipelines will behave correctly during ISBService/controller updates
	// It("Should create another PipelineRollout if it does not exist", func() {
	// 	CreatePipelineRollout(anotherPipelineRolloutName, Namespace, initialPipelineSpec, false)
	// })

	currentPipelineSpec = initialPipelineSpec

	It("Should create the MonoVertexRollout if it does not exist", func() {
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec)
	})

	currentMonoVertexSpec = initialMonoVertexSpec

	time.Sleep(2 * time.Second)

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		Document("Updating Pipeline directly")

		// update child Pipeline
		UpdatePipelineSpecInK8S(Namespace, pipelineRolloutName, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			pipelineSpec.Watermark.Disabled = true
			return pipelineSpec, nil
		})

		if UpgradeStrategy == config.PPNDStrategyID {
			Document("Verify that child Pipeline is not paused when an update not requiring pause is made")
			VerifyPipelineStatusConsistently(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused
			})
		}

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		Document("Verifying self-healing")
		VerifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return !retrievedPipelineSpec.Watermark.Disabled
		})

		VerifyPipelineRolloutDeployed(pipelineRolloutName)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)

		VerifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyPipelineRunning(Namespace, pipelineRolloutName, true)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {

		numPipelineVertices := len(updatedPipelineSpec.Vertices)
		UpdatePipelineRollout(pipelineRolloutName, updatedPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == numPipelineVertices
		}, true, true)

	})

	currentPipelineSpec = updatedPipelineSpec

	time.Sleep(2 * time.Second)

	It("Should pause the Pipeline if user requests it", func() {

		Document("setting desiredPhase=Paused")
		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused

		UpdatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhasePaused, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused
		}, false, false)

		VerifyPipelineStaysPaused(pipelineRolloutName)
	})

	time.Sleep(2 * time.Second)

	It("Should resume the Pipeline if user requests it", func() {

		Document("setting desiredPhase=Running")
		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning

		UpdatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhaseRunning
		}, false, false)
	})

	It("Should pause the MonoVertex if user requests it", func() {

		Document("setting desiredPhase=Paused")
		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhasePaused

		UpdateMonoVertexRollout(monoVertexRolloutName, currentMonoVertexSpec, numaflowv1.MonoVertexPhasePaused, func(spec numaflowv1.MonoVertexSpec) bool {
			return spec.Lifecycle.DesiredPhase == numaflowv1.MonoVertexPhasePaused
		})

		VerifyMonoVertexStaysPaused(monoVertexRolloutName)
	})

	time.Sleep(2 * time.Second)

	It("Should resume the MonoVertex if user requests it", func() {

		Document("setting desiredPhase=Running")
		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhaseRunning

		UpdateMonoVertexRollout(monoVertexRolloutName, currentMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning, func(spec numaflowv1.MonoVertexSpec) bool {
			return spec.Lifecycle.DesiredPhase == numaflowv1.MonoVertexPhaseRunning
		})

	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {
		UpdateNumaflowControllerRollout(initialNumaflowControllerVersion, updatedNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, true)
	})

	time.Sleep(2 * time.Second)

	It("Should fail if the NumaflowControllerRollout is updated with a bad version", func() {
		UpdateNumaflowControllerRollout(updatedNumaflowControllerVersion, invalidNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, false)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is restored back to previous version", func() {
		UpdateNumaflowControllerRollout(invalidNumaflowControllerVersion, updatedNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, true)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {

		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = updatedJetstreamVersion

		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName, OverrideSourceVertexReplicas: true}}, updatedISBServiceSpec, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
		}, true, false)

		// UpdateISBServiceRollout(isbServiceRolloutName, []string{pipelineRolloutName, anotherPipelineRolloutName}, updatedISBServiceSpec, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
		// 	return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
		// }, true, false, true, false)

	})

	It("Should update the child ISBService updating a no-data-loss field", func() {

		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, ISBServiceSpecNoDataLossField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
				*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
		}, false, false)

		// UpdateISBServiceRollout(isbServiceRolloutName, []string{pipelineRolloutName, anotherPipelineRolloutName}, ISBServiceSpecNoDataLossField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
		// 	return retrievedISBServiceSpec.JetStream != nil &&
		// 		retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
		// 		retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
		// 		*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
		// }, false, false, false, false)

	})

	It("Should update the child ISBService updating a recreate field", func() {

		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName, OverrideSourceVertexReplicas: true}}, ISBServiceSpecRecreateField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(revisedVolSize)
		}, false, true)

		// UpdateISBServiceRollout(isbServiceRolloutName, []string{pipelineRolloutName, anotherPipelineRolloutName}, ISBServiceSpecRecreateField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
		// 	return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(revisedVolSize)
		// }, false, true, true, false)

	})

	It("Should update child MonoVertex if the MonoVertexRollout is updated", func() {

		// new MonoVertex spec
		updatedMonoVertexSpec := initialMonoVertexSpec
		updatedMonoVertexSpec.Source.UDSource = nil
		rpu := int64(10)
		updatedMonoVertexSpec.Source.Generator = &numaflowv1.GeneratorSource{RPU: &rpu}

		UpdateMonoVertexRollout(monoVertexRolloutName, updatedMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning, func(spec numaflowv1.MonoVertexSpec) bool {
			return spec.Source != nil && spec.Source.Generator != nil && *spec.Source.Generator.RPU == rpu
		})

		VerifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return retrievedMonoVertexSpec.Source.Generator != nil && retrievedMonoVertexSpec.Source.UDSource == nil
		})

	})

	It("Should only be one child per Rollout", func() { // all prior children should be marked "Recyclable" and deleted
		Document("verifying just 1 Pipeline")
		Eventually(func() int {
			return GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
		}, TestTimeout, TestPollingInterval).Should(Equal(1))

		Document("verifying just 1 MonoVertex")
		Eventually(func() int {
			return GetNumberOfChildren(GetGVRForMonoVertex(), Namespace, monoVertexRolloutName)
		}, TestTimeout, TestPollingInterval).Should(Equal(1))

		Document("verifying just 1 InterstepBufferService")
		Eventually(func() int {
			return GetNumberOfChildren(GetGVRForISBService(), Namespace, isbServiceRolloutName)
		}, TestTimeout, TestPollingInterval).Should(Equal(1))
	})

	It("Should delete the PipelineRollouts and child Pipelines", func() {
		DeletePipelineRollout(pipelineRolloutName)
		// DeletePipelineRollout(anotherPipelineRolloutName)
	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {
		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete the ISBServiceRollout and child ISBService", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {
		DeleteNumaflowControllerRollout()
	})

})
