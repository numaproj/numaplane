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
	isbServiceRolloutName   = "test-isbservice-rollout"
	pipelineRolloutName     = "test-pipeline-rollout"
	initialJetstreamVersion = "2.10.17"
	updatedJetstreamVersion = "2.10.11"

	// anotherPipelineRolloutName = "another-pipeline-rollout"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	sourceVertexName     = "in"
	sourceVertexScaleMin = int32(4)
	sourceVertexScaleMax = int32(5)
	sourceVertexScaleTo  = int64(2)
	onePod               = int32(1)
	zeroReplicaSleepSec  = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	initialPipelineSpec  = numaflowv1.PipelineSpec{
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
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

	updatedPipelineSpec = numaflowv1.PipelineSpec{
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
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

func TestFunctionalE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Functional E2E Suite")
}

var _ = Describe("Functional e2e:", Serial, func() {

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
	})

	It("Should create the ISBServiceRollout if it doesn't exist", func() {
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should create the PipelineRollout if it does not exist", func() {
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false)
	})

	It("Should automatically heal a Pipeline if it is updated directly", func() {
		By("Updating Pipeline directly")

		// update child Pipeline
		UpdatePipelineSpecInK8S(Namespace, pipelineRolloutName, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			pipelineSpec.Watermark.Disabled = true
			return pipelineSpec, nil
		})

		if UpgradeStrategy == config.PPNDStrategyID {
			By("Verify that child Pipeline is not paused when an update not requiring pause is made")
			VerifyPipelineStatusConsistently(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused
			})
		}

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		By("Verifying self-healing")
		VerifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return !retrievedPipelineSpec.Watermark.Disabled
		})

		VerifyPipelineRolloutDeployed(pipelineRolloutName)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)

		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {
		numPipelineVertices := len(updatedPipelineSpec.Vertices)

		/*expectedPipelineTypeProgressiveStatusInProgress, expectedPipelineTypeProgressiveStatusOnDone := MakeExpectedPipelineTypeProgressiveStatus(
			GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), sourceVertexName,
			sourceVertexScaleTo, sourceVertexScaleTo,
			fmt.Sprintf("{\"max\":%d,\"min\":%d}", sourceVertexScaleMax, sourceVertexScaleMin),
			apiv1.AssessmentResultUnknown, apiv1.AssessmentResultSuccess,
		)*/

		UpdatePipelineRollout(pipelineRolloutName, updatedPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == numPipelineVertices
		}, true, true)
	})

	It("Should pause the Pipeline if user requests it", func() {
		By("setting desiredPhase=Paused")
		currentPipelineSpec := updatedPipelineSpec
		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused

		UpdatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhasePaused, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused
		}, false, false)

		VerifyPipelineStaysPaused(pipelineRolloutName)
	})

	It("Should resume the Pipeline if user requests it", func() {
		By("setting desiredPhase=Running")
		currentPipelineSpec := updatedPipelineSpec
		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning

		UpdatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhaseRunning
		}, false, false)
	})

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {
		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = updatedJetstreamVersion

		/*expectedPipelineTypeProgressiveStatusInProgress, expectedPipelineTypeProgressiveStatusOnDone := MakeExpectedPipelineTypeProgressiveStatus(
			GetInstanceName(pipelineRolloutName, 1), GetInstanceName(pipelineRolloutName, 2), sourceVertexName,
			sourceVertexScaleTo, sourceVertexScaleTo,
			fmt.Sprintf("{\"max\":%d,\"min\":%d}", sourceVertexScaleMax, sourceVertexScaleMin),
			apiv1.AssessmentResultUnknown, apiv1.AssessmentResultSuccess,
		)*/

		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, updatedISBServiceSpec, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
		}, true, false, true)

	})

	It("Should update the child ISBService updating a no-data-loss field", func() {
		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, ISBServiceSpecNoDataLossField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
				*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
		}, false, false, false)
	})

	It("Should update the child ISBService updating a recreate field", func() {
		/*expectedPipelineTypeProgressiveStatusInProgress, expectedPipelineTypeProgressiveStatusOnDone := MakeExpectedPipelineTypeProgressiveStatus(
			GetInstanceName(pipelineRolloutName, 2), GetInstanceName(pipelineRolloutName, 3), sourceVertexName,
			sourceVertexScaleTo, sourceVertexScaleTo,
			fmt.Sprintf("{\"max\":%d,\"min\":%d}", sourceVertexScaleMax, sourceVertexScaleMin),
			apiv1.AssessmentResultUnknown, apiv1.AssessmentResultSuccess,
		)*/

		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, ISBServiceSpecRecreateField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(revisedVolSize)
		}, false, true, true)
	})

	It("Should only be one child per Rollout", func() { // all prior children should be marked "Recyclable" and deleted
		CheckEventually("verifying just 1 Pipeline", func() int {
			return GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
		}).Should(Equal(1))

		CheckEventually("verifying just 1 InterstepBufferService", func() int {
			return GetNumberOfChildren(GetGVRForISBService(), Namespace, isbServiceRolloutName)
		}).Should(Equal(1))
	})

	It("Should delete the PipelineRollouts and child Pipelines", func() {
		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should delete the ISBServiceRollout and child ISBService", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {
		DeleteNumaflowControllerRollout()
	})
})
