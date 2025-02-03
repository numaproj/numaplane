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
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
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
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	numVertices         = int32(1)
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	currentPipelineSpec numaflowv1.PipelineSpec
	initialPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
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
				From: "in",
				To:   "out",
			},
		},
	}

	updatedPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
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
				From: "in",
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

	pipelinegvr   schema.GroupVersionResource
	isbservicegvr schema.GroupVersionResource
	monovertexgvr schema.GroupVersionResource
)

func TestFunctionalE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		beforeSuiteSetup()
	})

	RunSpecs(t, "Functional E2E Suite")
}

func init() {

	pipelinegvr = getGVRForPipeline()
	isbservicegvr = getGVRForISBService()
	monovertexgvr = getGVRForMonoVertex()

}

var _ = Describe("Functional e2e", Serial, func() {

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {
		createNumaflowControllerRollout(initialNumaflowControllerVersion)
	})

	It("Should create the ISBServiceRollout if it doesn't exist", func() {
		createISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should create the PipelineRollout if it does not exist", func() {
		createPipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec)
	})

	currentPipelineSpec = initialPipelineSpec

	It("Should create the MonoVertexRollout if it does not exist", func() {
		createMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec)
	})

	currentMonoVertexSpec = initialMonoVertexSpec

	time.Sleep(2 * time.Second)

	It("Should automatically heal a Pipeline if it is updated directly", func() {

		document("Updating Pipeline directly")

		// update child Pipeline
		updatePipelineSpecInK8S(Namespace, pipelineRolloutName, func(pipelineSpec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			pipelineSpec.Watermark.Disabled = true
			return pipelineSpec, nil
		})

		if upgradeStrategy == config.PPNDStrategyID {
			document("Verify that child Pipeline is not paused when an update not requiring pause is made")
			verifyPipelineStatusConsistently(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused
			})
		}

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		document("Verifying self-healing")
		verifyPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return !retrievedPipelineSpec.Watermark.Disabled
		})

		verifyPipelineRolloutDeployed(pipelineRolloutName)
		verifyPipelineRolloutHealthy(pipelineRolloutName)

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPipelineRunning(Namespace, pipelineRolloutName)

	})

	time.Sleep(2 * time.Second)

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {

		updatePipelineRollout(pipelineRolloutName, updatedPipelineSpec, numaflowv1.PipelinePhaseRunning, true)

	})

	currentPipelineSpec = updatedPipelineSpec

	time.Sleep(2 * time.Second)

	// TODO: https://github.com/numaproj/numaplane/issues/509:
	// move this into a ppnd-specific test suite
	It("Should allow data loss in the Pipeline if requested (PPND)", func() {

		if upgradeStrategy == config.PPNDStrategyID {

			slowPipelineRolloutName := "slow-pipeline-rollout"

			document("Creating a slow pipeline")
			slowPipelineSpec := updatedPipelineSpec.DeepCopy()
			highRPU := int64(10000000)
			readBatchSize := uint64(1)
			pauseGracePeriodSeconds := int64(3600) // numaflow will try to pause for 1 hour if we let it
			slowPipelineSpec.Lifecycle.PauseGracePeriodSeconds = &pauseGracePeriodSeconds
			slowPipelineSpec.Limits = &numaflowv1.PipelineLimits{ReadBatchSize: &readBatchSize}
			slowPipelineSpec.Vertices[0].Source.Generator.RPU = &highRPU
			slowPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{Container: &numaflowv1.Container{
				Image: "quay.io/numaio/numaflow-go/map-slow-cat:stable",
			}}

			pipelineRolloutSpec := createPipelineRolloutSpec(slowPipelineRolloutName, Namespace, *slowPipelineSpec)
			_, err := pipelineRolloutClient.Create(ctx, pipelineRolloutSpec, metav1.CreateOptions{})
			Expect(err).ShouldNot(HaveOccurred())

			document("Verifying that the Pipeline was created")
			verifyPipelineSpec(Namespace, slowPipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
				return len(slowPipelineSpec.Vertices) == len(retrievedPipelineSpec.Vertices)
			})

			verifyPipelineRunning(Namespace, slowPipelineRolloutName)
			verifyInProgressStrategy(slowPipelineRolloutName, apiv1.UpgradeStrategyNoOp)

			document("Updating Pipeline Topology to cause a PPND change")
			slowPipelineSpec.Vertices[1] = slowPipelineSpec.Vertices[2]
			slowPipelineSpec.Vertices = slowPipelineSpec.Vertices[0:2]
			slowPipelineSpec.Edges = []numaflowv1.Edge{
				{
					From: "in",
					To:   "out",
				},
			}
			rawSpec, err := json.Marshal(slowPipelineSpec)
			Expect(err).ShouldNot(HaveOccurred())

			updatePipelineRolloutInK8S(Namespace, slowPipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
				rollout.Spec.Pipeline.Spec.Raw = rawSpec
				return rollout, nil
			})

			document("Verifying that Pipeline tries to pause")
			verifyPipelineStatusEventually(Namespace, slowPipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing
			})
			document("Verifying that Pipeline keeps trying to pause")
			verifyPipelineStatusConsistently(Namespace, slowPipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing
			})

			document("Updating PipelineRollout to allow data loss")

			// update the PipelineRollout to allow data loss temporarily
			updatePipelineRolloutInK8S(Namespace, slowPipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
				if rollout.Annotations == nil {
					rollout.Annotations = make(map[string]string)
				}
				rollout.Annotations[common.LabelKeyAllowDataLoss] = "true"
				return rollout, nil
			})

			document("Verifying that Pipeline has stopped trying to pause")
			verifyPipelineRunning(Namespace, slowPipelineRolloutName)

			document("Deleting Slow PipelineRollout")

			err = pipelineRolloutClient.Delete(ctx, slowPipelineRolloutName, metav1.DeleteOptions{})
			Expect(err).ShouldNot(HaveOccurred())

		}
	})

	time.Sleep(2 * time.Second)

	It("Should pause the Pipeline if user requests it", func() {

		document("setting desiredPhase=Paused")
		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused

		updatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhasePaused, false)

		document("verifying Pipeline stays in paused or otherwise pausing")
		Consistently(func() bool {
			rollout, _ := pipelineRolloutClient.Get(ctx, pipelineRolloutName, metav1.GetOptions{})
			_, _, retrievedPipelineStatus, err := getPipelineSpecAndStatus(Namespace, pipelineRolloutName)
			if err != nil {
				return false
			}
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionPipelinePausingOrPaused) == metav1.ConditionTrue &&
				(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused || retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing)
		}, 15*time.Second, testPollingInterval).Should(BeTrue())

		verifyInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		pipeline, err := getPipeline(Namespace, pipelineRolloutName)
		Expect(err).ShouldNot(HaveOccurred())
		verifyPodsRunning(Namespace, 0, getVertexLabelSelector(pipeline.GetName()))
	})

	time.Sleep(2 * time.Second)

	It("Should resume the Pipeline if user requests it", func() {

		document("setting desiredPhase=Running")
		currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning

		updatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhaseRunning, false)
	})

	It("Should pause the MonoVertex if user requests it", func() {

		document("setting desiredPhase=Paused")
		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhasePaused

		updateMonoVertexRollout(monoVertexRolloutName, currentMonoVertexSpec, numaflowv1.MonoVertexPhasePaused)

		document("verifying MonoVertex stays in paused or otherwise pausing")
		Consistently(func() bool {
			rollout, _ := monoVertexRolloutClient.Get(ctx, monoVertexRolloutName, metav1.GetOptions{})
			_, _, retrievedMonoVertexStatus, err := getMonoVertexFromK8S(Namespace, monoVertexRolloutName)
			if err != nil {
				return false
			}
			return getRolloutConditionStatus(rollout.Status.Conditions, apiv1.ConditionMonoVertexPausingOrPaused) == metav1.ConditionTrue &&
				(retrievedMonoVertexStatus.Phase == numaflowv1.MonoVertexPhasePaused)
		}, 15*time.Second, testPollingInterval).Should(BeTrue())

		verifyInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		verifyPodsRunning(Namespace, 0, getVertexLabelSelector(monoVertexRolloutName))
	})

	time.Sleep(2 * time.Second)

	It("Should resume the MonoVertex if user requests it", func() {

		document("setting desiredPhase=Running")
		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhaseRunning

		updateMonoVertexRollout(monoVertexRolloutName, currentMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning)

	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {
		updateNumaflowControllerRollout(initialNumaflowControllerVersion, updatedNumaflowControllerVersion, pipelineRolloutName, true)
	})

	time.Sleep(2 * time.Second)

	It("Should fail if the NumaflowControllerRollout is updated with a bad version", func() {
		updateNumaflowControllerRollout(updatedNumaflowControllerVersion, invalidNumaflowControllerVersion, pipelineRolloutName, false)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child NumaflowController if the NumaflowControllerRollout is restored back to previous version", func() {
		updateNumaflowControllerRollout(invalidNumaflowControllerVersion, updatedNumaflowControllerVersion, pipelineRolloutName, true)
	})

	time.Sleep(2 * time.Second)

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {

		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = updatedJetstreamVersion

		updateISBServiceRollout(isbServiceRolloutName, pipelineRolloutName, updatedISBServiceSpec, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
		}, true, false)

	})

	It("Should update the child ISBService updating a no-data-loss field", func() {

		updateISBServiceRollout(isbServiceRolloutName, pipelineRolloutName, ISBServiceSpecNoDataLossField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
				*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
		}, false, false)

	})

	It("Should update child MonoVertex if the MonoVertexRollout is updated", func() {

		// new MonoVertex spec
		updatedMonoVertexSpec := initialMonoVertexSpec
		updatedMonoVertexSpec.Source.UDSource = nil
		rpu := int64(10)
		updatedMonoVertexSpec.Source.Generator = &numaflowv1.GeneratorSource{RPU: &rpu}

		updateMonoVertexRollout(monoVertexRolloutName, updatedMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning)

		verifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return retrievedMonoVertexSpec.Source.Generator != nil && retrievedMonoVertexSpec.Source.UDSource == nil
		})

	})

	It("Should delete the PipelineRollout and child Pipeline", func() {
		deletePipelineRollout(pipelineRolloutName)
	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {
		deleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete the ISBServiceRollout and child ISBService", func() {
		deleteISBServiceRollout(isbServiceRolloutName)
	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {
		deleteNumaflowControllerRollout()
	})

})
