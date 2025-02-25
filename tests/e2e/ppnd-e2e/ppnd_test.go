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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	isbServiceRolloutName            = "test-isbservice-rollout"
	slowPipelineRolloutName          = "slow-pipeline-rollout"
	failedPipelineRolloutName        = "failed-pipeline-rollout"
	initialNumaflowControllerVersion = "1.4.1"
	updatedNumaflowControllerVersion = "1.4.2"
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

	slowPipelineSpec *numaflowv1.PipelineSpec
)

func TestPauseAndDrainE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Pause and drain E2E Suite")
}

var _ = Describe("Pause and drain e2e", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(initialNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should allow data loss in the Pipeline if requested - Pipeline update", func() {
		if UpgradeStrategy == config.PPNDStrategyID {

			createSlowPipelineRollout()

			By("Updating Pipeline Topology to cause a PPND change")
			slowPipelineSpec.Vertices[1] = slowPipelineSpec.Vertices[2]
			slowPipelineSpec.Vertices = slowPipelineSpec.Vertices[0:2]
			slowPipelineSpec.Edges = []numaflowv1.Edge{
				{
					From: "in",
					To:   "out",
				},
			}

			UpdatePipelineRollout(slowPipelineRolloutName, *slowPipelineSpec, numaflowv1.PipelinePhasePausing, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
				return true
			}, true)

			verifyPipelineIsSlowToPause()
			allowDataLoss()
			DeletePipelineRollout(slowPipelineRolloutName)

		}
	})

	It("Should allow data loss in the Pipeline if requested - ISBService update", func() {
		if UpgradeStrategy == config.PPNDStrategyID {

			createSlowPipelineRollout()

			By("Updating ISBService to cause a PPND change")
			updatedISBServiceSpec := isbServiceSpec
			updatedISBServiceSpec.JetStream.Version = updatedJetstreamVersion
			rawSpec, err := json.Marshal(updatedISBServiceSpec)
			Expect(err).ShouldNot(HaveOccurred())

			// we use the direct k8s function since we need to control the PipelineRollout to complete the update
			UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
				rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
				return rollout, nil
			})

			verifyPipelineIsSlowToPause()
			allowDataLoss()
			DeletePipelineRollout(slowPipelineRolloutName)

			// confirm update
			VerifyISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
				return retrievedISBServiceSpec.JetStream.Version == updatedJetstreamVersion
			})

		}
	})

	It("Should allow data loss in the Pipeline if requested - Numaflow Controller update", func() {
		if UpgradeStrategy == config.PPNDStrategyID {

			createSlowPipelineRollout()

			By("Updating Numaflow controller to cause a PPND change")
			updatedNumaflowControllerROSpec := apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{Version: updatedNumaflowControllerVersion},
			}
			UpdateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
				rollout.Spec = updatedNumaflowControllerROSpec
				return rollout, nil
			})

			verifyPipelineIsSlowToPause()
			allowDataLoss()
			DeletePipelineRollout(slowPipelineRolloutName)

			// confirm update
			VerifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
				colon := strings.Index(d.Spec.Template.Spec.Containers[0].Image, ":")
				return colon != -1 && d.Spec.Template.Spec.Containers[0].Image[colon+1:] == "v"+updatedNumaflowControllerVersion
			})

		}
	})

	It("Should update a Pipeline even if the Pipeline is failed", func() {

		// add bad edge to automatically fail Pipeline
		failedPipelineSpec := initialPipelineSpec
		failedPipelineSpec.Edges = append(failedPipelineSpec.Edges, numaflowv1.Edge{From: "not", To: "valid"})

		CreatePipelineRollout(failedPipelineRolloutName, Namespace, failedPipelineSpec, true)
		VerifyPipelineFailed(Namespace, failedPipelineRolloutName)

		time.Sleep(5 * time.Second)

		// update spec to have topology change
		UpdatePipelineRollout(failedPipelineRolloutName, updatedPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == 3
		}, true)

		time.Sleep(5 * time.Second)

		DeletePipelineRollout(failedPipelineRolloutName)

	})

	time.Sleep(5 * time.Second)

	It("Should update an ISBService even if the Pipeline is failed", func() {

		// add bad edge to automatically fail Pipeline
		failedPipelineSpec := initialPipelineSpec
		failedPipelineSpec.Edges = append(failedPipelineSpec.Edges, numaflowv1.Edge{From: "not", To: "valid"})

		CreatePipelineRollout(failedPipelineRolloutName, Namespace, failedPipelineSpec, true)
		VerifyPipelineFailed(Namespace, failedPipelineRolloutName)

		time.Sleep(5 * time.Second)

		// update ISBService to have data loss update
		By("Updating ISBService to cause a PPND change")
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = initialJetstreamVersion

		// need to update function
		// update would normally cause data loss
		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: failedPipelineRolloutName, PipelineIsFailed: true}}, updatedISBServiceSpec, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == initialJetstreamVersion
		}, true, false)

		time.Sleep(5 * time.Second)

		DeletePipelineRollout(failedPipelineRolloutName)

	})

	time.Sleep(5 * time.Second)

	It("Should update a NumaflowController even if the Pipeline is failed", func() {

		// add bad edge to automatically fail Pipeline
		failedPipelineSpec := initialPipelineSpec
		failedPipelineSpec.Edges = append(failedPipelineSpec.Edges, numaflowv1.Edge{From: "not", To: "valid"})

		CreatePipelineRollout(failedPipelineRolloutName, Namespace, failedPipelineSpec, true)
		VerifyPipelineFailed(Namespace, failedPipelineRolloutName)

		time.Sleep(5 * time.Second)

		By("Updating Numaflow controller to cause a PPND change")
		UpdateNumaflowControllerRollout(updatedNumaflowControllerVersion, initialNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: failedPipelineRolloutName, PipelineIsFailed: true}}, true)

		time.Sleep(5 * time.Second)

		DeletePipelineRollout(failedPipelineRolloutName)

	})

	It("Should delete all remaining rollout objects", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})

})

func createSlowPipelineRollout() {

	By("Creating a slow pipeline")
	slowPipelineSpec = updatedPipelineSpec.DeepCopy()
	highRPU := int64(10000000)
	readBatchSize := uint64(1)
	pauseGracePeriodSeconds := int64(3600) // numaflow will try to pause for 1 hour if we let it
	slowPipelineSpec.Lifecycle.PauseGracePeriodSeconds = &pauseGracePeriodSeconds
	slowPipelineSpec.Limits = &numaflowv1.PipelineLimits{ReadBatchSize: &readBatchSize}
	slowPipelineSpec.Vertices[0].Source.Generator.RPU = &highRPU
	slowPipelineSpec.Vertices[1].UDF = &numaflowv1.UDF{Container: &numaflowv1.Container{
		Image: "quay.io/numaio/numaflow-go/map-slow-cat:stable",
	}}

	CreatePipelineRollout(slowPipelineRolloutName, Namespace, *slowPipelineSpec, false)

	By("Verifying that the slow pipeline was created")
	VerifyPipelineSpec(Namespace, slowPipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(slowPipelineSpec.Vertices) == len(retrievedPipelineSpec.Vertices)
	})

	VerifyPipelineRunning(Namespace, slowPipelineRolloutName)
	VerifyInProgressStrategy(slowPipelineRolloutName, apiv1.UpgradeStrategyNoOp)

}

func verifyPipelineIsSlowToPause() {

	By("Verifying that Pipeline tries to pause")
	VerifyPipelineStatusEventually(Namespace, slowPipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
		return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing
	})
	By("Verifying that Pipeline keeps trying to pause")
	VerifyPipelineStatusConsistently(Namespace, slowPipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
		return retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing
	})

}

func allowDataLoss() {

	// update the PipelineRollout to allow data loss temporarily
	UpdatePipelineRolloutInK8S(Namespace, slowPipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		if rollout.Annotations == nil {
			rollout.Annotations = make(map[string]string)
		}
		rollout.Annotations[common.LabelKeyAllowDataLoss] = "true"
		return rollout, nil
	})

	By("Verifying that Pipeline has stopped trying to pause")
	VerifyPipelineRunning(Namespace, slowPipelineRolloutName)

}
