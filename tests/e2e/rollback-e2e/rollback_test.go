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

	"github.com/numaproj/numaplane/internal/controller/config"
	. "github.com/numaproj/numaplane/tests/e2e"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
	monoVertexRolloutName = "test-monovertex-rollout"
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

	// topology change with invalid image
	failedPipelineSpec = numaflowv1.PipelineSpec{
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
						Name: "badcat",
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

	initialMVImage        = "quay.io/numaio/numaflow-go/source-simple-source:stable"
	initialMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{
			Min: &monoVertexScaleMin,
			Max: &monoVertexScaleMax,
		},
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: initialMVImage,
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}

	// invalid image change
	invalidMVImage       = "quay.io/numaio/numaflow-java/source-simple-source:invalid"
	failedMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{
			Min: &monoVertexScaleMin,
			Max: &monoVertexScaleMax,
		},
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: invalidMVImage,
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}

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

	// change to "Persistence" causes ISBService (and its Pipelines) to be recreated
	revisedVolSize, _     = apiresource.ParseQuantity("20Mi")
	updatedISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: InitialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &revisedVolSize,
			},
		},
	}
)

func TestRollbackE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Rollback right after update - E2E Suite")
}

var _ = Describe("Rollback e2e", Serial, func() {

	It("Should Create Rollouts", func() {
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil)
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil)
	})

	It("Should Update Rollouts and then immediately Roll them back", func() {

		// update each rollout
		updatedNCVersion := UpdatedNumaflowControllerVersion
		updateResources(&failedPipelineSpec, &updatedISBServiceSpec, &failedMonoVertexSpec, &updatedNCVersion)

		time.Sleep(30 * time.Second)

		// Now roll everything back to original versions
		initialNCVersion := InitialNumaflowControllerVersion
		updateResources(&initialPipelineSpec, &initialISBServiceSpec, &initialMonoVertexSpec, &initialNCVersion)

		// Verify everything got updated, and there's just one Promoted child for each with the correct spec

		By("verifying updates have completed")

		VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		CheckEventually("verifying just 1 InterstepBufferService", func() int {
			return GetNumberOfChildren(GetGVRForISBService(), Namespace, isbServiceRolloutName)
		}).Should(Equal(1))
		VerifyISBServiceRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		CheckEventually("verifying just 1 Pipeline", func() int {
			return GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
		}).Should(Equal(1))
		VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
		CheckEventually("verifying just 1 MonoVertex", func() int {
			return GetNumberOfChildren(GetGVRForMonoVertex(), Namespace, monoVertexRolloutName)
		}).Should(Equal(1))
		VerifyMonoVertexRolloutInProgressStrategyConsistently(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		By("Verifying NumaflowController ready and spec correct")
		VerifyNumaflowControllerRolloutReady()
		VerifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
			colon := strings.Index(d.Spec.Template.Spec.Containers[0].Image, ":")
			return colon != -1 && d.Spec.Template.Spec.Containers[0].Image[colon+1:] == "v"+InitialNumaflowControllerVersion
		})
		By("Verifying ISBService ready and spec correct")
		VerifyISBSvcRolloutReady(isbServiceRolloutName)
		VerifyPromotedISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(volSize)
		})
		By("Verifying Pipeline ready and spec correct")
		VerifyPipelineRolloutDeployed(pipelineRolloutName)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == 2
		})
		By("Verifying MonoVertex ready and spec correct")
		VerifyMonoVertexRolloutDeployed(monoVertexRolloutName)
		VerifyMonoVertexRolloutHealthy(monoVertexRolloutName)
		VerifyPromotedMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return retrievedMonoVertexSpec.Source.UDSource.Container.Image == initialMVImage
		})
	})

	It("Should update ISBServiceRollout and PipelineRollout and immediately rollback just PipelineRollout", func() {
		updateResources(&failedPipelineSpec, &updatedISBServiceSpec, nil, nil)

		time.Sleep(30 * time.Second)

		// Now roll everything back to original versions
		updateResources(&initialPipelineSpec, nil, nil, nil)

		By("verifying updates have completed")

		VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		CheckEventually("verifying just 1 InterstepBufferService", func() int {
			return GetNumberOfChildren(GetGVRForISBService(), Namespace, isbServiceRolloutName)
		}).Should(Equal(1))
		VerifyISBServiceRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		CheckEventually("verifying just 1 Pipeline", func() int {
			return GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
		}).Should(Equal(1))
		VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		By("Verifying ISBService ready and spec correct")
		VerifyISBSvcRolloutReady(isbServiceRolloutName)
		VerifyPromotedISBServiceSpec(Namespace, isbServiceRolloutName, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(revisedVolSize)
		})
		promotedISBServiceName, _ := GetPromotedISBServiceName(Namespace, isbServiceRolloutName)
		By("Verifying Pipeline ready and spec correct")
		VerifyPipelineRolloutDeployed(pipelineRolloutName)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return len(retrievedPipelineSpec.Vertices) == 2 && retrievedPipelineSpec.InterStepBufferServiceName == promotedISBServiceName
		})

	})

	if UpgradeStrategy == config.ProgressiveStrategyID {
		It("Should update PipelineRollout and MonoVertexRollout to a failed state and then roll them back", func() {

			// update each rollout
			updatedNCVersion := UpdatedNumaflowControllerVersion
			updateResources(&failedPipelineSpec, &updatedISBServiceSpec, &failedMonoVertexSpec, &updatedNCVersion)

			By("verifying Pipeline and MonoVertex have failed progressive rollout")

			// allow Pipeline and MonoVertex to fail
			promotedPipelineName := GetInstanceName(pipelineRolloutName, 4)
			upgradingPipelineName := GetInstanceName(pipelineRolloutName, 5)
			VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, promotedPipelineName, upgradingPipelineName, true, apiv1.AssessmentResultFailure, false)

			promotedMonoVertexName := GetInstanceName(monoVertexRolloutName, 0)
			upgradingMonoVertexName := GetInstanceName(monoVertexRolloutName, 1)
			VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, promotedMonoVertexName, upgradingMonoVertexName, true, apiv1.AssessmentResultFailure, false)

			// Now roll everything back to original versions
			updateResources(&initialPipelineSpec, nil, nil, nil)

			By("verifying Pipeline and MonoVertex are back to healthy after rollback")

			// Verify Pipeline
			VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
			CheckEventually("verifying just 1 promoted Pipeline, with expected name", func() bool {
				currentPromotedName, _ := GetPromotedPipelineName(Namespace, pipelineRolloutName)
				numChildren := GetNumberOfChildren(GetGVRForPipeline(), Namespace, pipelineRolloutName)
				return currentPromotedName == promotedPipelineName && numChildren == 1
			}).Should(Equal(true))
			VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
			VerifyPipelineRolloutHealthy(pipelineRolloutName)

			// Verify MonoVertex
			VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
			CheckEventually("verifying just 1 promoted MonoVertex, with expected name", func() bool {
				currentPromotedName, _ := GetPromotedMonoVertexName(Namespace, monoVertexRolloutName)
				numChildren := GetNumberOfChildren(GetGVRForMonoVertex(), Namespace, monoVertexRolloutName)
				return currentPromotedName == monoVertexRolloutName && numChildren == 1
			}).Should(Equal(true))
			VerifyMonoVertexRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
			VerifyMonoVertexRolloutHealthy(pipelineRolloutName)

		})
	}

	It("Should Delete Rollouts", func() {
		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})

})

func updateResources(pipelineSpec *numaflowv1.PipelineSpec, isbsvcSpec *numaflowv1.InterStepBufferServiceSpec, mvSpec *numaflowv1.MonoVertexSpec, numaflowVersion *string) {
	// update each rollout
	if isbsvcSpec != nil {
		rawSpec, err := json.Marshal(isbsvcSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
			rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
			return rollout, nil
		})
	}

	if pipelineSpec != nil {
		rawSpec, err := json.Marshal(pipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawSpec
			return rollout, nil
		})
	}

	if mvSpec != nil {
		rawSpec, err := json.Marshal(mvSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawSpec
			return rollout, nil
		})
	}

	if numaflowVersion != nil {
		UpdateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
			rollout.Spec = apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{Version: *numaflowVersion},
			}
			return rollout, nil
		})
	}
}
