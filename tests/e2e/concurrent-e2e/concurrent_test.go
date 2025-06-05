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

	// topology change
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

	// image change
	updatedMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{
			Min: &monoVertexScaleMin,
			Max: &monoVertexScaleMax,
		},
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
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

	// new jetstream version
	dataLossISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: UpdatedJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}

	// updating Resources.Limits will not cause data loss or require recreating ISBService
	updatedMemLimit, _        = apiresource.ParseQuantity("2Gi")
	directApplyISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: InitialJetstreamVersion,
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

	// change to "Persistence" causes ISBService (and its Pipelines) to be recreated
	revisedVolSize, _           = apiresource.ParseQuantity("20Mi")
	recreateFieldISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: InitialJetstreamVersion,
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

	pipelineDataLossFunc = func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return len(retrievedPipelineSpec.Vertices) == 3
	}

	isbServiceDataLossFunc = func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
		return retrievedISBServiceSpec.JetStream.Version == UpdatedJetstreamVersion
	}

	isbServiceRecreateFunc = func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
		return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(revisedVolSize)
	}

	isbServiceDirectApplyFunc = func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
		return retrievedISBServiceSpec.JetStream != nil &&
			retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
			retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
			*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
	}

	monoVertexImageFunc = func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
		return retrievedMonoVertexSpec.Source.UDSource.Container.Image == "quay.io/numaio/numaflow-java/source-simple-source:stable"
	}
)

// TODO: add remaining combinations
// PipelineRollout direct apply, ISBService recreate, Numaflow Controller data loss (version), MonoVertex image
// PipelineRollout direct apply, ISBService data loss, Numaflow Controller data loss (version), MonoVertex image

func TestConcurrentE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Concurrent updates E2E Suite")
}

var _ = Describe("Concurrent e2e", Serial, func() {

	testCases := []struct {
		combination          string
		pipelineSpec         numaflowv1.PipelineSpec
		isbServiceSpec       numaflowv1.InterStepBufferServiceSpec
		monovertexSpec       numaflowv1.MonoVertexSpec
		pipelineVerifyFunc   func(numaflowv1.PipelineSpec) bool
		isbServiceVerifyFunc func(numaflowv1.InterStepBufferServiceSpec) bool
		monoVertexVerifyFunc func(numaflowv1.MonoVertexSpec) bool
	}{
		{
			combination:          "Data Loss: Pipeline, ISBService, NumaflowController",
			pipelineSpec:         dataLossPipelineSpec,
			isbServiceSpec:       dataLossISBServiceSpec,
			monovertexSpec:       updatedMonoVertexSpec,
			pipelineVerifyFunc:   pipelineDataLossFunc,
			isbServiceVerifyFunc: isbServiceDataLossFunc,
			monoVertexVerifyFunc: monoVertexImageFunc,
		},
		{
			combination:          "Data Loss: Pipeline, NumaflowController; Recreate: ISBService",
			pipelineSpec:         dataLossPipelineSpec,
			isbServiceSpec:       recreateFieldISBServiceSpec,
			monovertexSpec:       updatedMonoVertexSpec,
			pipelineVerifyFunc:   pipelineDataLossFunc,
			isbServiceVerifyFunc: isbServiceRecreateFunc,
			monoVertexVerifyFunc: monoVertexImageFunc,
		},
		{
			combination:          "Data Loss: Pipeline, NumaflowController; Direct Apply: ISBService",
			pipelineSpec:         dataLossPipelineSpec,
			isbServiceSpec:       directApplyISBServiceSpec,
			monovertexSpec:       updatedMonoVertexSpec,
			pipelineVerifyFunc:   pipelineDataLossFunc,
			isbServiceVerifyFunc: isbServiceDirectApplyFunc,
			monoVertexVerifyFunc: monoVertexImageFunc,
		},
	}

	for _, tc := range testCases {

		It(fmt.Sprintf("Should update all rollouts concurrently\nCOMBINATION: %s", tc.combination), func() {

			// create initial objects
			CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
			CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
			CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil)
			CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil)

			// update each rollout
			By("Updating PipelineRollout")
			rawSpec, err := json.Marshal(tc.pipelineSpec)
			Expect(err).ShouldNot(HaveOccurred())
			UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
				// for this test, we start the assessment window at 4 minutes in, which is longer
				// than other tests, due to the fact that Numaflow Controller is also restarting
				// and therefore can take longer to do its reconciliations of Pipeline changes
				rollout.Spec.Strategy = &apiv1.PipelineTypeRolloutStrategy{
					PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
						Progressive: apiv1.ProgressiveStrategy{
							AssessmentSchedule: "240,60,10",
						},
					},
				}
				rollout.Spec.Pipeline.Spec.Raw = rawSpec
				return rollout, nil
			})

			By("Updating MonoVertexRollout")
			rawSpec, err = json.Marshal(tc.monovertexSpec)
			Expect(err).ShouldNot(HaveOccurred())
			UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
				// for this test, we start the assessment window at 4 minutes in, which is longer
				// than other tests, due to the fact that Numaflow Controller is also restarting
				// and therefore can take longer to do its reconciliations of MonoVertex changes
				rollout.Spec.Strategy = &apiv1.PipelineTypeRolloutStrategy{
					PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
						Progressive: apiv1.ProgressiveStrategy{
							AssessmentSchedule: "240,60,10",
						},
					},
				}
				rollout.Spec.MonoVertex.Spec.Raw = rawSpec
				return rollout, nil
			})

			By("Updating ISBServiceRollout")
			rawSpec, err = json.Marshal(tc.isbServiceSpec)
			Expect(err).ShouldNot(HaveOccurred())
			UpdateISBServiceRolloutInK8S(isbServiceRolloutName, func(rollout apiv1.ISBServiceRollout) (apiv1.ISBServiceRollout, error) {
				rollout.Spec.InterStepBufferService.Spec.Raw = rawSpec
				return rollout, nil
			})

			By("Updating NumaflowControllerRollout")
			UpdateNumaflowControllerRolloutInK8S(func(rollout apiv1.NumaflowControllerRollout) (apiv1.NumaflowControllerRollout, error) {
				rollout.Spec = apiv1.NumaflowControllerRolloutSpec{
					Controller: apiv1.Controller{Version: UpdatedNumaflowControllerVersion},
				}
				return rollout, nil
			})

			time.Sleep(10 * time.Second)

			By("Verifying NumaflowController got updated")
			VerifyNumaflowControllerDeployment(Namespace, func(d appsv1.Deployment) bool {
				colon := strings.Index(d.Spec.Template.Spec.Containers[0].Image, ":")
				return colon != -1 && d.Spec.Template.Spec.Containers[0].Image[colon+1:] == "v"+UpdatedNumaflowControllerVersion
			})
			VerifyNumaflowControllerRolloutReady()

			By("Verifying ISBService got updated")
			VerifyPromotedISBServiceSpec(Namespace, isbServiceRolloutName, tc.isbServiceVerifyFunc)
			VerifyISBSvcRolloutReady(isbServiceRolloutName)
			VerifyISBServiceRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)

			By("Verifying Pipeline got updated")
			VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, tc.pipelineVerifyFunc)
			VerifyPipelineRolloutDeployed(pipelineRolloutName)
			VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

			By("Verifying MonoVertexRollout got updated")
			VerifyPromotedMonoVertexSpec(Namespace, monoVertexRolloutName, tc.monoVertexVerifyFunc)
			VerifyMonoVertexRolloutDeployed(monoVertexRolloutName)
			VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

			// case cleanup
			DeleteMonoVertexRollout(monoVertexRolloutName)
			DeletePipelineRollout(pipelineRolloutName)
			DeleteISBServiceRollout(isbServiceRolloutName)
			DeleteNumaflowControllerRollout()

		})

	}

})
