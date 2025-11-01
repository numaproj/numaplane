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
	"fmt"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"

	// anotherPipelineRolloutName = "another-pipeline-rollout"
)

var (
	pullPolicyAlways           = corev1.PullAlways
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
				Scale: numaflowv1.Scale{Min: &fourPods, Max: &fivePods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
						ImagePullPolicy: &pullPolicyAlways,
						Env: []corev1.EnvVar{
							{
								Name:  "my-env",
								Value: "{{.pipeline-namespace}}-{{.pipeline-name}}",
							},
						},
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

	pipelineMetadata = metav1.ObjectMeta{
		Labels: map[string]string{
			"my-label": "{{.pipeline-namespace}}-{{.pipeline-name}}",
		},
		Annotations: map[string]string{
			"my-annotation": "{{.pipeline-namespace}}-{{.pipeline-name}}",
		},
	}

	volSize, _     = apiresource.ParseQuantity("10Mi")
	isbServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: InitialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
	updatedMemLimit, _            = apiresource.ParseQuantity("2Gi")
	ISBServiceSpecNoDataLossField = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: UpdatedJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
			ContainerTemplate: &numaflowv1.ContainerTemplate{
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{corev1.ResourceMemory: updatedMemLimit},
				},
			},
		},
	}

	revisedVolSize, _           = apiresource.ParseQuantity("20Mi")
	ISBServiceSpecRecreateField = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: UpdatedJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &revisedVolSize,
			},
			ContainerTemplate: &numaflowv1.ContainerTemplate{
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{corev1.ResourceMemory: updatedMemLimit},
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
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
	})

	It("Should create the ISBServiceRollout if it doesn't exist", func() {
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should create the PipelineRollout if it does not exist", func() {
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil, pipelineMetadata)

		VerifyPromotedPipelineMetadata(Namespace, pipelineRolloutName, func(metadata metav1.ObjectMeta) bool {
			return metadata.Labels != nil && metadata.Labels["my-label"] == fmt.Sprintf("%s-%s", Namespace, GetInstanceName(pipelineRolloutName, 0))
		})
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
			VerifyPromotedPipelineStatusConsistently(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
				return retrievedPipelineStatus.Phase != numaflowv1.PipelinePhasePaused
			})
		}

		// allow time for self healing to reconcile
		time.Sleep(5 * time.Second)

		// get updated Pipeline again to compare spec
		By("Verifying self-healing")
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			return !retrievedPipelineSpec.Watermark.Disabled
		})

		VerifyPipelineRolloutDeployed(pipelineRolloutName)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)

		// Verify no in progress strategy set
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
	})

	It("Should update the child Pipeline if the PipelineRollout is updated", func() {
		numPipelineVertices := len(updatedPipelineSpec.Vertices)

		UpdatePipelineRollout(pipelineRolloutName, updatedPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
			currentPromotedPipelineName, _ := GetPromotedPipelineName(Namespace, pipelineRolloutName)
			if len(retrievedPipelineSpec.Vertices) < numPipelineVertices {
				// not yet updated
				return false
			}
			evaluatedEnvironmentVariable := retrievedPipelineSpec.Vertices[1].UDF.Container.Env[0]
			return evaluatedEnvironmentVariable.Name == "my-env" && evaluatedEnvironmentVariable.Value == fmt.Sprintf("%s-%s", Namespace, currentPromotedPipelineName)
		}, true, true, true, apiv1.Metadata{})

	})

	It("Should pause the Pipeline if user requests it and resume it", func() {

		// test that pause works, as well as that pipeline resumes gradually
		testPauseResume(updatedPipelineSpec, false)

		// update PipelineRollout to set FastResume to true such pipeline should resume fast, not gradually
		By("Setting FastResume=true")
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Strategy = &apiv1.PipelineStrategy{
				PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
					PauseResumeStrategy: apiv1.PauseResumeStrategy{
						FastResume: true,
					},
				},
			}
			return rollout, nil
		})

		// test that pause works, as well as that pipeline resumes fast
		testPauseResume(updatedPipelineSpec, true)
	})

	It("Should update the child ISBService if the ISBServiceRollout is updated", func() {
		// new ISBService spec
		updatedISBServiceSpec := isbServiceSpec
		updatedISBServiceSpec.JetStream.Version = UpdatedJetstreamVersion

		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, updatedISBServiceSpec, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Version == UpdatedJetstreamVersion
		}, true, false, true)

		// Verify no in progress strategy set when it's done
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
	})

	It("Should update the child ISBService updating a no-data-loss/no recreate field", func() {
		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, ISBServiceSpecNoDataLossField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate != nil &&
				retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() != nil &&
				*retrievedISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits.Memory() == updatedMemLimit
		}, false, false, false)

	})

	It("Should update the child ISBService updating a recreate field", func() {
		UpdateISBServiceRollout(isbServiceRolloutName, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, ISBServiceSpecRecreateField, func(retrievedISBServiceSpec numaflowv1.InterStepBufferServiceSpec) bool {
			return retrievedISBServiceSpec.JetStream.Persistence.VolumeSize.Equal(revisedVolSize)
		}, false, true, true)

		// Verify no in progress strategy set when it's done
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategy(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategyConsistently(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyPipelineRolloutInProgressStrategyConsistently(isbServiceRolloutName, apiv1.UpgradeStrategyNoOp)
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

// test that user can cause Pipeline through PipelineRollout desiredPhase field
// as well as that user can unpause (either gradually or fast depending on configuration)
func testPauseResume(currentPipelineSpec numaflowv1.PipelineSpec, resumeFast bool) {

	By("setting desiredPhase=Paused")
	currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused

	UpdatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhasePaused, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused
	}, false, false, true, apiv1.Metadata{})

	VerifyPromotedPipelineStaysPaused(pipelineRolloutName)

	By("setting desiredPhase=Running")

	currentPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhaseRunning

	// Verify since strategy.pauseResume.resumeFast isn't set it defaults to resuming slowly
	// which means that vertex.replicas will get set to null

	// patch "in" vertex's replicas to 5, thereby imitating the Numaflow autoscaler scaling up
	promotedPipelineName, err := GetPromotedPipelineName(Namespace, pipelineRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	vertexName := fmt.Sprintf("%s-in", promotedPipelineName)
	UpdateVertexInK8S(vertexName, func(retrievedVertex *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		five := int64(5)
		unstructured.RemoveNestedField(retrievedVertex.Object, "spec", "replicas")
		err := unstructured.SetNestedField(retrievedVertex.Object, five, "spec", "replicas")
		return retrievedVertex, err
	})

	// Resume Pipeline
	UpdatePipelineRollout(pipelineRolloutName, currentPipelineSpec, numaflowv1.PipelinePhaseRunning, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhaseRunning
	}, false, false, true, apiv1.Metadata{})

	// then verify that replicas is null
	VerifyVertexSpecStatus(Namespace, vertexName, func(spec numaflowv1.VertexSpec, status numaflowv1.VertexStatus) bool {
		if resumeFast {
			return spec.Replicas != nil && *spec.Replicas == 5
		} else {
			return spec.Replicas == nil
		}
	})
}
