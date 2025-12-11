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
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/numaproj/numaplane/internal/controller/progressive"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName = "test-monovertex-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
	isbServiceRolloutName = "test-isbservice-rollout"
)

var (
	monoVertexScaleMin  = int32(4)
	monoVertexScaleMax  = int32(5)
	zeroReplicaSleepSec = uint32(15)

	initialMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{Min: &monoVertexScaleMin, Max: &monoVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/simple-source:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}

	validUDTransformerImage = "quay.io/numaio/numaflow-rs/source-transformer-now:stable"

	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	validImagePath       = "quay.io/numaio/numaflow-go/map-cat:stable"
	sourceVertexScaleMin = int32(5)
	sourceVertexScaleMax = int32(9)
	numVertices          = int32(1)
	pullPolicyAlways     = corev1.PullAlways

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
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           validImagePath,
						ImagePullPolicy: &pullPolicyAlways,
						Env: []corev1.EnvVar{
							{
								Name:  "my-env",
								Value: "{{.pipeline-namespace}}-{{pipeline-name}}",
							},
						},
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

	volSize, _            = apiresource.ParseQuantity("10Mi")
	initialISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: "2.10.17",
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}
)

func TestSkipProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Skip Progressive E2E Suite")
}

var _ = Describe("Skip Progressive E2E", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
	})

	It("Should skip analysis if MonoVertexRollout has scale.max=0", func() {
		verifyMonoVertexAnalysisSkipped(func(spec *numaflowv1.MonoVertexSpec) *numaflowv1.MonoVertexSpec {
			updatedMonoVertexSpec := spec.DeepCopy()
			updatedMonoVertexSpec.Scale.Max = ptr.To(int32(0))
			updatedMonoVertexSpec.Scale.Min = ptr.To(int32(0))
			return updatedMonoVertexSpec
		})
		time.Sleep(5 * time.Second)
	})

	It("Should skip analysis if MonoVertexRollout is set to pause", func() {
		verifyMonoVertexAnalysisSkipped(func(spec *numaflowv1.MonoVertexSpec) *numaflowv1.MonoVertexSpec {
			updatedMonoVertexSpec := spec.DeepCopy()
			updatedMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhasePaused
			return updatedMonoVertexSpec
		})
	})

	It("Should create ISBServiceRollout", func() {
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
	})

	It("Should skip analysis if PipelineRollout has source vertex with scale.max=0", func() {
		verifyPipelineAnalysisSkipped(func(spec *numaflowv1.PipelineSpec) *numaflowv1.PipelineSpec {
			updatedPipelineSpec := spec.DeepCopy()
			updatedPipelineSpec.Vertices[0].Scale.Max = ptr.To(int32(0))
			updatedPipelineSpec.Vertices[0].Scale.Min = ptr.To(int32(0))
			return updatedPipelineSpec
		})
		time.Sleep(5 * time.Second)
	})

	It("Should skip analysis if PipelineRollout is set to pause", func() {
		verifyPipelineAnalysisSkipped(func(spec *numaflowv1.PipelineSpec) *numaflowv1.PipelineSpec {
			updatedPipelineSpec := spec.DeepCopy()
			updatedPipelineSpec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused
			return updatedPipelineSpec
		})
	})

	It("Should delete Rollouts", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

func updateMonoVertexRolloutImage(initialMonoVertexSpec numaflowv1.MonoVertexSpec) *numaflowv1.MonoVertexSpec {
	By("Updating the MonoVertex Topology to cause a Progressive change")
	updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
	updatedMonoVertexSpec.Source.UDTransformer = &numaflowv1.UDTransformer{Container: &numaflowv1.Container{}}
	updatedMonoVertexSpec.Source.UDTransformer.Container.Image = validUDTransformerImage
	return updatedMonoVertexSpec
}

func verifyMonoVertexAnalysisSkipped(updateFunc func(*numaflowv1.MonoVertexSpec) *numaflowv1.MonoVertexSpec) {
	CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, nil, apiv1.Metadata{})
	// wait for MonoVertex to be ready
	err := VerifyPromotedMonoVertexRunning(Namespace, monoVertexRolloutName)
	Expect(err).ShouldNot(HaveOccurred())

	updatedMonoVertexSpec := updateMonoVertexRolloutImage(initialMonoVertexSpec)
	updatedMonoVertexSpec = updateFunc(updatedMonoVertexSpec)
	rawSpec, err := json.Marshal(updatedMonoVertexSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(mvr apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
		mvr.Spec.MonoVertex.Spec.Raw = rawSpec
		return mvr, nil
	})
	VerifyMonoVertexRolloutStatusEventually(monoVertexRolloutName, func(status apiv1.MonoVertexRolloutStatus) bool {
		return status.ProgressiveStatus.UpgradingMonoVertexStatus != nil &&
			status.ProgressiveStatus.UpgradingMonoVertexStatus.ForcedSuccess &&
			status.ProgressiveStatus.UpgradingMonoVertexStatus.ForcedSuccessReason == string(progressive.SkipProgressiveAssessmentReasonNoDataIngestion) &&
			status.ProgressiveStatus.UpgradingMonoVertexStatus.Name == GetInstanceName(monoVertexRolloutName, 1)
	})
	DeleteMonoVertexRollout(monoVertexRolloutName)
}

func updatePipelineRolloutImage(initialPipelineSpec numaflowv1.PipelineSpec) *numaflowv1.PipelineSpec {
	By("Updating the Pipeline Topology to cause a Progressive change")
	updatedPipelineSpec := initialPipelineSpec.DeepCopy()
	// Change the UDF image to trigger a progressive upgrade
	updatedPipelineSpec.Vertices[1].UDF.Container.Image = "quay.io/numaio/numaflow-go/map-cat:v0.9.0"
	return updatedPipelineSpec
}

func verifyPipelineAnalysisSkipped(updateFunc func(*numaflowv1.PipelineSpec) *numaflowv1.PipelineSpec) {
	CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initialPipelineSpec, apiv1.PipelineStrategy{}, apiv1.Metadata{})

	updatedPipelineSpec := updatePipelineRolloutImage(initialPipelineSpec)
	updatedPipelineSpec = updateFunc(updatedPipelineSpec)
	rawSpec, err := json.Marshal(updatedPipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pr apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		pr.Spec.Pipeline.Spec.Raw = rawSpec
		return pr, nil
	})
	VerifyPipelineRolloutStatusEventually(pipelineRolloutName, func(status apiv1.PipelineRolloutStatus) bool {
		return status.ProgressiveStatus.UpgradingPipelineStatus != nil &&
			status.ProgressiveStatus.UpgradingPipelineStatus.ForcedSuccess &&
			status.ProgressiveStatus.UpgradingPipelineStatus.ForcedSuccessReason == string(progressive.SkipProgressiveAssessmentReasonNoDataIngestion) &&
			status.ProgressiveStatus.UpgradingPipelineStatus.Name == GetInstanceName(pipelineRolloutName, 1)
	})
	DeletePipelineRollout(pipelineRolloutName)
}
