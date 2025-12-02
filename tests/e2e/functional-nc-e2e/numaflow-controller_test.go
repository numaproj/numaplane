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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	isbServiceRolloutName            = "test-isbservice-rollout"
	pipelineRolloutName              = "test-pipeline-rollout"
	monoVertexRolloutName            = "test-monovertex-rollout"
	invalidNumaflowControllerVersion = "99.99.99"
	initialJetstreamVersion          = "2.10.17"
)

var (
	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	sourceVertexScaleMin = int32(5)
	sourceVertexScaleMax = int32(9)
	numVertices          = int32(1)
	zeroReplicaSleepSec  = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	initialPipelineSpec  = numaflowv1.PipelineSpec{
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

	initialMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{
			Min: &sourceVertexScaleMin,
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
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil, apiv1.Metadata{})
	})

	It("Should create the MonoVertexRollout if it does not exist", func() {
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil, apiv1.Metadata{})
	})

	It("Should update the child NumaflowController if the NumaflowControllerRollout is updated", func() {
		UpdateNumaflowControllerRollout(InitialNumaflowControllerVersion, UpdatedNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, true)
		VerifyPromotedISBSvcReady(Namespace, isbServiceRolloutName, 3)
		VerifyPipelineRolloutHealthy(pipelineRolloutName)
		VerifyPromotedPipelineRunning(Namespace, pipelineRolloutName)
		VerifyMonoVertexRolloutReady(monoVertexRolloutName)
		Expect(VerifyPromotedMonoVertexRunning(Namespace, monoVertexRolloutName)).ShouldNot(HaveOccurred())
	})

	It("Should fail if the NumaflowControllerRollout is updated with a bad version", func() {
		UpdateNumaflowControllerRollout(UpdatedNumaflowControllerVersion, invalidNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, false)
	})

	It("Should update the child NumaflowController if the NumaflowControllerRollout is restored back to previous version", func() {
		UpdateNumaflowControllerRollout(invalidNumaflowControllerVersion, UpdatedNumaflowControllerVersion, []PipelineRolloutInfo{{PipelineRolloutName: pipelineRolloutName}}, true)
	})

	It("Should delete the PipelineRollouts and child Pipelines", func() {
		DeletePipelineRollout(pipelineRolloutName)
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
