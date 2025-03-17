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
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName = "monovertex-rollout"
)

var (
	monoVertexScaleMin  = int32(5)
	monoVertexScaleMax  = int32(9)
	zeroReplicaSleepSec = uint32(15)

	udTransformer = &numaflowv1.UDTransformer{Container: &numaflowv1.Container{}}
	// validUDTransformerImage   = "quay.io/numaio/numaflow-rs/source-transformer-now:stable"
	invalidUDTransformerImage = "quay.io/numaio/numaflow-rs/source-transformer-now:invalid-e8y78rwq5h"

	initialMonoVertexSpec = &numaflowv1.MonoVertexSpec{
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
				UDSink: &numaflowv1.UDSink{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-rs/sink-log:stable",
					},
				},
			},
		},
	}
)

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive E2E Suite")
}

var _ = Describe("Progressive E2E", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
	})

	It("Should create the initial MonoVertex", func() {
		By("Creating a monovertex rollout")

		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, *initialMonoVertexSpec)

		By("Verifying that the monovertex was created")
		VerifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return *retrievedMonoVertexSpec.Scale.Min == *initialMonoVertexSpec.Scale.Min &&
				retrievedMonoVertexSpec.Source.UDSource.Container.Image == initialMonoVertexSpec.Source.UDSource.Container.Image
		})
		VerifyInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		// TODO: verify status, etc.
	})

	It("Should update the initial MonoVertex - Failure", func() {
		By("Updating MonoVertex Topology to cause a Failing Progressive change")
		updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
		updatedMonoVertexSpec.Source.UDTransformer = udTransformer
		updatedMonoVertexSpec.Source.UDTransformer.Container.Image = invalidUDTransformerImage

		UpdateMonoVertexRollout(monoVertexRolloutName, *updatedMonoVertexSpec, numaflowv1.MonoVertexPhaseFailed, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return true
		})

		// TODO: verify status, etc.
	})

	// It("Should update the initial MonoVertex - Success", func() {
	// 	By("Updating MonoVertex Topology to cause a Successful Progressive change")
	// 	updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
	// 	updatedMonoVertexSpec.Source.UDTransformer = udTransformer
	// 	updatedMonoVertexSpec.Source.UDTransformer.Container.Image = validUDTransformerImage

	// 	UpdateMonoVertexRollout(monoVertexRolloutName, *updatedMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
	// 		return retrievedMonoVertexSpec.Source.UDTransformer.Container.Image == validUDTransformerImage
	// 	})

	// 	// TODO: verify status, etc.
	// })

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
