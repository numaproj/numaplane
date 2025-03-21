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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName = "test-monovertex-rollout"
)

var (
	sourceVertexScaleMin = int32(5)

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

var _ = Describe("Functional e2e:", Serial, func() {

	It("Should create the NumaflowControllerRollout if it doesn't exist", func() {
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
	})

	It("Should create the MonoVertexRollout if it does not exist", func() {
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil)
	})

	It("Should pause the MonoVertex if user requests it", func() {

		By("setting desiredPhase=Paused")
		currentMonoVertexSpec := initialMonoVertexSpec
		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhasePaused

		UpdateMonoVertexRollout(monoVertexRolloutName, currentMonoVertexSpec, numaflowv1.MonoVertexPhasePaused, func(spec numaflowv1.MonoVertexSpec) bool {
			return spec.Lifecycle.DesiredPhase == numaflowv1.MonoVertexPhasePaused
		})

		VerifyMonoVertexStaysPaused(monoVertexRolloutName)
	})

	It("Should resume the MonoVertex if user requests it", func() {

		By("setting desiredPhase=Running")
		currentMonoVertexSpec := initialMonoVertexSpec
		currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhaseRunning

		UpdateMonoVertexRollout(monoVertexRolloutName, currentMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning, func(spec numaflowv1.MonoVertexSpec) bool {
			return spec.Lifecycle.DesiredPhase == numaflowv1.MonoVertexPhaseRunning
		})
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
		CheckEventually("verifying just 1 MonoVertex", func() int {
			return GetNumberOfChildren(GetGVRForMonoVertex(), Namespace, monoVertexRolloutName)
		}).Should(Equal(1))
	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {
		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {
		DeleteNumaflowControllerRollout()
	})
})
