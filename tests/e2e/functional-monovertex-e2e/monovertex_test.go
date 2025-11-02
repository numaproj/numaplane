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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName = "test-monovertex-rollout"
)

var (
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
					Env: []corev1.EnvVar{
						{
							Name:  "my-env",
							Value: "{{.monovertex-namespace}}-{{.monovertex-name}}",
						},
					},
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}

	mvtxMetadata = apiv1.Metadata{
		Labels: map[string]string{
			"my-label": "{{.monovertex-namespace}}-{{.monovertex-name}}",
		},
		Annotations: map[string]string{
			"my-annotation": "{{.monovertex-namespace}}-{{.monovertex-name}}",
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

	It("Should update child MonoVertex if the MonoVertexRollout is updated", func() {

		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil, mvtxMetadata)

		// new MonoVertex spec
		updatedMonoVertexSpec := initialMonoVertexSpec
		updatedMonoVertexSpec.Source.UDSource = nil
		rpu := int64(10)
		updatedMonoVertexSpec.Source.Generator = &numaflowv1.GeneratorSource{RPU: &rpu}

		UpdateMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, updatedMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning, func(spec numaflowv1.MonoVertexSpec) bool {
			return spec.Source != nil && spec.Source.Generator != nil && *spec.Source.Generator.RPU == rpu
		}, true, 0)

		VerifyPromotedMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			currentPromotedMvtxName, _ := GetPromotedPipelineName(Namespace, monoVertexRolloutName)

			evaluatedEnvironmentVariable := retrievedMonoVertexSpec.Source.UDSource.Container.Env[0]
			return retrievedMonoVertexSpec.Source.Generator != nil && retrievedMonoVertexSpec.Source.UDSource == nil &&
				evaluatedEnvironmentVariable.Name == "my-env" && evaluatedEnvironmentVariable.Value == fmt.Sprintf("%s-%s", Namespace, currentPromotedMvtxName)
		})

		VerifyPromotedMonoVertexMetadata(Namespace, monoVertexRolloutName, func(metadata apiv1.Metadata) bool {
			return metadata.Labels != nil && metadata.Labels["my-label"] == fmt.Sprintf("%s-%s", Namespace, GetInstanceName(monoVertexRolloutName, 0))

		})

		// Verify no in progress strategy set
		VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
		VerifyMonoVertexRolloutInProgressStrategyConsistently(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		CheckEventually("verifying just 1 MonoVertex", func() int {
			return GetNumberOfChildren(GetGVRForMonoVertex(), Namespace, monoVertexRolloutName)
		}).Should(Equal(1))

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should pause the MonoVertex if user requests it and resume it", func() {
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, nil, mvtxMetadata)

		// test that pause works, as well as that monovertex resumes gradually
		testPauseResume(0, initialMonoVertexSpec, false)

		// update MonoVertexRollout to set FastResume to true such monovertex should resume fast, not gradually
		By("Setting FastResume=true")
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Strategy = &apiv1.PipelineTypeRolloutStrategy{
				PauseResumeStrategy: apiv1.PauseResumeStrategy{
					FastResume: true,
				},
			}

			return rollout, nil
		})

		// test that pause works, as well as that monovertex resumes fast
		testPauseResume(0, initialMonoVertexSpec, true)

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete the NumaflowControllerRollout and child NumaflowController", func() {
		DeleteNumaflowControllerRollout()
	})
})

// test that user can cause MonoVertex through MonoVertexRollout desiredPhase field
// as well as that user can unpause (either gradually or fast depending on configuration)
func testPauseResume(currentMonoVertexIndex int, currentMonoVertexSpec numaflowv1.MonoVertexSpec, resumeFast bool) {

	By("setting desiredPhase=Paused")
	originalMonoVertexSpec := currentMonoVertexSpec.DeepCopy()
	currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhasePaused

	UpdateMonoVertexRollout(monoVertexRolloutName, *originalMonoVertexSpec, currentMonoVertexSpec, numaflowv1.MonoVertexPhasePaused, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
		return retrievedMonoVertexSpec.Lifecycle.DesiredPhase == numaflowv1.MonoVertexPhasePaused
	}, false, currentMonoVertexIndex)

	VerifyPromotedMonoVertexStaysPaused(monoVertexRolloutName)

	By("setting desiredPhase=Running")

	currentMonoVertexSpec.Lifecycle.DesiredPhase = numaflowv1.MonoVertexPhaseRunning

	// Verify since strategy.pauseResume.resumeFast isn't set it defaults to resuming slowly
	// which means that spec.replicas will get set to null

	// patch monovertex's replicas to 5, thereby imitating the Numaflow autoscaler scaling up
	promotedMonoVertexName, err := GetPromotedMonoVertexName(Namespace, monoVertexRolloutName)
	Expect(err).ShouldNot(HaveOccurred())
	UpdateMonoVertexInK8S(promotedMonoVertexName, func(monoVertex *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		newReplicas := int64(monoVertexScaleMax)
		unstructured.RemoveNestedField(monoVertex.Object, "spec", "replicas")
		err := unstructured.SetNestedField(monoVertex.Object, newReplicas, "spec", "replicas")
		return monoVertex, err
	})

	// Resume MonoVertex
	UpdateMonoVertexRollout(monoVertexRolloutName, *originalMonoVertexSpec, currentMonoVertexSpec, numaflowv1.MonoVertexPhaseRunning, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
		return retrievedMonoVertexSpec.Lifecycle.DesiredPhase == numaflowv1.MonoVertexPhaseRunning
	}, false, currentMonoVertexIndex)

	// then verify that replicas is null
	VerifyPromotedMonoVertexSpec(Namespace, monoVertexRolloutName, func(spec numaflowv1.MonoVertexSpec) bool {
		if resumeFast {
			return spec.Replicas != nil && *spec.Replicas == 5
		} else {
			return spec.Replicas == nil
		}
	})
}
