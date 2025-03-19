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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"

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

	monoVertexScaleTo               = int64(2)
	monoVertexScaleMinMaxJSONString = fmt.Sprintf("{\"max\":%d,\"min\":%d}", monoVertexScaleMax, monoVertexScaleMin)

	defaultStrategy = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "120,60,10", // TODO: revisit these values
			},
		},
	}

	udTransformer             = &numaflowv1.UDTransformer{Container: &numaflowv1.Container{}}
	validUDTransformerImage   = "docker.intuit.com/quay-rmt/numaio/numaflow-rs/source-transformer-now:stable"
	invalidUDTransformerImage = "docker.intuit.com/quay-rmt/numaio/numaflow-rs/source-transformer-now:invalid-e8y78rwq5h"

	initialMonoVertexSpec = &numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{Min: &monoVertexScaleMin, Max: &monoVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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
		// TODO: add ISBSvc when adding pipeline tests
	})

	It("Should validate MonoVertex upgrade using Progressive strategy", func() {
		By("Creating a MonoVertexRollout")
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, *initialMonoVertexSpec, &defaultStrategy)

		By("Verifying that the MonoVertex was created")
		VerifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return *retrievedMonoVertexSpec.Scale.Min == *initialMonoVertexSpec.Scale.Min &&
				retrievedMonoVertexSpec.Source.UDSource.Container.Image == initialMonoVertexSpec.Source.UDSource.Container.Image
		})
		VerifyInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		time.Sleep(5 * time.Second)

		By("Updating the MonoVertex Topology to cause a Progressive change - Failure case")
		updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
		updatedMonoVertexSpec.Source.UDTransformer = udTransformer
		updatedMonoVertexSpec.Source.UDTransformer.Container.Image = invalidUDTransformerImage

		rawSpec, err := json.Marshal(updatedMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(mvr apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			mvr.Spec.MonoVertex.Spec.Raw = rawSpec
			return mvr, nil
		})

		VerifyMonoVertexRolloutScaledDownForProgressive(monoVertexRolloutName, 0, monoVertexScaleTo, int64(monoVertexScaleMin), monoVertexScaleMinMaxJSONString, monoVertexScaleTo)
		VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, 0, 1, true, apiv1.AssessmentResultFailure)

		VerifyVerticesPodsRunning(Namespace, fmt.Sprintf("%s-%d", monoVertexRolloutName, 0),
			[]numaflowv1.AbstractVertex{{Scale: updatedMonoVertexSpec.Scale}}, ComponentMonoVertex)
		VerifyVerticesPodsRunning(Namespace, fmt.Sprintf("%s-%d", monoVertexRolloutName, 1),
			[]numaflowv1.AbstractVertex{{Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}}}, ComponentMonoVertex)

		time.Sleep(5 * time.Second)

		By("Updating the MonoVertex Topology to cause a Progressive change - Successful case")
		updatedMonoVertexSpec = initialMonoVertexSpec.DeepCopy()
		updatedMonoVertexSpec.Source.UDTransformer = udTransformer
		updatedMonoVertexSpec.Source.UDTransformer.Container.Image = validUDTransformerImage

		rawSpec, err = json.Marshal(updatedMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(mvr apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			mvr.Spec.MonoVertex.Spec.Raw = rawSpec
			return mvr, nil
		})

		VerifyMonoVertexRolloutScaledDownForProgressive(monoVertexRolloutName, 0, monoVertexScaleTo, int64(monoVertexScaleMin), monoVertexScaleMinMaxJSONString, monoVertexScaleTo)
		VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, 0, 2, false, apiv1.AssessmentResultSuccess)

		VerifyVerticesPodsRunning(Namespace, fmt.Sprintf("%s-%d", monoVertexRolloutName, 2),
			[]numaflowv1.AbstractVertex{{Scale: updatedMonoVertexSpec.Scale}}, ComponentMonoVertex)
		VerifyVerticesPodsRunning(Namespace, fmt.Sprintf("%s-%d", monoVertexRolloutName, 1),
			[]numaflowv1.AbstractVertex{{Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}}}, ComponentMonoVertex)

		time.Sleep(5 * time.Second)

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should validate MonoVertex upgrade using Progressive strategy via Forced Promotion", func() {
		By("Creating a MonoVertexRollout with ForcePromote enabled")
		strategy := defaultStrategy.DeepCopy()
		strategy.Progressive.ForcePromote = true
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, *initialMonoVertexSpec, strategy)

		By("Verifying that the MonoVertex was created")
		VerifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
			return *retrievedMonoVertexSpec.Scale.Min == *initialMonoVertexSpec.Scale.Min &&
				retrievedMonoVertexSpec.Source.UDSource.Container.Image == initialMonoVertexSpec.Source.UDSource.Container.Image
		})
		VerifyInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)

		time.Sleep(5 * time.Second)

		By("Updating the MonoVertex Topology to cause a Progressive change - Force promoted failure into success")
		updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
		updatedMonoVertexSpec.Source.UDTransformer = udTransformer
		updatedMonoVertexSpec.Source.UDTransformer.Container.Image = invalidUDTransformerImage

		rawSpec, err := json.Marshal(updatedMonoVertexSpec)
		Expect(err).ShouldNot(HaveOccurred())
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(mvr apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			mvr.Spec.MonoVertex.Spec.Raw = rawSpec
			return mvr, nil
		})

		VerifyMonoVertexRolloutScaledDownForProgressive(monoVertexRolloutName, 0, monoVertexScaleTo, int64(monoVertexScaleMin), monoVertexScaleMinMaxJSONString, monoVertexScaleTo)
		// TODO:
		// VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, 0, 1, false, apiv1.AssessmentResultSuccess)
		/*
					progressiveStatus:
			    upgradingMonoVertexStatus:
			      assessmentResult: Success
			      forcedSuccess: true
			      name: monovertex-rollout-1
		*/

		// TODO: test these (WIP)
		// VerifyVerticesPodsRunning(Namespace, fmt.Sprintf("%s-%d", monoVertexRolloutName, 1),
		// 	[]numaflowv1.AbstractVertex{{Scale: updatedMonoVertexSpec.Scale}}, ComponentMonoVertex)
		// VerifyVerticesPodsRunning(Namespace, fmt.Sprintf("%s-%d", monoVertexRolloutName, 0),
		// 	[]numaflowv1.AbstractVertex{{Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}}}, ComponentMonoVertex)

		time.Sleep(5 * time.Second)

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	// TODO: tests for pipeline

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
