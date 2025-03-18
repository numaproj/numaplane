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
		RolloutStrategy: apiv1.RolloutStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "30,30,10",
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

		// TODO: verify progressive status changes here (ex: scale values)
		// - check that the upgrading monovertex is scaled to zero after upgrade failure: look at monovertex spec scale and replicas and also at actual running pods count
		// - check that the promoted monovertex is scaled to original after upgrade failure: look at monovertex spec scale and replicas and also at actual running pods count

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

		// TODO: verify progressive status changes here (ex: scale values)
		// - check that the upgrading monovertex is scaled to zero after upgrade failure: look at monovertex spec scale and replicas and also at actual running pods count
		// - check that the promoted monovertex is scaled to original after upgrade failure: look at monovertex spec scale and replicas and also at actual running pods count

		time.Sleep(5 * time.Second)

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	// TODO: tests for pipeline

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
