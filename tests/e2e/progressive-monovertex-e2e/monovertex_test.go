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
	"fmt"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName = "test-monovertex-rollout"
)

var (
	monoVertexScaleMin  = int32(4)
	monoVertexScaleMax  = int32(5)
	zeroReplicaSleepSec = uint32(15)

	monoVertexScaleTo               = int64(2)
	monoVertexScaleMinMaxJSONString = fmt.Sprintf("{\"max\":%d,\"min\":%d}", monoVertexScaleMax, monoVertexScaleMin)

	defaultStrategy = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "60,180,30,10",
			},
		},
	}

	udTransformer             = numaflowv1.UDTransformer{Container: &numaflowv1.Container{}}
	validUDTransformerImage   = "quay.io/numaio/numaflow-rs/source-transformer-now:stable"
	invalidUDTransformerImage = "quay.io/numaio/numaflow-rs/source-transformer-now:invalid-e8y78rwq5h"

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
)

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive MonoVertex E2E Suite")
}

var _ = Describe("Progressive MonoVertex E2E", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
	})

	It("Should validate MonoVertex upgrade using Progressive strategy", func() {
		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, &defaultStrategy)

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForFailure(monoVertexRolloutName, invalidUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updatedMonoVertexSpec, monoVertexScaleTo, false)

		updatedMonoVertexSpec = UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveSuccess(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo, updatedMonoVertexSpec,
			0, 2, false, true)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 1))

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should validate MonoVertex upgrade using Progressive strategy via Forced Promotion configured on MonoVertexRollout Failure case", func() {
		strategy := defaultStrategy.DeepCopy()
		strategy.Progressive.ForcePromote = true
		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, strategy)

		By("Updating the MonoVertex Topology to cause a Progressive change Force promoted failure into success")
		updatedMonoVertexSpec := UpdateMonoVertexRolloutForFailure(monoVertexRolloutName, invalidUDTransformerImage, initialMonoVertexSpec, udTransformer)

		VerifyMonoVertexProgressiveSuccess(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo, updatedMonoVertexSpec,
			0, 1, true, false)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should validate MonoVertex upgrade using Progressive strategy via Forced Promotion configured on MonoVertex", func() {
		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, &defaultStrategy)

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForFailure(monoVertexRolloutName, invalidUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updatedMonoVertexSpec, monoVertexScaleTo, false)

		By("Updating the MonoVertex to set the 'force promote' Label")
		UpdateMonoVertexInK8S(GetInstanceName(monoVertexRolloutName, 1), func(monovertex *unstructured.Unstructured) (*unstructured.Unstructured, error) {
			labels := monovertex.GetLabels()
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[common.LabelKeyForcePromote] = "true"
			monovertex.SetLabels(labels)
			return monovertex, nil
		})

		VerifyMonoVertexProgressiveSuccess(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo, updatedMonoVertexSpec,
			0, 1, true, false)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
