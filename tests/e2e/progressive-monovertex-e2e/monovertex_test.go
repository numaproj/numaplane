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
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
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
				AssessmentSchedule: "60,30,10",
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
		createInitialMonoVertexRollout(&defaultStrategy)

		updatedMonoVertexSpec := updateMonoVertexRolloutForFailure()
		verifyProgressiveFailure(updatedMonoVertexSpec)

		updatedMonoVertexSpec = updateMonoVertexRolloutForSuccess()
		verifyProgressiveSuccess(updatedMonoVertexSpec, 0, 2, false, true)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 1))

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should validate MonoVertex upgrade using Progressive strategy via Forced Promotion configured on MonoVertexRollout Failure case", func() {
		strategy := defaultStrategy.DeepCopy()
		strategy.Progressive.ForcePromote = true
		createInitialMonoVertexRollout(strategy)

		By("Updating the MonoVertex Topology to cause a Progressive change Force promoted failure into success")
		updatedMonoVertexSpec := updateMonoVertexRolloutForFailure()

		verifyProgressiveSuccess(updatedMonoVertexSpec, 0, 1, true, false)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should validate MonoVertex upgrade using Progressive strategy via Forced Promotion configured on MonoVertex", func() {
		createInitialMonoVertexRollout(&defaultStrategy)

		updatedMonoVertexSpec := updateMonoVertexRolloutForFailure()
		verifyProgressiveFailure(updatedMonoVertexSpec)

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

		verifyProgressiveSuccess(updatedMonoVertexSpec, 0, 1, true, false)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		DeleteMonoVertexRollout(monoVertexRolloutName)

	})

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})

func createInitialMonoVertexRollout(strategy *apiv1.PipelineTypeRolloutStrategy) {
	By("Creating a MonoVertexRollout")
	CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, strategy)

	By("Verifying that the MonoVertex spec is as expected")
	VerifyMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
		return reflect.DeepEqual(retrievedMonoVertexSpec, initialMonoVertexSpec)
	})
	VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyMonoVertexRolloutHealthy(monoVertexRolloutName)
}

func updateMonoVertexRolloutForFailure() *numaflowv1.MonoVertexSpec {
	By("Updating the MonoVertex Topology to cause a Progressive change Failure case")
	updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
	updatedMonoVertexSpec.Source.UDTransformer = &udTransformer
	updatedMonoVertexSpec.Source.UDTransformer.Container.Image = invalidUDTransformerImage
	rawSpec, err := json.Marshal(updatedMonoVertexSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(mvr apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
		mvr.Spec.MonoVertex.Spec.Raw = rawSpec
		return mvr, nil
	})
	return updatedMonoVertexSpec
}

func verifyProgressiveFailure(updatedMonoVertexSpec *numaflowv1.MonoVertexSpec) {
	VerifyMonoVertexRolloutScaledDownForProgressive(monoVertexRolloutName, GetInstanceName(monoVertexRolloutName, 0), monoVertexScaleMinMaxJSONString, monoVertexScaleTo)
	VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, GetInstanceName(monoVertexRolloutName, 0), GetInstanceName(monoVertexRolloutName, 1), true, apiv1.AssessmentResultFailure, defaultStrategy.Progressive.ForcePromote)

	// Verify that when the "upgrading" MonoVertex fails, it scales down to 0 Pods, and the "promoted" MonoVertex scales back up
	VerifyVerticesPodsRunning(Namespace, GetInstanceName(monoVertexRolloutName, 0),
		[]numaflowv1.AbstractVertex{{Scale: updatedMonoVertexSpec.Scale}}, ComponentMonoVertex)
	VerifyVerticesPodsRunning(Namespace, GetInstanceName(monoVertexRolloutName, 1),
		[]numaflowv1.AbstractVertex{{Scale: numaflowv1.Scale{Min: ptr.To(int32(0)), Max: ptr.To(int32(0))}}}, ComponentMonoVertex)
}

func updateMonoVertexRolloutForSuccess() *numaflowv1.MonoVertexSpec {
	By("Updating the MonoVertex Topology to cause a Progressive change Successful case")
	updatedMonoVertexSpec := initialMonoVertexSpec.DeepCopy()
	updatedMonoVertexSpec.Source.UDTransformer = &udTransformer
	updatedMonoVertexSpec.Source.UDTransformer.Container.Image = validUDTransformerImage
	rawSpec, err := json.Marshal(updatedMonoVertexSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(mvr apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
		mvr.Spec.MonoVertex.Spec.Raw = rawSpec
		return mvr, nil
	})
	return updatedMonoVertexSpec
}

func verifyProgressiveSuccess(updatedMonoVertexSpec *numaflowv1.MonoVertexSpec, promotedMonoVertexIndex int, updatedMonoVertexIndex int, forcedSuccess bool, checkRunningVertices bool) {
	if !forcedSuccess {
		VerifyMonoVertexRolloutScaledDownForProgressive(monoVertexRolloutName, GetInstanceName(monoVertexRolloutName, promotedMonoVertexIndex), monoVertexScaleMinMaxJSONString, monoVertexScaleTo)
	}
	VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, GetInstanceName(monoVertexRolloutName, promotedMonoVertexIndex), GetInstanceName(monoVertexRolloutName, updatedMonoVertexIndex), false, apiv1.AssessmentResultSuccess, forcedSuccess)

	VerifyMonoVertexPromotedScale(Namespace, monoVertexRolloutName, map[string]numaflowv1.Scale{
		GetInstanceName(monoVertexRolloutName, updatedMonoVertexIndex): updatedMonoVertexSpec.Scale,
	})

	VerifyMonoVertexUpgradeState(Namespace, GetInstanceName(monoVertexRolloutName, updatedMonoVertexIndex), common.LabelValueUpgradePromoted)

	if checkRunningVertices {
		VerifyVerticesPodsRunning(Namespace, GetInstanceName(monoVertexRolloutName, updatedMonoVertexIndex),
			[]numaflowv1.AbstractVertex{{Scale: updatedMonoVertexSpec.Scale}}, ComponentMonoVertex)
	}

}
