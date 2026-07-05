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
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
	validImagePath        = "quay.io/numaio/numaflow-go/map-cat:stable"
)

var (
	volSize, _            = apiresource.ParseQuantity("10Mi")
	initialISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: InitialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}

	pipelineSpecSourceRPU      = int64(1000)
	progressiveAlternateSourceRPU = int64(500)
	finalPromotionSourceRPU       = int64(501)
	pipelineSpecSourceDuration = metav1.Duration{Duration: time.Second}
	pullPolicyAlways           = corev1.PullAlways
	onePod                     = int32(1)
	twoPods                    = int32(2)
	threePods                  = int32(3)
	fourPods                   = int32(4)
	fivePods                   = int32(5)
	zeroReplicaSleepSec        = uint32(15)
	initialPipelineSpec        = numaflowv1.PipelineSpec{
		Templates: &numaflowv1.Templates{
			VertexTemplate: &numaflowv1.VertexTemplate{
				ContainerTemplate: &numaflowv1.ContainerTemplate{
					Env: []corev1.EnvVar{{Name: "NUMAFLOW_DEBUG", Value: "true"}},
				},
			},
		},
		InterStepBufferServiceName: isbServiceRolloutName,
		Lifecycle:                  numaflowv1.Lifecycle{PauseGracePeriodSeconds: ptr.To(int64(60))},
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &twoPods, Max: &threePods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Container: &numaflowv1.Container{
						Image:           validImagePath,
						ImagePullPolicy: &pullPolicyAlways,
					},
				},
				Scale: numaflowv1.Scale{Min: &fourPods, Max: &fivePods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name:  "out",
				Sink:  &numaflowv1.Sink{AbstractSink: numaflowv1.AbstractSink{Log: &numaflowv1.Log{}}},
				Scale: numaflowv1.Scale{Min: &onePod, Max: &onePod, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{{From: "in", To: "cat"}, {From: "cat", To: "out"}},
	}

	progressiveAlternatePipelineSpec *numaflowv1.PipelineSpec
	finalPromotionPipelineSpec       *numaflowv1.PipelineSpec
)

func init() {
	// log/blackhole sink changes use UpgradeStrategyApply (in-place), not Progressive.
	// Use generator RPU changes so createRecyclablePipelineViaDiscontinue creates a new Pipeline child.
	progressiveAlternatePipelineSpec = initialPipelineSpec.DeepCopy()
	progressiveAlternatePipelineSpec.Vertices[0].Source.Generator.RPU = &progressiveAlternateSourceRPU

	finalPromotionPipelineSpec = initialPipelineSpec.DeepCopy()
	finalPromotionPipelineSpec.Vertices[0].Source.Generator.RPU = &finalPromotionSourceRPU
}

func TestForceDrainBackwardCompatibilityE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Force Drain Backward Compatibility - E2E Suite")
}

var _ = Describe("Force drain backward compatibility e2e", Serial, func() {

	It("Should create NumaflowControllerRollout, ISBServiceRollout, PipelineRollout", func() {
		CreateNumaflowControllerRollout(UpdatedNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil, apiv1.Metadata{})
	})

	It("Should migrate started-only legacy annotations and delete the recyclable Pipeline", func() {
		promotedPipelineName := GetInstanceName(pipelineRolloutName, 0)
		recyclablePipelineName := GetInstanceName(pipelineRolloutName, 1)

		createRecyclablePipelineViaDiscontinue(1)
		applyMidForceDrainPipelineSpec(1)
		injectLegacyForceDrainState(
			pipelineRolloutName,
			recyclablePipelineName,
			[]string{promotedPipelineName},
			nil,
		)

		// Only assert the synthetic original-spec attempt migration prepends (stable once migrated).
		// We do not check the promoted Pipeline's drain attempt here: after migration it is in-progress
		// (false) until force drain completes (true), then the recyclable status entry is pruned on delete—
		// either timing races with Eventually. Deletion below is the completion check.
		VerifyRecyclablePipelineDrainAttempts(pipelineRolloutName, recyclablePipelineName, []ExpectedDrainAttempt{
			{SourcePipelineSpec: recyclablePipelineName, DrainAttemptComplete: true},
		})

		VerifyPipelineDeletion(recyclablePipelineName)
	})

	It("Should migrate started+completed legacy annotations and delete after a new promoted Pipeline", func() {
		oldPromotedPipelineName := GetInstanceName(pipelineRolloutName, 0)
		recyclablePipelineName := GetInstanceName(pipelineRolloutName, 2)
		newPromotedPipelineName := GetInstanceName(pipelineRolloutName, 3)

		createRecyclablePipelineViaDiscontinue(2)
		injectLegacyForceDrainState(
			pipelineRolloutName,
			recyclablePipelineName,
			[]string{oldPromotedPipelineName},
			[]string{oldPromotedPipelineName},
		)

		// Both complete attempts are stable after migration (legacy started+completed annotations).
		// We do not assert the new promoted Pipeline's in-progress drain attempt: it would race the
		// same way as the started-only case. Scaled-to-zero, new promotion, and deletion below
		// cover skip-and-wait plus reattempt behavior.
		VerifyRecyclablePipelineDrainAttempts(pipelineRolloutName, recyclablePipelineName, []ExpectedDrainAttempt{
			{SourcePipelineSpec: recyclablePipelineName, DrainAttemptComplete: true},
			{SourcePipelineSpec: oldPromotedPipelineName, DrainAttemptComplete: true},
		})

		verifyPipelineScaledToZero(2)
		updatePipeline(finalPromotionPipelineSpec)

		VerifyPipelineRolloutStatusEventually(pipelineRolloutName, func(status apiv1.PipelineRolloutStatus) bool {
			return status.ProgressiveStatus.PromotedPipelineStatus != nil &&
				status.ProgressiveStatus.PromotedPipelineStatus.Name == newPromotedPipelineName
		})

		VerifyPipelineDeletion(recyclablePipelineName)
	})

	It("Should Delete Rollouts", func() {
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

// createRecyclablePipelineViaDiscontinue performs a quick A->B->A progressive update so the upgrading
// Pipeline is marked recyclable without running a failed assessment path. Spec B must use a field that
// triggers UpgradeStrategyProgressive (e.g. generator RPU); log/blackhole sink changes apply in-place.
func createRecyclablePipelineViaDiscontinue(recyclableIndex int) {
	recyclablePipelineName := GetInstanceName(pipelineRolloutName, recyclableIndex)

	updatePipeline(progressiveAlternatePipelineSpec)
	VerifyPipelineExists(Namespace, recyclablePipelineName)

	updatePipeline(&initialPipelineSpec)

	discontinueReason := string(common.LabelValueDiscontinueProgressive)
	VerifyPipelineUpgradeState(Namespace, recyclablePipelineName, string(common.LabelValueUpgradeRecyclable), &discontinueReason)
}

// applyMidForceDrainPipelineSpec simulates a recyclable Pipeline that already has the promoted spec
// force-applied and is mid force-drain (as the legacy annotations imply).
func applyMidForceDrainPipelineSpec(recyclablePipelineIndex int) {
	recyclablePipelineName := GetInstanceName(pipelineRolloutName, recyclablePipelineIndex)
	UpdatePipelineInK8S(recyclablePipelineName, func(pipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		err := UpdatePipelineSpec(pipeline, func(spec numaflowv1.PipelineSpec) (numaflowv1.PipelineSpec, error) {
			spec.Vertices[1].UDF.Container.Image = validImagePath
			spec.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused
			return spec, nil
		})
		return pipeline, err
	})
}

// injectLegacyForceDrainState sets legacy force-drain annotations and ensures PipelineRollout status has
// no DrainAttempts, simulating a cluster upgraded from a controller that only used annotations.
func injectLegacyForceDrainState(
	pipelineRolloutName string,
	recyclablePipelineName string,
	forceDrainStarted []string,
	forceDrainCompleted []string,
) {
	setPipelineLegacyForceDrainAnnotations(recyclablePipelineName, forceDrainStarted, forceDrainCompleted)
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		recyclablePipelineStatus := rollout.Status.ProgressiveStatus.GetOrCreateRecyclablePipelineStatus(recyclablePipelineName)
		recyclablePipelineStatus.DrainAttempts = nil
		return rollout, nil
	})
}

func setPipelineLegacyForceDrainAnnotations(pipelineName string, forceDrainStarted, forceDrainCompleted []string) {
	UpdatePipelineInK8S(pipelineName, func(pipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		annotations := pipeline.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[common.AnnotationKeyForceDrainSpecsStarted] = joinCommaDelimitedAnnotationValues(forceDrainStarted)
		if len(forceDrainCompleted) > 0 {
			annotations[common.AnnotationKeyForceDrainSpecsCompleted] = joinCommaDelimitedAnnotationValues(forceDrainCompleted)
		} else {
			delete(annotations, common.AnnotationKeyForceDrainSpecsCompleted)
		}
		pipeline.SetAnnotations(annotations)
		return pipeline, nil
	})
}

func joinCommaDelimitedAnnotationValues(values []string) string {
	if len(values) == 0 {
		return ""
	}
	result := ""
	for _, value := range values {
		result += value + ","
	}
	return result
}

func updatePipeline(pipelineSpec *numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})
}

func verifyPipelineScaledToZero(pipelineIndex int) {
	pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	VerifyPipelineSpecStatus(Namespace, pipelineName, func(spec numaflowv1.PipelineSpec, status numaflowv1.PipelineStatus) bool {
		for _, vertex := range spec.Vertices {
			if vertex.Scale.Min == nil || *vertex.Scale.Min != 0 {
				return false
			}
			if vertex.Scale.Max == nil || *vertex.Scale.Max != 0 {
				return false
			}
		}
		return true
	})
}
