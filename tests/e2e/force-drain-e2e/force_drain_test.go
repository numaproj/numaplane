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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
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
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	pullPolicyAlways    = corev1.PullAlways
	validImagePath      = "quay.io/numaio/numaflow-go/map-cat:stable"
	onePod              = int32(1)
	twoPods             = int32(2)
	threePods           = int32(3)
	fourPods            = int32(4)
	fivePods            = int32(5)
	zeroReplicaSleepSec = uint32(15) // if for some reason the Vertex has 0 replicas, this will cause Numaflow to scale it back up
	initialPipelineSpec = numaflowv1.PipelineSpec{
		Templates: &numaflowv1.Templates{
			VertexTemplate: &numaflowv1.VertexTemplate{
				ContainerTemplate: &numaflowv1.ContainerTemplate{
					Env: []corev1.EnvVar{
						{Name: "NUMAFLOW_DEBUG", Value: "true"},
					},
				},
			},
		},
		InterStepBufferServiceName: isbServiceRolloutName,
		Lifecycle: numaflowv1.Lifecycle{
			PauseGracePeriodSeconds: ptr.To(int64(60)),
		},
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
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &onePod, Max: &onePod, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

	updatedPipelineSpec *numaflowv1.PipelineSpec
)

func init() {
	updatedPipelineSpec = initialPipelineSpec.DeepCopy()
	updatedPipelineSpec.Vertices[2] = numaflowv1.AbstractVertex{
		Name: "out",
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				Blackhole: &numaflowv1.Blackhole{},
			},
		},
	}
}

func TestForceDrainE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Force Drain - E2E Suite")
}

var _ = Describe("Force Drain e2e", Serial, func() {

	It("Should create NumaflowControllerRollout, ISBServiceRollout, PipelineRollout", func() {
		CreateNumaflowControllerRollout(UpdatedNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)

		// this will be the original successful Pipeline to drain
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil, apiv1.Metadata{})
	})

	It("Should create 2 failed Pipelines which will need to be drained and deleted and update back to original Pipeline", func() {

		// Create pipeline-1 (progressive-replaced) and pipeline-2, then restore immediately.
		// Do not wait for failure assessment: it races on fast clusters and is cleared after discontinue.
		updateFailedPipelinesBackToBack(1, false)

		updatePipeline(&initialPipelineSpec)
		VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)

		VerifyPromotedPipelineExists(Namespace, pipelineRolloutName)
		VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(spec numaflowv1.PipelineSpec) bool {
			return spec.Vertices[1].UDF != nil &&
				spec.Vertices[1].UDF.Container != nil &&
				spec.Vertices[1].UDF.Container.Image == validImagePath
		})

		verifyPipelinesPausingWithValidSpecAndDeleted([]int{1, 2})

		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 1), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 2), "Normal")
	})

	It("Should create 3 failed Pipelines which attempt to drain with an unhealthy Pipeline spec", func() {

		updateFailedPipelinesBackToBack(3, false)
		VerifyPipelineExists(Namespace, GetInstanceName(pipelineRolloutName, 4))
		forcePromote(pipelineRolloutName, 4)
		verifyRecyclablePipelinesFailedDrainAttempts([]int{3}, GetInstanceName(pipelineRolloutName, 4), "badpath2")

		updatePipelineImage("badpath3")
		VerifyPipelineExists(Namespace, GetInstanceName(pipelineRolloutName, 5))
		forcePromote(pipelineRolloutName, 5)
		verifyRecyclablePipelinesFailedDrainAttempts([]int{3, 4}, GetInstanceName(pipelineRolloutName, 5), "badpath3")

		// Pipeline-0 drains with its original spec and may already be deleted (CI artifact force-drain-7-5-1123).
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 0))
	})

	It("Should successfully drain all previous Pipelines once there's a Pipeline with a healthy spec", func() {

		// updated Pipeline updates the sink ("test-pipeline-rollout-6")
		updatePipeline(updatedPipelineSpec)

		verifyPipelinesPausingWithValidSpecAndDeleted([]int{3, 4, 5})

		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 0), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 3), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 4), "Normal")
		VerifyPipelineEvent(Namespace, GetInstanceName(pipelineRolloutName, 5), "Normal")

	})

	It("Should reach max recyclable duration and delete the Pipeline", func() {

		// updateFailedPipelinesBackToBack takes several minutes; keep a generous limit during setup
		// so the recyclable Pipeline is not deleted before force promote and drain verification.
		UpdateNumaplaneControllerConfig(map[string]string{
			"pipeline.maxRecyclableDurationMinutes": "60",
		})

		// create test-pipeline-rollout-7 and test-pipeline-rollout-8 and force promote 8
		updateFailedPipelinesBackToBack(7, false)
		VerifyPipelineExists(Namespace, GetInstanceName(pipelineRolloutName, 8))
		forcePromote(pipelineRolloutName, 8)
		verifyRecyclablePipelinesFailedDrainAttempts([]int{7}, GetInstanceName(pipelineRolloutName, 8), "badpath2")

		// Pipeline-7 has been recyclable for longer than 2 minutes by now; apply the short limit
		// and verify the controller deletes it even though drain cannot succeed.
		UpdateNumaplaneControllerConfig(map[string]string{
			"pipeline.maxRecyclableDurationMinutes": "2",
		})

		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 7))
	})

	It("Should Delete Rollouts", func() {
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

// update twice:
// the first pipeline will be failed but it will be replaced before it's assessed
// the second pipeline will actually be assessed as Failed
// We test both since they take slightly different paths in the recycle code
// nextIndex is the expected index of the first pipeline that will be created (second will be nextIndex+1)
func updateFailedPipelinesBackToBack(nextIndex int, waitForFailureAssessment ...bool) {
	wait := true
	if len(waitForFailureAssessment) > 0 {
		wait = waitForFailureAssessment[0]
	}

	// this will be a failed Pipeline which will be replaced before it has a chance to be assessed
	time.Sleep(10 * time.Second)
	updatePipelineImage("badpath1")

	// verify the first pipeline was created
	VerifyPipelineExists(Namespace, GetInstanceName(pipelineRolloutName, nextIndex))

	// this will be a failed Pipeline which will be assessed as Failed
	time.Sleep(45 * time.Second)
	updatePipelineImage("badpath2")

	// verify the second pipeline was created
	VerifyPipelineExists(Namespace, GetInstanceName(pipelineRolloutName, nextIndex+1))

	if !wait {
		return
	}

	waitForUpgradingPipelineFailureAssessment(nextIndex + 1)
}

func waitForUpgradingPipelineFailureAssessment(pipelineIndex int) {
	VerifyPipelineRolloutStatusEventually(pipelineRolloutName, func(status apiv1.PipelineRolloutStatus) bool {
		return status.ProgressiveStatus.UpgradingPipelineStatus != nil &&
			status.ProgressiveStatus.UpgradingPipelineStatus.Name == GetInstanceName(pipelineRolloutName, pipelineIndex) &&
			status.ProgressiveStatus.UpgradingPipelineStatus.AssessmentResult == apiv1.AssessmentResultFailure
	})
}

func verifyRecyclablePipelinesFailedDrainAttempts(pipelineIndices []int, forceDrainSourcePipelineName string, forceDrainImagePath string) {
	for _, pipelineIndex := range pipelineIndices {
		verifyRecyclablePipelineFailedDrainAttempt(pipelineIndex, forceDrainSourcePipelineName, forceDrainImagePath)
	}
}

// verifyRecyclablePipelineFailedDrainAttempt checks per-pipeline unhealthy force-drain progress.
// Prefer rollout status DrainAttemptComplete (stable); use live pipeline spec only while in progress.
// Do not use for test 2 (restore/discontinue): assert end state via deletion only.
func verifyRecyclablePipelineFailedDrainAttempt(pipelineIndex int, forceDrainSourcePipelineName string, forceDrainImagePath string) {
	pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	CheckEventually(fmt.Sprintf("Verifying recyclable Pipeline %s drain attempt status", pipelineName), func() bool {
		rolloutStatus, err := GetPipelineRolloutStatus(pipelineRolloutName)
		if err != nil {
			return false
		}

		recyclableStatus := rolloutStatus.ProgressiveStatus.GetRecyclablePipelineStatus(pipelineName)
		if recyclableStatus == nil {
			return false
		}

		originalAttempt := recyclableStatus.GetDrainAttempt(pipelineName)
		if originalAttempt == nil || !originalAttempt.DrainAttemptComplete {
			return false
		}

		forceAttempt := recyclableStatus.GetDrainAttempt(forceDrainSourcePipelineName)
		if forceAttempt == nil {
			return false
		}

		// Stable signal from status; does not require the pipeline object to still exist.
		if forceAttempt.DrainAttemptComplete {
			return true
		}

		_, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(Namespace, pipelineName)
		if errors.IsNotFound(err) {
			return false
		}
		if err != nil {
			return false
		}

		// Accept mid-drain or spec already applied while the attempt is still in progress.
		if pipelinePausingDuringForceDrain(retrievedPipelineSpec, retrievedPipelineStatus, forceDrainImagePath) {
			return true
		}
		return pipelineHasForceDrainSpecApplied(retrievedPipelineSpec, forceDrainImagePath)
	}).WithTimeout(DefaultTestTimeout).Should(BeTrue())
}

func pipelineHasForceDrainSpecApplied(retrievedPipelineSpec numaflowv1.PipelineSpec, forceDrainImagePath string) bool {
	return retrievedPipelineSpec.Vertices[1].UDF != nil &&
		retrievedPipelineSpec.Vertices[1].UDF.Container != nil &&
		retrievedPipelineSpec.Vertices[1].UDF.Container.Image == forceDrainImagePath &&
		retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused
}

func pipelinePausingWithValidSpec(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus) bool {
	return pipelinePausingDuringForceDrain(retrievedPipelineSpec, retrievedPipelineStatus, validImagePath)
}

func pipelinePausingDuringForceDrain(retrievedPipelineSpec numaflowv1.PipelineSpec, retrievedPipelineStatus numaflowv1.PipelineStatus, forceDrainImagePath string) bool {
	return retrievedPipelineSpec.Vertices[1].UDF != nil && retrievedPipelineSpec.Vertices[1].UDF.Container != nil &&
		retrievedPipelineSpec.Vertices[1].UDF.Container.Image == forceDrainImagePath &&
		retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused &&
		(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePausing ||
			(retrievedPipelineStatus.Phase == numaflowv1.PipelinePhasePaused && retrievedPipelineStatus.DrainedOnPause))
}

func verifyPipelinesPausingWithValidSpecAndDeleted(pipelineIndices []int) {
	for _, pipelineIndex := range pipelineIndices {
		pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
		CheckEventually(fmt.Sprintf("Verifying Pipeline %s pausing with valid spec or deleted", pipelineName), func() bool {
			pipeline, retrievedPipelineSpec, retrievedPipelineStatus, err := GetPipelineSpecAndStatus(Namespace, pipelineName)
			if errors.IsNotFound(err) {
				// A faster pipeline may already be drained and deleted while we wait on others.
				return true
			}
			if err != nil {
				return false
			}

			annotations, found, err := unstructured.NestedMap(pipeline.Object, "metadata", "annotations")
			if !found || err != nil || annotations == nil {
				return false
			}

			return pipelinePausingWithValidSpec(retrievedPipelineSpec, retrievedPipelineStatus)
		}).WithTimeout(DefaultTestTimeout).Should(BeTrue())

		VerifyPipelineDeletion(pipelineName)
	}
}

func updatePipeline(pipelineSpec *numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})
}

func forcePromote(pipelineRolloutName string, pipelineIndex int) {
	pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
	By(fmt.Sprintf("Force promoting Pipeline %s", pipelineName))
	UpdatePipelineInK8S(pipelineName, func(pipeline *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		labels := pipeline.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[common.LabelKeyForcePromote] = "true"
		pipeline.SetLabels(labels)
		return pipeline, nil
	})
}

func updatePipelineImage(imagePath string) *numaflowv1.PipelineSpec {
	By(fmt.Sprintf("Updating PipelineRollout with image %s", imagePath))
	pipelineSpec := initialPipelineSpec.DeepCopy()
	pipelineSpec.Vertices[1] = numaflowv1.AbstractVertex{
		Name: "cat",
		UDF: &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image: imagePath,
			},
		},
	}
	updatePipeline(pipelineSpec)
	return pipelineSpec
}
