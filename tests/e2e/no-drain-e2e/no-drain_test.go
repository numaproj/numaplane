package nodraine2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		Redis: nil,
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
	zeroPods            = int32(0)
	onePod              = int32(1)
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
				Scale: numaflowv1.Scale{Min: &zeroPods, Max: &zeroPods, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

// The purpose of this test is verify that we won't try to drain a Pipeline that has never been configured in a way that it can ingest data
func TestNoDrainE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "No Drain - E2E Suite")
}

var _ = Describe("No Drain e2e", Serial, func() {

	It("Should create NumaflowControllerRollout, ISBServiceRollout, PipelineRollout with Source Vertex not ingesting data", func() {
		CreateNumaflowControllerRollout(UpdatedNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)

		// this will be the original successful Pipeline to drain
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil, metav1.ObjectMeta{})
	})

	It("Should not bother trying to drain a Pipeline which has been configured with scale.max=0", func() {
		// perform Progressive Rollout, thereby creating test-pipeline-rollout-1
		updatePipeline(updatedPipelineSpec)

		time.Sleep(30 * time.Second)

		// reverse the change, therefore causing test-pipeline-rollout-1 to be marked "recyclable"
		updatePipeline(&initialPipelineSpec)

		// verify the original Pipeline gets deleted without being paused first
		// (after Progressive upgrade succeeds)
		verifyPipelinesDeletedWithNoPause([]int{1})
	})

	// repeat the same test except confirm it still works if PipelineRollout is configured with desiredPhase=Paused instead of Source max=0
	It("Should not bother trying to drain a Pipeline which has been configured with desiredPhase=Paused", func() {
		initialPipelineSpecPaused := initialPipelineSpec.DeepCopy()
		initialPipelineSpecPaused.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused
		initialPipelineSpecPaused.Vertices[0].Scale = numaflowv1.Scale{} // this means min=max=1

		updatePipeline(initialPipelineSpecPaused)
		time.Sleep(20 * time.Second)

		updatedPipelineSpecPaused := updatedPipelineSpec.DeepCopy()
		updatedPipelineSpecPaused.Lifecycle.DesiredPhase = numaflowv1.PipelinePhasePaused
		updatedPipelineSpecPaused.Vertices[0].Scale = numaflowv1.Scale{} // this means min=max=1

		// perform Progressive Rollout, thereby creating test-pipeline-rollout-2
		updatePipeline(updatedPipelineSpecPaused)

		time.Sleep(30 * time.Second)

		// reverse the change, therefore causing test-pipeline-rollout-1 to be marked "recyclable"
		updatePipeline(initialPipelineSpecPaused)

		// verify the original Pipeline gets deleted without being paused first
		// (after Progressive upgrade succeeds)
		verifyPipelinesDeletedWithNoPause([]int{2})

	})

	It("Should Delete Rollouts", func() {
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

func updatePipeline(pipelineSpec *numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(pipelineSpec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		rollout.Spec.Pipeline.Spec.Raw = rawSpec
		return rollout, nil
	})
}

func verifyPipelinesDeletedWithNoPause(pipelineIndices []int) {
	CheckConsistently(fmt.Sprintf("Verifying that the Pipeline(s) (%v) are deleted without pausing", pipelineIndices), func() bool {

		// allow that the Pipeline could be deleted, but otherwise should not be pausing
		for _, pipelineIndex := range pipelineIndices {
			pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
			_, retrievedPipelineSpec, _, err := GetPipelineSpecAndStatus(Namespace, pipelineName)
			if err != nil {
				if errors.IsNotFound(err) {
					continue // Pipeline was deleted - that's fine
				}
				return false
			}

			if retrievedPipelineSpec.Lifecycle.DesiredPhase == numaflowv1.PipelinePhasePaused {
				return false
			}
		}
		return true
	}).WithTimeout(time.Minute * 1)

	for _, pipelineIndex := range pipelineIndices {
		pipelineName := GetInstanceName(pipelineRolloutName, pipelineIndex)
		VerifyPipelineDeletion(pipelineName)
	}
}
