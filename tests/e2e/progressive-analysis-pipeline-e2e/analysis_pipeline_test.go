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
	argov1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"reflect"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"

	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	pipelineRolloutName     = "test-pipeline-rollout"
	isbServiceRolloutName   = "test-isbservice-rollout"
	initialJetstreamVersion = "2.10.17"
	analysisTemplateName    = "test-pipeline-template"
)

var (
	defaultStrategy = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "120,30,10",
			},
			Analysis: apiv1.Analysis{
				Templates: []argov1alpha1.AnalysisTemplateRef{
					{
						TemplateName: analysisTemplateName,
						ClusterScope: false,
					},
				},
			},
		},
	}

	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	sourceVertexScaleMin = int32(5)
	sourceVertexScaleMax = int32(9)
	numVertices          = int32(1)
	zeroReplicaSleepSec  = uint32(15)
	initialPipelineSpec  = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "cat",
				UDF: &numaflowv1.UDF{
					Builtin: &numaflowv1.Function{
						Name: "cat",
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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
	updatedPipelineSpec = numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices: []numaflowv1.AbstractVertex{
			{
				Name: "in",
				Source: &numaflowv1.Source{
					Generator: &numaflowv1.GeneratorSource{
						RPU:      &pipelineSpecSourceRPU,
						Duration: &pipelineSpecSourceDuration,
					},
				},
				Scale: numaflowv1.Scale{Min: &sourceVertexScaleMin, Max: &sourceVertexScaleMax, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
				Scale: numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}

	volSize, _            = apiresource.ParseQuantity("10Mi")
	initialISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}

	initialAnalysisTemplateSpec = argov1alpha1.AnalysisTemplateSpec{
		Metrics: []argov1alpha1.Metric{
			{
				Name:         "pipeline-example",
				FailureLimit: ptr.To(intstrutil.FromInt32(10)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query:   "increase(pipeline_ack_total{namespace=\"{{args.pipeline-namespace}}\", pipeline_name=\"{{args.upgrading-pipeline-name}}\", pipeline_replica=\"0\"}[1m])",
					},
				},
				SuccessCondition: "len(result) == 0",
			},
		},
		Args: []argov1alpha1.Argument{
			{Name: "upgrading-pipeline-name"},
			{Name: "promoted-pipeline-name"},
			{Name: "pipeline-namespace"},
			{Name: "prometheus-port", Value: ptr.To("9090")},
		},
	}
)

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive Pipeline E2E Suite")
}

var _ = Describe("Progressive Pipeline and ISBService E2E", Serial, func() {
	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
	})

	It("Should validate Pipeline and ISBService upgrade using Progressive strategy", func() {
		CreateAnalysisTemplate(analysisTemplateName, Namespace, initialAnalysisTemplateSpec)
		createPipelineRollout(GetInstanceName(isbServiceRolloutName, 0))

		By("Updating the Pipeline Topology to cause a Progressive change - Successful case")
		updatePipeline(updatedPipelineSpec)

		verifyPipelineSuccess(GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), false, updatedPipelineSpec)
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 0))

		DeletePipelineRollout(pipelineRolloutName)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})

func createPipelineRollout(currentPromotedISBService string) {
	By("Creating a PipelineRollout")
	CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, &defaultStrategy)

	By("Verifying that the Pipeline spec is as expected")
	originalPipelineSpecISBSvcName := initialPipelineSpec.InterStepBufferServiceName
	initialPipelineSpec.InterStepBufferServiceName = currentPromotedISBService
	VerifyPromotedPipelineSpec(Namespace, pipelineRolloutName, func(retrievedPipelineSpec numaflowv1.PipelineSpec) bool {
		return reflect.DeepEqual(retrievedPipelineSpec, initialPipelineSpec)
	})
	initialPipelineSpec.InterStepBufferServiceName = originalPipelineSpecISBSvcName
	VerifyPipelineRolloutInProgressStrategy(pipelineRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyPipelineRolloutHealthy(pipelineRolloutName)
}

func verifyPipelineSuccess(promotedPipelineName string, upgradingPipelineName string, forcedSuccess bool, upgradingPipelineSpec numaflowv1.PipelineSpec) {
	if !forcedSuccess {
		VerifyPromotedPipelineScaledDownForProgressive(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0))
	}
	VerifyPipelineRolloutProgressiveStatus(pipelineRolloutName, promotedPipelineName, upgradingPipelineName, true, apiv1.AssessmentResultSuccess, forcedSuccess)

	newPipelineSpecVertices := []numaflowv1.AbstractVertex{}
	for _, vertex := range upgradingPipelineSpec.Vertices {
		newPipelineSpecVertices = append(newPipelineSpecVertices, numaflowv1.AbstractVertex{Name: vertex.Name, Scale: vertex.Scale})
	}
	VerifyVerticesPodsRunning(Namespace, upgradingPipelineName, newPipelineSpecVertices, ComponentVertex)

	// Verify the previously promoted pipeline was deleted
	VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 0))
}

func updatePipeline(spec numaflowv1.PipelineSpec) {
	rawSpec, err := json.Marshal(spec)
	Expect(err).ShouldNot(HaveOccurred())
	UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(pipelineRollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
		pipelineRollout.Spec.Pipeline.Spec.Raw = rawSpec
		return pipelineRollout, nil
	})
}
