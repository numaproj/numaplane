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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	intstrutil "k8s.io/apimachinery/pkg/util/intstr"

	argov1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"

	"k8s.io/utils/ptr"
)

const (
	monoVertexRolloutName   = "test-monovertex-analysis-rollout"
	analysisTemplateNameOne = "test-monovertex-template-1"
	analysisTemplateNameTwo = "test-monovertex-template-2"
	analysisRunName         = "monovertex-" + monoVertexRolloutName
)

var (
	monoVertexScaleMin  = int32(4)
	monoVertexScaleMax  = int32(5)
	zeroReplicaSleepSec = uint32(15)

	monoVertexScaleTo               = int64(2)
	monoVertexScaleMinMaxJSONString = fmt.Sprintf("{\"max\":%d,\"min\":%d}", monoVertexScaleMax, monoVertexScaleMin)
	monovertexSinkBadImage          = "quay.io/numaio/numaflow-go/sink-failure:stable"

	defaultStrategy = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "60,30,10",
			},
			Analysis: apiv1.Analysis{
				Templates: []argov1alpha1.AnalysisTemplateRef{
					{
						TemplateName: analysisTemplateNameOne,
						ClusterScope: false,
					},
					{
						TemplateName: analysisTemplateNameTwo,
						ClusterScope: false,
					},
				},
			},
		},
	}

	udTransformer           = numaflowv1.UDTransformer{Container: &numaflowv1.Container{}}
	validUDTransformerImage = "quay.io/numaio/numaflow-rs/source-transformer-now:stable"

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

	initialAnalysisTemplateSpec = argov1alpha1.AnalysisTemplateSpec{
		Metrics: []argov1alpha1.Metric{
			{
				Name:                    "mvtx-example-1",
				FailureLimit:            ptr.To(intstrutil.FromInt32(3)),
				Interval:                "60s",
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query: `
(
  absent(sum(monovtx_read_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"}))
  OR
  sum(monovtx_read_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"}) == 0
)
OR
(
  sum(monovtx_ack_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"}) > 0
)`,
					},
				},
				SuccessCondition: "result[0] > 0",
			},
		},
		Args: []argov1alpha1.Argument{
			{Name: "upgrading-monovertex-name"},
			{Name: "promoted-monovertex-name"},
			{Name: "monovertex-namespace"},
			{Name: "prometheus-port", Value: ptr.To("9090")},
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

	It("Should validate MonoVertex upgrade using Analysis template for Progressive strategy - Success case", func() {
		CreateAnalysisTemplate(analysisTemplateNameOne, Namespace, initialAnalysisTemplateSpec)

		// Update the initial AnalysisTemplateSpec to use a different metric name and success condition
		updatedAnalysisTemplateSpec := initialAnalysisTemplateSpec.DeepCopy()
		updatedAnalysisTemplateSpec.Metrics[0].Name = "mvtx-example-2"
		updatedAnalysisTemplateSpec.Metrics[0].SuccessCondition = "len(result) > 0"
		CreateAnalysisTemplate(analysisTemplateNameTwo, Namespace, *updatedAnalysisTemplateSpec)

		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, &defaultStrategy)

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveSuccess(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo, updatedMonoVertexSpec,
			0, 1, false, true)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		VerifyAnalysisRunStatus("mvtx-example-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("mvtx-example-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameOne)
		DeleteAnalysisTemplate(analysisTemplateNameTwo)
	})

	It("Should validate MonoVertex upgrade using Analysis template for Progressive strategy - Failure case", func() {
		CreateAnalysisTemplate(analysisTemplateNameOne, Namespace, initialAnalysisTemplateSpec)
		// Update a fake query to cause a success status
		updatedAnalysisTemplate := initialAnalysisTemplateSpec.DeepCopy()
		updatedAnalysisTemplate.Metrics[0].Name = "mvtx-example-2"
		updatedAnalysisTemplate.Metrics[0].SuccessCondition = "true"
		updatedAnalysisTemplate.Metrics[0].Provider.Prometheus.Query = "vector(1)"

		CreateAnalysisTemplate(analysisTemplateNameTwo, Namespace, *updatedAnalysisTemplate)

		// Update the initial MonoVertexSpec to use a bad image for the sink
		initialMonoVertexSpec.Sink.AbstractSink.Blackhole = nil
		initialMonoVertexSpec.Sink.AbstractSink.UDSink = &numaflowv1.UDSink{Container: &numaflowv1.Container{Image: monovertexSinkBadImage}}
		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, &defaultStrategy)

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updatedMonoVertexSpec, monoVertexScaleTo, false)

		// Verify the AnalysisRun status is Failed
		VerifyAnalysisRunStatus("mvtx-example-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseError)
		VerifyAnalysisRunStatus("mvtx-example-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameOne)
		DeleteAnalysisTemplate(analysisTemplateNameTwo)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
