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

	argov1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName          = "test-monovertex-analysis-rollout"
	analysisTemplateNameSuccessOne = "test-monovertex-template-success-1"
	analysisTemplateNameSuccessTwo = "test-monovertex-template-success-2"
	analysisTemplateNameFailureOne = "test-monovertex-template-failure-1"
	analysisTemplateNameFailureTwo = "test-monovertex-template-failure-2"
	analysisRunName                = "monovertex-" + monoVertexRolloutName
	// Aligns metric interval, initialDelay, and PromQL increase() lookback.
	analysisMetricInterval = "60s"
)

var (
	monoVertexScaleMin  = int32(4)
	monoVertexScaleMax  = int32(5)
	zeroReplicaSleepSec = uint32(15)

	monoVertexScaleTo               = int64(2)
	monoVertexScaleMinMaxJSONString = fmt.Sprintf("{\"disabled\":null,\"max\":%d,\"min\":%d}", monoVertexScaleMax, monoVertexScaleMin)
	monovertexSinkBadImage          = "quay.io/numaio/numaflow-go/sink-log-failure:stable"

	defaultStrategyForSuccessCase = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "10,180,30,10",
			},
			Analysis: apiv1.Analysis{
				Templates: []argov1alpha1.AnalysisTemplateRef{
					{
						TemplateName: analysisTemplateNameSuccessOne,
						ClusterScope: false,
					},
					{
						TemplateName: analysisTemplateNameSuccessTwo,
						ClusterScope: false,
					},
				},
			},
		},
	}

	defaultStrategyForFailureCase = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "10,180,30,10",
			},
			Analysis: apiv1.Analysis{
				Templates: []argov1alpha1.AnalysisTemplateRef{
					{
						TemplateName: analysisTemplateNameFailureOne,
						ClusterScope: false,
					},
					{
						TemplateName: analysisTemplateNameFailureTwo,
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

	analysisTemplateArgs = []argov1alpha1.Argument{
		{Name: "upgrading-monovertex-name"},
		{Name: "promoted-monovertex-name"},
		{Name: "monovertex-namespace"},
		{Name: "prometheus-port", Value: ptr.To("9090")},
		{Name: "interval", Value: ptr.To(analysisMetricInterval)},
	}

	initialAnalysisTemplateSpec = argov1alpha1.AnalysisTemplateSpec{
		Metrics: []argov1alpha1.Metric{
			{
				Name:                    "mvtx-no-critical-errors-1",
				FailureLimit:            ptr.To(intstrutil.FromInt32(3)),
				Interval:                argov1alpha1.DurationString(analysisMetricInterval),
				InitialDelay:            argov1alpha1.DurationString(analysisMetricInterval),
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query: `
(
  sum(increase(monovtx_critical_error_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"}[{{args.interval}}])) == 0
)
OR
(
  absent(monovtx_critical_error_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"})
)`,
					},
				},
				SuccessCondition: "len(result) > 0 && result[0] > 0",
			},
		},
		Args: analysisTemplateArgs,
	}

	// Strict query without the absent() pass-through: when monovtx_critical_error_total
	// has never been emitted (e.g. sink never acks but also never increments the counter),
	// Prometheus returns no series and the AnalysisRun metric fails.
	failureAnalysisTemplateSpec = argov1alpha1.AnalysisTemplateSpec{
		Metrics: []argov1alpha1.Metric{
			{
				Name:                    "mvtx-no-critical-errors-1",
				FailureLimit:            ptr.To(intstrutil.FromInt32(3)),
				Interval:                argov1alpha1.DurationString(analysisMetricInterval),
				InitialDelay:            argov1alpha1.DurationString(analysisMetricInterval),
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query:   `sum(increase(monovtx_critical_error_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"}[{{args.interval}}])) == 0`,
					},
				},
				SuccessCondition: "len(result) > 0 && result[0] > 0",
			},
		},
		Args: analysisTemplateArgs,
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
		CreateAnalysisTemplate(analysisTemplateNameSuccessOne, Namespace, initialAnalysisTemplateSpec)

		// Use a second template with the same query to verify multiple AnalysisTemplates are merged
		updatedAnalysisTemplateSpec := initialAnalysisTemplateSpec.DeepCopy()
		updatedAnalysisTemplateSpec.Metrics[0].Name = "mvtx-no-critical-errors-2"
		CreateAnalysisTemplate(analysisTemplateNameSuccessTwo, Namespace, *updatedAnalysisTemplateSpec)

		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, &defaultStrategyForSuccessCase, apiv1.Metadata{})

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveSuccess(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo, updatedMonoVertexSpec,
			0, 1, false, true)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		VerifyAnalysisRunStatus("mvtx-no-critical-errors-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("mvtx-no-critical-errors-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameSuccessOne)
		DeleteAnalysisTemplate(analysisTemplateNameSuccessTwo)
	})

	It("Should validate MonoVertex upgrade using Analysis template for Progressive strategy - Failure case", func() {
		CreateAnalysisTemplate(analysisTemplateNameFailureOne, Namespace, failureAnalysisTemplateSpec)
		updatedAnalysisTemplate := failureAnalysisTemplateSpec.DeepCopy()
		updatedAnalysisTemplate.Metrics[0].Name = "mvtx-no-critical-errors-2"
		CreateAnalysisTemplate(analysisTemplateNameFailureTwo, Namespace, *updatedAnalysisTemplate)

		CreateInitialMonoVertexRollout(monoVertexRolloutName, initialMonoVertexSpec, &defaultStrategyForFailureCase, apiv1.Metadata{})

		// Bad sink on the upgrading MonoVertex keeps pods healthy but prevents acks; with the strict
		// query (no absent() branch) the AnalysisRun fails because the critical-error metric never appears.
		updatedMonoVertexSpec := UpdateMonoVertexRolloutForAnalysisFailure(monoVertexRolloutName, initialMonoVertexSpec, udTransformer, validUDTransformerImage, monovertexSinkBadImage)
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updatedMonoVertexSpec, monoVertexScaleTo, false)

		VerifyAnalysisRunStatus("mvtx-no-critical-errors-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)
		VerifyAnalysisRunStatus("mvtx-no-critical-errors-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameFailureOne)
		DeleteAnalysisTemplate(analysisTemplateNameFailureTwo)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
