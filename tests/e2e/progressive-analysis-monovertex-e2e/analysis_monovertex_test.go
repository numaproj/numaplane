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
	"os/exec"
	"strings"
	"testing"
	"time"

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
	// Aligns metric interval, initialDelay, and PromQL range lookback.
	analysisMetricInterval = "60s"
)

// numaflowVersionForCriticalErrorAnalysis is the oldest Numaflow release whose metrics server
// stays up when the forwarder fails. On 1.8.3 the process exits in the same moment
// monovtx_critical_error_total is incremented, so Prometheus never scrapes it and absent()
// passes. 1.9 restarts the forwarder in-process and the counter keeps rising.
// NUMAFLOW_CONTROLLER_VERSION is not read here; CI never exercised that override.
const numaflowVersionForCriticalErrorAnalysis = "1.9.0"

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
)

func monoVertexAnalysisTemplateSpec(criticalErrorsMetricName, acknowledgedMessagesMetricName string) argov1alpha1.AnalysisTemplateSpec {
	return argov1alpha1.AnalysisTemplateSpec{
		Metrics: []argov1alpha1.Metric{
			{
				Name:                    criticalErrorsMetricName,
				FailureLimit:            ptr.To(intstrutil.FromInt32(0)),
				Interval:                argov1alpha1.DurationString(analysisMetricInterval),
				InitialDelay:            argov1alpha1.DurationString(analysisMetricInterval),
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query: `
(
  sum(increase(monovtx_critical_error_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"}[{{args.interval}}])) == bool 0
)
OR
(
  absent(monovtx_critical_error_total{namespace="{{args.monovertex-namespace}}", mvtx_name="{{args.upgrading-monovertex-name}}"})
)`,
					},
				},
				SuccessCondition: "len(result) > 0 && result[0] > 0",
			},
			{
				Name:                    acknowledgedMessagesMetricName,
				FailureLimit:            ptr.To(intstrutil.FromInt32(3)),
				Interval:                argov1alpha1.DurationString(analysisMetricInterval),
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query: `(vector(1) and on() ((sum(monovtx_read_total{namespace="{{args.monovertex-namespace}}",
mvtx_name="{{args.upgrading-monovertex-name}}"}) or vector(0)) == 0)) or
(vector(1) and on() ((sum(monovtx_ack_total{namespace="{{args.monovertex-namespace}}",
mvtx_name="{{args.upgrading-monovertex-name}}"}) or vector(0)) > 0)) or
vector(0)`,
					},
				},
				SuccessCondition: "len(result) > 0 && result[0] > 0",
			},
		},
		Args: analysisTemplateArgs,
	}
}

// monoVertexCriticalErrorOnlySpec keeps only the critical-error metric.
// The ack metric is omitted so a rollout failure can be attributed to this check.
func monoVertexCriticalErrorOnlySpec(metricName string) argov1alpha1.AnalysisTemplateSpec {
	spec := monoVertexAnalysisTemplateSpec(metricName, "unused-ack")
	spec.Metrics = spec.Metrics[:1]
	return spec
}

// startTerminatingUpgradingTransformer kills the upgrading source transformer every 20s in the
// background until the returned stop func is called, which returns the number of successful kills.
// monovtx_critical_error_total is created at 1 on the first error, so a single kill leaves
// increase() at 0; the counter must keep rising across Prometheus scrapes (30s).
// It runs in the background because the failure checks must observe the promoted MonoVertex
// while it is still scaled down. Kills fail while the container is in restart backoff.
func startTerminatingUpgradingTransformer(monoVertexName string) func() int {
	const killInterval = 20 * time.Second
	selector := fmt.Sprintf("numaflow.numaproj.io/mono-vertex-name=%s,app.kubernetes.io/component=mono-vertex", monoVertexName)
	stopCh := make(chan struct{})
	killsCh := make(chan int)

	By("Repeatedly terminating the upgrading source transformer while its AnalysisRun is active")
	go func() {
		kills := 0
		ticker := time.NewTicker(killInterval)
		defer ticker.Stop()
		for {
			output, err := exec.Command("kubectl", "get", "pods", "-n", Namespace, "-l", selector,
				"--field-selector=status.phase=Running", "-o", "jsonpath={.items[0].metadata.name}").Output()
			if podName := strings.TrimSpace(string(output)); err == nil && podName != "" {
				if exec.Command("kubectl", "exec", "-n", Namespace, podName, "-c", "transformer", "--",
					"/bin/sh", "-c", "kill 1").Run() == nil {
					kills++
				}
			}
			select {
			case <-stopCh:
				killsCh <- kills
				return
			case <-ticker.C:
			}
		}
	}()

	return func() int {
		close(stopCh)
		return <-killsCh
	}
}

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive MonoVertex E2E Suite")
}

var _ = Describe("Progressive MonoVertex E2E", Serial, func() {

	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(numaflowVersionForCriticalErrorAnalysis)
	})

	It("Should validate MonoVertex upgrade using Analysis template for Progressive strategy - Success case", func() {
		CreateAnalysisTemplate(analysisTemplateNameSuccessOne, Namespace,
			monoVertexAnalysisTemplateSpec("mvtx-no-critical-errors-1", "mvtx-acknowledged-messages-1"))

		// Use a second template with the same metrics to verify multiple AnalysisTemplates are merged
		CreateAnalysisTemplate(analysisTemplateNameSuccessTwo, Namespace,
			monoVertexAnalysisTemplateSpec("mvtx-no-critical-errors-2", "mvtx-acknowledged-messages-2"))

		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, &defaultStrategyForSuccessCase, apiv1.Metadata{})
		VerifyMonoVertexRolloutHealthy(monoVertexRolloutName)

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, initialMonoVertexSpec, udTransformer)
		VerifyMonoVertexProgressiveSuccess(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo, updatedMonoVertexSpec,
			0, 1, false, true)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		VerifyAnalysisRunStatus("mvtx-no-critical-errors-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("mvtx-acknowledged-messages-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("mvtx-no-critical-errors-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("mvtx-acknowledged-messages-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameSuccessOne)
		DeleteAnalysisTemplate(analysisTemplateNameSuccessTwo)
	})

	It("Should validate MonoVertex upgrade using Analysis template for Progressive strategy - Failure case", func() {
		CreateAnalysisTemplate(analysisTemplateNameFailureOne, Namespace,
			monoVertexAnalysisTemplateSpec("mvtx-no-critical-errors-1", "mvtx-acknowledged-messages-1"))
		CreateAnalysisTemplate(analysisTemplateNameFailureTwo, Namespace,
			monoVertexAnalysisTemplateSpec("mvtx-no-critical-errors-2", "mvtx-acknowledged-messages-2"))

		// Start with a bad sink so reads occur without acks before the upgrade; upgrading keeps the same sink.
		initialWithBadSink := initialMonoVertexSpec.DeepCopy()
		initialWithBadSink.Sink.AbstractSink.Blackhole = nil
		initialWithBadSink.Sink.AbstractSink.UDSink = &numaflowv1.UDSink{Container: &numaflowv1.Container{Image: monovertexSinkBadImage}}
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, *initialWithBadSink, &defaultStrategyForFailureCase, apiv1.Metadata{})
		VerifyMonoVertexRolloutHealthy(monoVertexRolloutName)

		updatedMonoVertexSpec := UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, *initialWithBadSink, udTransformer)
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updatedMonoVertexSpec, monoVertexScaleTo, false)

		// Rollout failure is driven by the legacy acknowledged-messages metric (reads without acks).
		VerifyAnalysisRunStatus("mvtx-acknowledged-messages-1", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)
		VerifyAnalysisRunStatus("mvtx-acknowledged-messages-2", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameFailureOne)
		DeleteAnalysisTemplate(analysisTemplateNameFailureTwo)
	})

	It("fails analysis when the upgrading MonoVertex reports a critical error", func() {
		const templateName = "test-monovertex-template-critical-only"
		CreateAnalysisTemplate(templateName, Namespace, monoVertexCriticalErrorOnlySpec("mvtx-no-critical-errors"))

		strategy := apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					AssessmentSchedule: "10,180,30,10",
				},
				Analysis: apiv1.Analysis{
					Templates: []argov1alpha1.AnalysisTemplateRef{{
						TemplateName: templateName,
						ClusterScope: false,
					}},
				},
			},
		}

		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, &strategy, apiv1.Metadata{})
		VerifyMonoVertexRolloutHealthy(monoVertexRolloutName)

		// Start with a healthy upgrade so Numaplane creates the AnalysisRun before the runtime error.
		updated := UpdateMonoVertexRolloutForSuccess(monoVertexRolloutName, validUDTransformerImage, initialMonoVertexSpec, udTransformer)
		upgradingMonoVertexName := GetInstanceName(monoVertexRolloutName, 1)
		CheckEventually("waiting for the critical-error AnalysisRun to start", func() bool {
			status := GetMonoVertexRolloutProgressiveStatus(monoVertexRolloutName)
			return status.UpgradingMonoVertexStatus != nil &&
				status.UpgradingMonoVertexStatus.Analysis.AnalysisRunName == GetInstanceName(analysisRunName, 1)
		}).Should(BeTrue())

		stopTerminating := startTerminatingUpgradingTransformer(upgradingMonoVertexName)
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updated, monoVertexScaleTo, false)
		Expect(stopTerminating()).Should(BeNumerically(">=", 2))
		VerifyAnalysisRunStatus("mvtx-no-critical-errors", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(templateName)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})
