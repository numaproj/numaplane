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
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	argov1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	pipelineRolloutName         = "test-pipeline-rollout"
	isbServiceRolloutName       = "test-isbservice-rollout"
	initialJetstreamVersion     = "2.10.17"
	analysisTemplateNameSuccess = "test-pipeline-template-success"
	analysisTemplateNameFailure = "test-pipeline-template-failure"
	analysisRunName             = "pipeline-" + pipelineRolloutName
	// Aligns metric interval, initialDelay, and PromQL increase() lookback.
	analysisMetricInterval = "60s"
	pipelineSinkBadImage   = "quay.io/numaio/numaflow-go/sink-log-failure:stable"
)

// numaflowControllerVersionForTest returns the Numaflow controller version to deploy.
// Override via shell env (not Makefile): NUMAFLOW_CONTROLLER_VERSION=1.7.5 make test-progressive-analysis-pipeline-e2e
func numaflowControllerVersionForTest() string {
	if v := os.Getenv("NUMAFLOW_CONTROLLER_VERSION"); v != "" {
		return v
	}
	return PrimaryNumaflowControllerVersion
}

var (
	pullPolicyAlways              = corev1.PullAlways
	defaultStrategyForSuccessCase = apiv1.PipelineStrategy{
		PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					AssessmentSchedule: "10,180,30,10",
				},
				Analysis: apiv1.Analysis{
					Templates: []argov1alpha1.AnalysisTemplateRef{
						{
							TemplateName: analysisTemplateNameSuccess,
							ClusterScope: false,
						},
					},
				},
			},
		},
	}

	defaultStrategyForFailureCase = apiv1.PipelineStrategy{
		PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					AssessmentSchedule: "10,180,30,10",
				},
				Analysis: apiv1.Analysis{
					Templates: []argov1alpha1.AnalysisTemplateRef{
						{
							TemplateName: analysisTemplateNameFailure,
							ClusterScope: false,
						},
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
					Container: &numaflowv1.Container{
						Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
						ImagePullPolicy: &pullPolicyAlways,
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
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}

	analysisTemplateArgs = []argov1alpha1.Argument{
		{Name: "upgrading-pipeline-name"},
		{Name: "promoted-pipeline-name"},
		{Name: "pipeline-namespace"},
		{Name: "prometheus-port", Value: ptr.To("9090")},
		{Name: "interval", Value: ptr.To(analysisMetricInterval)},
	}
)

func pipelineAnalysisTemplateSpec() argov1alpha1.AnalysisTemplateSpec {
	return argov1alpha1.AnalysisTemplateSpec{
		Metrics: []argov1alpha1.Metric{
			{
				Name:                    "no-critical-errors",
				FailureLimit:            ptr.To(intstrutil.FromInt32(1)),
				Interval:                argov1alpha1.DurationString(analysisMetricInterval),
				InitialDelay:            argov1alpha1.DurationString(analysisMetricInterval),
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query: `
(
  sum(increase(forwarder_critical_error_total{namespace="{{args.pipeline-namespace}}", pipeline="{{args.upgrading-pipeline-name}}"}[{{args.interval}}])) == 0
)
OR
(
  absent(forwarder_critical_error_total{namespace="{{args.pipeline-namespace}}", pipeline="{{args.upgrading-pipeline-name}}"})
)`,
					},
				},
				SuccessCondition: "len(result) > 0 && result[0] > 0",
			},
			{
				Name:                    "acknowledged-messages",
				FailureLimit:            ptr.To(intstrutil.FromInt32(3)),
				Interval:                argov1alpha1.DurationString(analysisMetricInterval),
				ConsecutiveSuccessLimit: ptr.To(intstrutil.FromInt32(3)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query: `absent(
(
  sum by (vertex) (forwarder_read_total{namespace="{{args.pipeline-namespace}}", pipeline="{{args.upgrading-pipeline-name}}"}) > 0
)
unless
(
  sum by (vertex) (forwarder_ack_total{namespace="{{args.pipeline-namespace}}", pipeline="{{args.upgrading-pipeline-name}}"}) > 0
)
)`,
					},
				},
				SuccessCondition: "len(result) > 0 && result[0] > 0",
			},
		},
		Args: analysisTemplateArgs,
	}
}

// pipelineSpecWithFailingSink copies spec and points every sink at an image that reads without acking.
// sink-log-failure does not increment forwarder_critical_error_total; it only breaks ack.
func pipelineSpecWithFailingSink(spec numaflowv1.PipelineSpec) numaflowv1.PipelineSpec {
	copied := spec.DeepCopy()
	for i := range copied.Vertices {
		if copied.Vertices[i].Sink == nil {
			continue
		}
		copied.Vertices[i].Sink.Log = nil
		copied.Vertices[i].Sink.UDSink = &numaflowv1.UDSink{
			Container: &numaflowv1.Container{
				Image:           pipelineSinkBadImage,
				ImagePullPolicy: &pullPolicyAlways,
			},
		}
	}
	return *copied
}

func vertexScale() numaflowv1.Scale {
	return numaflowv1.Scale{Min: &numVertices, Max: &numVertices, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec}
}

func generatorVertex(name string, rpu int64, duration time.Duration) numaflowv1.AbstractVertex {
	rpuCopy := rpu
	dur := metav1.Duration{Duration: duration}
	return numaflowv1.AbstractVertex{
		Name: name,
		Source: &numaflowv1.Source{
			Generator: &numaflowv1.GeneratorSource{RPU: &rpuCopy, Duration: &dur},
		},
		Scale: vertexScale(),
	}
}

func catVertex(name string) numaflowv1.AbstractVertex {
	return numaflowv1.AbstractVertex{
		Name: name,
		UDF: &numaflowv1.UDF{
			Container: &numaflowv1.Container{
				Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
				ImagePullPolicy: &pullPolicyAlways,
			},
		},
		Scale: vertexScale(),
	}
}

func logSinkVertex(name string) numaflowv1.AbstractVertex {
	return numaflowv1.AbstractVertex{
		Name:  name,
		Sink:  &numaflowv1.Sink{AbstractSink: numaflowv1.AbstractSink{Log: &numaflowv1.Log{}}},
		Scale: vertexScale(),
	}
}

func failingSinkVertex(name string) numaflowv1.AbstractVertex {
	return numaflowv1.AbstractVertex{
		Name: name,
		Sink: &numaflowv1.Sink{AbstractSink: numaflowv1.AbstractSink{
			UDSink: &numaflowv1.UDSink{Container: &numaflowv1.Container{
				Image:           pipelineSinkBadImage,
				ImagePullPolicy: &pullPolicyAlways,
			}},
		}},
		Scale: vertexScale(),
	}
}

func linearPipeline(vertices ...numaflowv1.AbstractVertex) numaflowv1.PipelineSpec {
	edges := make([]numaflowv1.Edge, 0, len(vertices)-1)
	for i := 0; i < len(vertices)-1; i++ {
		edges = append(edges, numaflowv1.Edge{From: vertices[i].Name, To: vertices[i+1].Name})
	}
	return numaflowv1.PipelineSpec{
		InterStepBufferServiceName: isbServiceRolloutName,
		Vertices:                   vertices,
		Edges:                      edges,
	}
}

func strategyForTemplate(templateName string) apiv1.PipelineStrategy {
	return apiv1.PipelineStrategy{
		PipelineTypeRolloutStrategy: apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{AssessmentSchedule: "10,180,30,10"},
				Analysis: apiv1.Analysis{
					Templates: []argov1alpha1.AnalysisTemplateRef{{
						TemplateName: templateName,
						ClusterScope: false,
					}},
				},
			},
		},
	}
}

func pipelineAnalysisTemplateSpecUnreachable() argov1alpha1.AnalysisTemplateSpec {
	spec := pipelineAnalysisTemplateSpec()
	for i := range spec.Metrics {
		if spec.Metrics[i].Provider.Prometheus != nil {
			spec.Metrics[i].Provider.Prometheus.Address = "http://prometheus-unreachable.prometheus.svc.cluster.local:9090"
		}
	}
	return spec
}

// forwarderReadTotal queries Prometheus for the upgrading pipeline's read counter.
// An empty result means the series was never created.
func forwarderReadTotal(pipelineName string) (float64, bool) {
	cmd := exec.Command("kubectl", "port-forward", "-n", "prometheus", "svc/prometheus-kube-prometheus-prometheus", "19090:9090")
	cmd.Stdout = GinkgoWriter
	cmd.Stderr = GinkgoWriter
	Expect(cmd.Start()).To(Succeed())
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}()

	query := fmt.Sprintf(`sum(forwarder_read_total{namespace="%s",pipeline="%s"})`, Namespace, pipelineName)
	var parsed struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	Eventually(func() error {
		resp, err := http.Get("http://127.0.0.1:19090/api/v1/query?query=" + url.QueryEscape(query))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return json.Unmarshal(body, &parsed)
	}, 30*time.Second, time.Second).Should(Succeed())
	Expect(parsed.Status).To(Equal("success"))
	if len(parsed.Data.Result) == 0 {
		return 0, false
	}
	raw, _ := parsed.Data.Result[0].Value[1].(string)
	var value float64
	_, err := fmt.Sscan(raw, &value)
	Expect(err).NotTo(HaveOccurred())
	return value, true
}

// crashUpgradingUDF kills the udf sidecar container and leaves the numa container running
// so forwarder_critical_error_total (reason=map_runtime_error) stays scrapable.
// kubectl debug cannot signal that sidecar on this runtime; docker kill reaches it directly.
func crashUpgradingUDF(pipelineName, vertexName string) {
	selector := fmt.Sprintf("app.kubernetes.io/name=%s-%s,app.kubernetes.io/component=vertex", pipelineName, vertexName)
	out, err := exec.Command("kubectl", "get", "pods", "-n", Namespace, "-l", selector, "-o", "jsonpath={.items[0].metadata.name}").CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(out))
	pod := strings.TrimSpace(string(out))
	Expect(pod).NotTo(BeEmpty())

	By("Killing the udf container on " + pod)
	filter := fmt.Sprintf("name=k8s_udf_%s_%s_", pod, Namespace)
	idOut, idErr := exec.Command("docker", "ps", "-q", "--filter", filter).CombinedOutput()
	Expect(idErr).NotTo(HaveOccurred(), string(idOut))
	id := strings.TrimSpace(string(idOut))
	Expect(id).NotTo(BeEmpty(), "no udf container matched %s", filter)
	if i := strings.IndexByte(id, '\n'); i >= 0 {
		id = id[:i]
	}
	killOut, killErr := exec.Command("docker", "kill", id).CombinedOutput()
	Expect(killErr).NotTo(HaveOccurred(), string(killOut))
}

func TestProgressiveE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Progressive Pipeline E2E Suite")
}

var _ = Describe("Progressive Pipeline and ISBService E2E", Serial, func() {
	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(numaflowControllerVersionForTest())
		CreateISBServiceRollout(isbServiceRolloutName, initialISBServiceSpec)
	})

	It("Should validate Pipeline and ISBService upgrade using Progressive strategy - Successful analysis", func() {
		CreateAnalysisTemplate(analysisTemplateNameSuccess, Namespace, pipelineAnalysisTemplateSpec())
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initialPipelineSpec, defaultStrategyForSuccessCase, apiv1.Metadata{})

		By("Updating the Pipeline Topology to cause a Progressive change - Successful case")
		UpdatePipeline(pipelineRolloutName, updatedPipelineSpec)

		VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), false, updatedPipelineSpec)
		VerifyAnalysisRunStatus("no-critical-errors", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("acknowledged-messages", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyPipelineDeletion(GetInstanceName(pipelineRolloutName, 0))

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameSuccess)
	})

	It("Should validate Pipeline upgrade using Progressive strategy - failure analysis", func() {
		CreateAnalysisTemplate(analysisTemplateNameFailure, Namespace, pipelineAnalysisTemplateSpec())
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initialPipelineSpec, defaultStrategyForFailureCase, apiv1.Metadata{})

		// Upgrading sink reads without acking. Critical-error stays healthy; acknowledged-messages fails the run.
		updatedFailureSpec := pipelineSpecWithFailingSink(updatedPipelineSpec)

		By("Updating the Pipeline Topology to cause a Progressive change")
		UpdatePipeline(pipelineRolloutName, updatedFailureSpec)

		VerifyPipelineProgressiveFailure(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), initialPipelineSpec, updatedFailureSpec)
		VerifyAnalysisRunStatus("acknowledged-messages", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(analysisTemplateNameFailure)
	})

	It("passes analysis for a pipeline that never reads", func() {
		const templateName = "test-pipeline-template-idle"
		CreateAnalysisTemplate(templateName, Namespace, pipelineAnalysisTemplateSpec())
		initial := linearPipeline(generatorVertex("idle-in", 0, time.Second), logSinkVertex("idle-out"))
		updated := linearPipeline(generatorVertex("idle-in", 0, 2*time.Second), logSinkVertex("idle-out"))
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initial, strategyForTemplate(templateName), apiv1.Metadata{})

		By("Updating generator duration to cause a Progressive change with no messages")
		UpdatePipeline(pipelineRolloutName, updated)

		VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), false, updated)
		VerifyAnalysisRunStatus("no-critical-errors", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("acknowledged-messages", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		reads, present := forwarderReadTotal(GetInstanceName(pipelineRolloutName, 1))
		if present {
			Expect(reads).To(Equal(float64(0)))
		}

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(templateName)
	})

	It("passes analysis when vertex names are not in, cat, and out", func() {
		const templateName = "test-pipeline-template-renamed"
		CreateAnalysisTemplate(templateName, Namespace, pipelineAnalysisTemplateSpec())
		initial := linearPipeline(generatorVertex("alpha", 5, time.Second), catVertex("beta"), logSinkVertex("gamma"))
		updated := linearPipeline(generatorVertex("alpha", 5, time.Second), logSinkVertex("gamma"))
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initial, strategyForTemplate(templateName), apiv1.Metadata{})

		By("Removing the beta vertex to cause a Progressive change")
		UpdatePipeline(pipelineRolloutName, updated)

		VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), false, updated)
		VerifyAnalysisRunStatus("no-critical-errors", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)
		VerifyAnalysisRunStatus("acknowledged-messages", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseSuccessful)

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(templateName)
	})

	It("fails analysis when two sinks read without acking", func() {
		const templateName = "test-pipeline-template-two-sinks"
		CreateAnalysisTemplate(templateName, Namespace, pipelineAnalysisTemplateSpec())
		initial := numaflowv1.PipelineSpec{
			InterStepBufferServiceName: isbServiceRolloutName,
			Vertices: []numaflowv1.AbstractVertex{
				generatorVertex("origin", 5, time.Second),
				logSinkVertex("left"),
				logSinkVertex("right"),
			},
			Edges: []numaflowv1.Edge{{From: "origin", To: "left"}, {From: "origin", To: "right"}},
		}
		updated := numaflowv1.PipelineSpec{
			InterStepBufferServiceName: isbServiceRolloutName,
			Vertices: []numaflowv1.AbstractVertex{
				generatorVertex("origin", 5, time.Second),
				failingSinkVertex("left"),
				failingSinkVertex("right"),
			},
			Edges: []numaflowv1.Edge{{From: "origin", To: "left"}, {From: "origin", To: "right"}},
		}
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initial, strategyForTemplate(templateName), apiv1.Metadata{})

		By("Pointing both sinks at sink-log-failure")
		UpdatePipeline(pipelineRolloutName, updated)

		VerifyPipelineProgressiveFailure(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), initial, updated)
		VerifyAnalysisRunStatus("acknowledged-messages", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(templateName)
	})

	It("records an analysis error when Prometheus cannot be reached", func() {
		const templateName = "test-pipeline-template-prom-down"
		CreateAnalysisTemplate(templateName, Namespace, pipelineAnalysisTemplateSpecUnreachable())
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initialPipelineSpec, strategyForTemplate(templateName), apiv1.Metadata{})

		By("Updating the Pipeline Topology to cause a Progressive change")
		UpdatePipeline(pipelineRolloutName, updatedPipelineSpec)

		// Numaplane treats AnalysisPhaseError as success: a dead provider is not a failed rollout.
		VerifyPipelineProgressiveSuccess(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), GetInstanceName(pipelineRolloutName, 1), false, updatedPipelineSpec)
		VerifyAnalysisRunStatus("acknowledged-messages", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseError)

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(templateName)
	})

	It("fails analysis when a vertex udf crash increments critical errors", func() {
		if os.Getenv("TEST_CRITICAL_ERROR") != "true" {
			Skip("set TEST_CRITICAL_ERROR=true to crash a udf and require forwarder_critical_error_total to fail the rollout")
		}
		const templateName = "test-pipeline-template-critical"
		CreateAnalysisTemplate(templateName, Namespace, pipelineAnalysisTemplateSpec())
		initial := linearPipeline(generatorVertex("in", 5, time.Second), catVertex("cat"), logSinkVertex("out"))
		updated := linearPipeline(generatorVertex("in", 5, 2*time.Second), catVertex("cat"), logSinkVertex("out"))
		CreateInitialPipelineRollout(pipelineRolloutName, GetInstanceName(isbServiceRolloutName, 0), initial, strategyForTemplate(templateName), apiv1.Metadata{})

		By("Updating generator duration to cause a Progressive change")
		UpdatePipeline(pipelineRolloutName, updated)

		upgrading := GetInstanceName(pipelineRolloutName, 1)
		CheckEventually("waiting for AnalysisRun to start", func() bool {
			status := GetPipelineRolloutProgressiveStatus(pipelineRolloutName)
			return status.UpgradingPipelineStatus != nil &&
				status.UpgradingPipelineStatus.Analysis.AnalysisRunName == GetInstanceName(analysisRunName, 1)
		}).Should(BeTrue())

		crashUpgradingUDF(upgrading, "cat")

		VerifyPipelineProgressiveFailure(pipelineRolloutName, GetInstanceName(pipelineRolloutName, 0), upgrading, initial, updated)
		VerifyAnalysisRunStatus("no-critical-errors", GetInstanceName(analysisRunName, 1), argov1alpha1.AnalysisPhaseFailed)

		DeletePipelineRollout(pipelineRolloutName)
		DeleteAnalysisTemplate(templateName)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteISBServiceRollout(isbServiceRolloutName)
		DeleteNumaflowControllerRollout()
	})
})
