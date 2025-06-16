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

	intstrutil "k8s.io/apimachinery/pkg/util/intstr"

	argov1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"

	"k8s.io/utils/ptr"
)

const (
	monoVertexRolloutName = "test-monovertex-analysis-rollout"
	analysisTemplateName  = "test-monovertex-template"
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
				Name:         "mvtx-example",
				FailureLimit: ptr.To(intstrutil.FromInt32(10)),
				Provider: argov1alpha1.MetricProvider{
					Prometheus: &argov1alpha1.PrometheusMetric{
						Address: "http://prometheus-kube-prometheus-prometheus.prometheus.svc.cluster.local:{{args.prometheus-port}}",
						Query:   "increase(monovtx_ack_total{namespace=\"{{args.monovertex-namespace}}\", mvtx_name=\"{{args.upgrading-monovertex-name}}\", mvtx_replica=\"0\"}[1m])",
					},
				},
				SuccessCondition: "len(result) == 0",
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

	It("Should validate MonoVertex upgrade using Analysis template for Progressive strategy", func() {
		CreateAnalysisTemplate(analysisTemplateName, Namespace, initialAnalysisTemplateSpec)
		createInitialMonoVertexRollout(&defaultStrategy)

		updatedMonoVertexSpec := updateMonoVertexRolloutForSuccess()
		verifyProgressiveSuccess(updatedMonoVertexSpec, 0, 1, false, true)

		// Verify the previously promoted monovertex was deleted
		VerifyMonoVertexDeletion(GetInstanceName(monoVertexRolloutName, 0))

		VerifyAnalysisRunStatus(GetInstanceName(monoVertexRolloutName, 1), argov1alpha1.AnalysisPhaseSuccessful)

		DeleteMonoVertexRollout(monoVertexRolloutName)
		DeleteAnalysisTemplate(analysisTemplateName)
	})

	It("Should delete all remaining rollout objects", func() {
		DeleteNumaflowControllerRollout()
	})
})

func updateMonoVertexRolloutForSuccess() *numaflowv1.MonoVertexSpec {
	By("Updating the MonoVertex Topology to cause a Progressive change")
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

func createInitialMonoVertexRollout(strategy *apiv1.PipelineTypeRolloutStrategy) {
	By("Creating a MonoVertexRollout")
	CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, strategy)

	By("Verifying that the MonoVertex spec is as expected")
	VerifyPromotedMonoVertexSpec(Namespace, monoVertexRolloutName, func(retrievedMonoVertexSpec numaflowv1.MonoVertexSpec) bool {
		return reflect.DeepEqual(retrievedMonoVertexSpec, initialMonoVertexSpec)
	})
	VerifyMonoVertexRolloutInProgressStrategy(monoVertexRolloutName, apiv1.UpgradeStrategyNoOp)
	VerifyMonoVertexRolloutHealthy(monoVertexRolloutName)
}

func verifyProgressiveSuccess(updatedMonoVertexSpec *numaflowv1.MonoVertexSpec, promotedMonoVertexIndex int, updatedMonoVertexIndex int, forcedSuccess bool, checkRunningVertices bool) {
	promotedMonoVertexName := GetInstanceName(monoVertexRolloutName, promotedMonoVertexIndex)
	upgradingMonoVertexName := GetInstanceName(monoVertexRolloutName, updatedMonoVertexIndex)
	if !forcedSuccess {
		VerifyMonoVertexRolloutScaledDownForProgressive(monoVertexRolloutName, promotedMonoVertexName, monoVertexScaleMinMaxJSONString, monoVertexScaleTo)
	}
	VerifyMonoVertexRolloutProgressiveStatus(monoVertexRolloutName, promotedMonoVertexName, upgradingMonoVertexName, false, apiv1.AssessmentResultSuccess, forcedSuccess)

	By(fmt.Sprintf("Verifying MonoVertex %s has become the Promoted one", upgradingMonoVertexName))
	VerifyPromotedMonoVertexName(Namespace, monoVertexRolloutName, upgradingMonoVertexName)

	VerifyMonoVertexPromotedScale(Namespace, monoVertexRolloutName, map[string]numaflowv1.Scale{
		upgradingMonoVertexName: updatedMonoVertexSpec.Scale,
	})

	if checkRunningVertices {
		VerifyVerticesPodsRunning(Namespace, upgradingMonoVertexName,
			[]numaflowv1.AbstractVertex{{Scale: updatedMonoVertexSpec.Scale}}, ComponentMonoVertex)
	}
}
