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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

const (
	monoVertexRolloutName = "test-monovertex-rollout"
)

var (
	monoVertexScaleMin  = int32(3)
	monoVertexScaleMax  = int32(5)
	zeroReplicaSleepSec = uint32(15)

	monoVertexScaleTo               = int64(2)
	monoVertexScaleMinMaxJSONString = fmt.Sprintf("{\"disabled\":null,\"max\":%d,\"min\":%d}", monoVertexScaleMax, monoVertexScaleMin)

	hpaGVR = schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}

	defaultStrategy = apiv1.PipelineTypeRolloutStrategy{
		PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
			Progressive: apiv1.ProgressiveStrategy{
				AssessmentSchedule: "10,180,30,10",
			},
		},
	}

	udTransformer             = numaflowv1.UDTransformer{Container: &numaflowv1.Container{}}
	validUDTransformerImage   = "quay.io/numaio/numaflow-rs/source-transformer-now:stable"
	invalidUDTransformerImage = "quay.io/numaio/numaflow-rs/source-transformer-now:invalid-e8y78rwq5h"

	initialMonoVertexSpec = numaflowv1.MonoVertexSpec{
		Scale: numaflowv1.Scale{Min: &monoVertexScaleMin, Max: &monoVertexScaleMax, Disabled: true, ZeroReplicaSleepSeconds: &zeroReplicaSleepSec},
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

	defaultHPA = autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "autoscaling/v2",
			Kind:       "HorizontalPodAutoscaler",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "hpa",
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: ptr.To(int32(1)),
			MaxReplicas: 10,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ObjectMetricSourceType,
					Object: &autoscalingv2.ObjectMetricSource{
						Metric: autoscalingv2.MetricIdentifier{
							Name: "namespace_app_monovertex_container_cpu_utilization",
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"container": "udsource",
								},
							},
						},
						Target: autoscalingv2.MetricTarget{
							Type:  autoscalingv2.ValueMetricType,
							Value: ptr.To(resource.MustParse("80")),
						},
						DescribedObject: autoscalingv2.CrossVersionObjectReference{
							APIVersion: "apps/v1",
							Kind:       "Deployment",
							Name:       "{{.monovertex-name}}",
						},
					},
				},
			},
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "numaflow.numaproj.io/v1alpha1",
				Kind:       "MonoVertex",
				Name:       "{{.monovertex-name}}",
			},
		},
	}
)

func TestHPAE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "HPA MonoVertex E2E Suite")
}

var _ = Describe("HPA MonoVertex E2E", Serial, func() {
	It("Should create initial rollout objects", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
	})

	It("Should create MonoVertexRollout with HPA rider", func() {
		// Create the MonoVertexRollout first
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, initialMonoVertexSpec, &defaultStrategy, apiv1.Metadata{})

		// Add HPA Rider to the MonoVertexRollout
		rawHPASpec, err := json.Marshal(defaultHPA)
		Expect(err).ShouldNot(HaveOccurred())

		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders = []apiv1.Rider{
				{
					Progressive: false,
					Definition:  runtime.RawExtension{Raw: rawHPASpec},
				},
			}
			return rollout, nil
		})

		// Verify the HPA is created for the MonoVertex
		monoVertexName := fmt.Sprintf("%s-0", monoVertexRolloutName)
		hpaName := fmt.Sprintf("hpa-%s", monoVertexName)
		VerifyResourceExists(hpaGVR, hpaName)
		VerifyResourceFieldMatchesRegex(hpaGVR, hpaName, "spec.scaleTargetRef.name", monoVertexName)
	})

	It("Should perform a Progressive Upgrade which fails", func() {
		// Update the MonoVertexRollout to use an invalid UDTransformer image
		UpdateMonoVertexRolloutForFailure(monoVertexRolloutName, invalidUDTransformerImage, initialMonoVertexSpec, udTransformer)

		// Verify there is no HPA running for either monovertex and scale is fixed/disabled=false during assessment
		VerifyPromotedMonoVertex()

		// Verify the Progressive Upgrade fails
		VerifyMonoVertexProgressiveFailure(monoVertexRolloutName, monoVertexScaleMinMaxJSONString, updatedMonoVertexSpec, monoVertexScaleTo, false)
	})
})
