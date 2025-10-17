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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

func TestMonoVertexRiderE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "MonoVertex Rider E2E Suite")
}

const (
	monoVertexRolloutName = "test-monovertex-rollout"
)

var (
	monoVertexIndex            = 0
	monoVertexSpecWithoutRider numaflowv1.MonoVertexSpec
	monoVertexSpecWithCMRef    numaflowv1.MonoVertexSpec

	defaultConfigMap = corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-configmap",
		},
		Data: map[string]string{
			"my-key": "my-value",
		},
	}
	currentConfigMap = &defaultConfigMap

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
	currentHPA = &defaultHPA
)

func init() {
	monoVertexSpecWithoutRider = numaflowv1.MonoVertexSpec{
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

	monoVertexSpecWithCMRef = *monoVertexSpecWithoutRider.DeepCopy()
	monoVertexSpecWithCMRef.Volumes = []corev1.Volume{
		{
			Name: "volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "my-configmap-{{.monovertex-name}}",
					},
				},
			},
		},
	}
	monoVertexSpecWithCMRef.Source.UDSource.Container.VolumeMounts = []corev1.VolumeMount{
		{
			Name:      "volume",
			MountPath: "/etc/config",
		},
	}

}

var _ = Describe("Rider E2E", Serial, func() {

	It("Should create NumaflowControllerRollout and MonoVertexRollout", func() {
		CreateNumaflowControllerRollout(PrimaryNumaflowControllerVersion)
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, monoVertexSpecWithoutRider, nil)
	})

	It("Should add ConfigMap Rider to MonoVertexRollout", func() {

		// Add ConfigMap Rider and update MonoVertex spec to use it
		rawMVSpec, err := json.Marshal(monoVertexSpecWithCMRef)
		Expect(err).ShouldNot(HaveOccurred())
		rawConfigMapSpec, err := json.Marshal(defaultConfigMap)
		Expect(err).ShouldNot(HaveOccurred())

		// update the MonoVertexRollout
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawMVSpec

			rollout.Spec.Riders = []apiv1.Rider{
				{
					Progressive: false,
					Definition:  runtime.RawExtension{Raw: rawConfigMapSpec},
				},
			}
			return rollout, nil
		})

		// verify ConfigMap is created
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)
	})

	It("Should add HPA Rider to MonoVertexRollout", func() {

		// Add HPA Rider to existing ConfigMap Rider
		rawHPASpec, err := json.Marshal(defaultHPA)
		Expect(err).ShouldNot(HaveOccurred())

		// update the MonoVertexRollout to include both riders
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders = append(rollout.Spec.Riders, apiv1.Rider{
				Progressive: true,
				Definition:  runtime.RawExtension{Raw: rawHPASpec},
			})

			return rollout, nil
		})

		// verify HPA is created for the MonoVertex
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// HPA is named with the monovertex name as the suffix
		hpaName := fmt.Sprintf("hpa-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, hpaName)
	})

	It("Should update the ConfigMap Rider as a Progressive rollout change", func() {
		// Update ConfigMap to add a new key/value pair
		currentConfigMap = currentConfigMap.DeepCopy()
		currentConfigMap.Data["my-key-2"] = "my-value-2"
		rawConfigMapSpec, err := json.Marshal(currentConfigMap)
		Expect(err).ShouldNot(HaveOccurred())

		// Keep HPA unchanged
		rawHPASpec, err := json.Marshal(currentHPA)
		Expect(err).ShouldNot(HaveOccurred())

		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders[0].Definition = runtime.RawExtension{Raw: rawConfigMapSpec}
			rollout.Spec.Riders[1].Definition = runtime.RawExtension{Raw: rawHPASpec}
			return rollout, nil
		})

		monoVertexIndex++

		// Verify that this caused a Progressive upgrade and generated new ConfigMap and HPA
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)

		// Verify that the ConfigMap content was updated to include the new key-value pair
		CheckEventually(fmt.Sprintf("verifying ConfigMap %s has updated content", configMapName), func() bool {
			configMapResource, err := GetResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, Namespace, configMapName)
			if err != nil || configMapResource == nil {
				return false
			}

			// Extract data from the ConfigMap
			data, found, err := unstructured.NestedStringMap(configMapResource.Object, "data")
			if err != nil || !found {
				return false
			}

			// Check that both original and new key-value pairs exist
			return data["my-key"] == "my-value" && data["my-key-2"] == "my-value-2"
		}).WithTimeout(DefaultTestTimeout).Should(BeTrue())

		// HPA is named with the monovertex name as the suffix
		hpaName := fmt.Sprintf("hpa-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, hpaName)

		// Now verify that with the Progressive upgrade, the original MonoVertex,
		// ConfigMap, and HPA get cleaned up
		mvOriginalName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex-1)
		originalConfigMap := fmt.Sprintf("my-configmap-%s", mvOriginalName)
		originalHPA := fmt.Sprintf("hpa-%s", mvOriginalName)
		VerifyResourceDoesntExist(numaflowv1.MonoVertexGroupVersionResource, mvOriginalName)
		VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, originalConfigMap)
		VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, originalHPA)
	})

	It("Should update the HPA Rider in place", func() {

		// Update HPA to change maxReplicas
		currentHPA = currentHPA.DeepCopy()
		currentHPA.Spec.MaxReplicas = 15
		rawHPASpec, err := json.Marshal(currentHPA)
		Expect(err).ShouldNot(HaveOccurred())

		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders[1].Definition = runtime.RawExtension{Raw: rawHPASpec}
			return rollout, nil
		})

		// Verify that this caused an in place update of the HPA
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is still there and named with the same monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)
		// HPA is still there and named with the same monovertex name as the suffix
		hpaName := fmt.Sprintf("hpa-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, hpaName)

		// Verify that the HPA content was updated to reflect the maxReplicas change from 10 to 15
		CheckEventually(fmt.Sprintf("verifying HPA %s has maxReplicas=15", hpaName), func() bool {
			hpaResource, err := GetResource(schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, Namespace, hpaName)
			if err != nil || hpaResource == nil {
				return false
			}

			// Extract maxReplicas from the HPA spec
			spec, found, err := unstructured.NestedMap(hpaResource.Object, "spec")
			if err != nil || !found {
				return false
			}

			maxReplicas, found, err := unstructured.NestedInt64(spec, "maxReplicas")
			if err != nil || !found {
				return false
			}

			return maxReplicas == 15
		}).WithTimeout(DefaultTestTimeout).Should(BeTrue())
	})

	It("Should delete the ConfigMap and HPA Riders", func() {
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders = []apiv1.Rider{}
			return rollout, nil
		})

		// Confirm the ConfigMap and HPA were deleted (but the monovertex is still present)
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		hpaName := fmt.Sprintf("hpa-%s", monoVertexName)
		VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)
		VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, hpaName)
		VerifyResourceExists(numaflowv1.MonoVertexGroupVersionResource, monoVertexName)
	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {
		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete the NumaflowControllerRollout", func() {
		DeleteNumaflowControllerRollout()
	})
})
