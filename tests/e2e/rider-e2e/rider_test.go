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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	autoscalingk8siov1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRiderE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Rider E2E Suite")
}

const (
	monoVertexRolloutName = "test-monovertex-rollout"
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
)

var (
	monoVertexIndex            = 0
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
	monoVertexSpecWithRider numaflowv1.MonoVertexSpec

	defaultConfigMap = v1.ConfigMap{
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

	initialJetstreamVersion = "2.10.17"
	volSize, _              = apiresource.ParseQuantity("10Mi")
	isbServiceSpec          = numaflowv1.InterStepBufferServiceSpec{
		Redis: nil,
		JetStream: &numaflowv1.JetStreamBufferService{
			Version: initialJetstreamVersion,
			Persistence: &numaflowv1.PersistenceStrategy{
				VolumeSize: &volSize,
			},
		},
	}

	pipelineSpecSourceRPU      = int64(5)
	pipelineSpecSourceDuration = metav1.Duration{
		Duration: time.Second,
	}
	pipelineSpecWithoutRider = numaflowv1.PipelineSpec{
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
			},
			{
				Name: "out",
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						Log: &numaflowv1.Log{},
					},
				},
			},
		},
		Edges: []numaflowv1.Edge{
			{
				From: "in",
				To:   "out",
			},
		},
	}

	defaultVertexVPA = autoscalingk8siov1.VerticalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			Kind:       "VerticalPodAutoscaler",
			APIVersion: "autoscaling.k8s.io/v1beta2",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-vertex-vpa",
		},
		Spec: autoscalingk8siov1.VerticalPodAutoscalerSpec{
			TargetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       "Vertex",
				APIVersion: "numaproj.io/v1alpha1",
				Name:       "{{.vertex-name}}",
			},
		},
	}
)

func init() {
	monoVertexSpecWithRider = *monoVertexSpecWithoutRider.DeepCopy()
	monoVertexSpecWithRider.Volumes = []v1.Volume{
		{
			Name: "volume",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: "my-configmap-{{.monovertex-name}}",
					},
				},
			},
		},
	}
	monoVertexSpecWithRider.Source.UDSource.Container.VolumeMounts = []v1.VolumeMount{
		{
			Name:      "volume",
			MountPath: "/etc/config",
		},
	}

}

var _ = Describe("Rider E2E", Serial, func() {

	It("Should create NumaflowControllerRollout and MonoVertexRollout", func() {
		CreateNumaflowControllerRollout(InitialNumaflowControllerVersion)
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, monoVertexSpecWithoutRider, nil)
	})

	It("Should add ConfigMap Rider to MonoVertexRollout", func() {

		// Add ConfigMap Rider and update MonoVertex spec to use it
		rawMVSpec, err := json.Marshal(monoVertexSpecWithRider)
		Expect(err).ShouldNot(HaveOccurred())
		rawConfigMapSpec, err := json.Marshal(defaultConfigMap)
		Expect(err).ShouldNot(HaveOccurred())

		// update the MonoVertexRollout
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.MonoVertex.Spec.Raw = rawMVSpec

			rollout.Spec.Riders = []apiv1.Rider{
				{
					Progressive: true,
					Definition:  runtime.RawExtension{Raw: rawConfigMapSpec},
				},
			}
			return rollout, nil
		})

		// verify Rider is created
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)
	})

	It("Should update the ConfigMap Rider", func() {
		// Update ConfigMap to add a new key/value pair
		currentConfigMap = currentConfigMap.DeepCopy()
		currentConfigMap.Data["my-key-2"] = "my-value-2"
		rawConfigMapSpec, err := json.Marshal(currentConfigMap)
		Expect(err).ShouldNot(HaveOccurred())

		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders[0].Definition = runtime.RawExtension{Raw: rawConfigMapSpec}
			return rollout, nil
		})

		monoVertexIndex++

		// Verify that this caused a Progressive upgrade and generated a new ConfigMap
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)

		// Now verify that with the Progressive upgrade, the original MonoVertex and
		// ConfigMap get cleaned up
		mvOriginalName := fmt.Sprintf("%s-%d", monoVertexRolloutName, 0)
		originalConfigMap := fmt.Sprintf("my-configmap-%s", mvOriginalName)
		VerifyResourceDoesntExist(numaflowv1.MonoVertexGroupVersionResource, mvOriginalName)
		VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, originalConfigMap)
	})

	It("Should delete the ConfigMap Rider", func() {
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders = []apiv1.Rider{}
			return rollout, nil
		})

		// Confirm the ConfigMap was deleted (but the monovertex is still present)
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)
		VerifyResourceExists(numaflowv1.MonoVertexGroupVersionResource, monoVertexName)
	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {
		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should create the ISBServiceRollout", func() {
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should create the PipelineRollout", func() {
		CreatePipelineRollout(pipelineRolloutName, Namespace, pipelineSpecWithoutRider, false)
	})

	It("Should add VPA Rider to PipelineRollout", func() {
		/*UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Riders = []apiv1.PipelineRider{
				{
					PerVertex: true,
					Rider:     apiv1.Rider{},
				},
			}
		})*/
	})
})
