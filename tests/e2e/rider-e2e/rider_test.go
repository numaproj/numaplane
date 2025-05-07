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

	v1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

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
	pipelineIndex              = 0
	monoVertexSpecWithoutRider numaflowv1.MonoVertexSpec
	monoVertexSpecWithRider    numaflowv1.MonoVertexSpec

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
	initialPipelineSpec numaflowv1.PipelineSpec
	updatedPipelineSpec numaflowv1.PipelineSpec
	defaultVertexVPA    = `
	{
		"apiVersion": "autoscaling.k8s.io/v1",
		"kind": "VerticalPodAutoscaler",
	 	"metadata": 
		{
	 		"name": "my-vpa"
	 	},
	 	"spec": 
		{
	 		"targetRef": {
	 			"apiVersion": "numaproj.io/v1alpha1",
	 			"kind": "Vertex",
	 			"name": "{{.pipeline-name}}-{{.vertex-name}}"
	 		}
	 	}
	}
	
`
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

	initialPipelineSpec = numaflowv1.PipelineSpec{
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
	updatedPipelineSpec = *initialPipelineSpec.DeepCopy()
	outVertex := updatedPipelineSpec.Vertices[1]
	updatedPipelineSpec.Vertices[1] = numaflowv1.AbstractVertex{
		Name: "cat",
		UDF: &numaflowv1.UDF{
			Builtin: &numaflowv1.Function{
				Name: "cat",
			},
		},
	}
	updatedPipelineSpec.Vertices = append(updatedPipelineSpec.Vertices, outVertex)
	updatedPipelineSpec.Edges = []numaflowv1.Edge{
		{From: "in", To: "cat"},
		{From: "cat", To: "out"},
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

		// verify ConfigMap is created
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
		mvOriginalName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex-1)
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
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false)
	})

	It("Should add VPA Rider to PipelineRollout", func() {

		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Riders = []apiv1.PipelineRider{
				{
					PerVertex: true,
					Rider: apiv1.Rider{
						Definition: runtime.RawExtension{Raw: []byte(defaultVertexVPA)},
					},
				},
			}
			return rollout, nil
		})

		// verify VPAs are created
		pipelineName := fmt.Sprintf("%s-%d", pipelineRolloutName, pipelineIndex)

		vertices := []string{"in", "out"}
		for _, vertex := range vertices {
			// VPA is named with the pipeline name and vertex name as the suffix
			vpaName := fmt.Sprintf("my-vpa-%s-%s", pipelineName, vertex)
			VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}, vpaName)
		}

		// VPA is named with the pipeline name and vertex name as the suffix
		vpaName := fmt.Sprintf("my-vpa-%s-in", pipelineName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}, vpaName)
		vpaName = fmt.Sprintf("my-vpa-%s-out", pipelineName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}, vpaName)
	})

	It("Should update Pipeline Topology in PipelineRollout", func() {
		rawPipelineSpec, err := json.Marshal(updatedPipelineSpec)
		Expect(err).ShouldNot(HaveOccurred())

		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Pipeline.Spec.Raw = rawPipelineSpec
			return rollout, nil
		})

		pipelineIndex++

		// make sure we created VPAs for all 3 vertices
		newPipelineName := fmt.Sprintf("%s-%d", pipelineRolloutName, pipelineIndex)
		vertices := []string{"in", "cat", "out"}
		for _, vertex := range vertices {
			// VPA is named with the pipeline name and vertex name as the suffix
			vpaName := fmt.Sprintf("my-vpa-%s-%s", newPipelineName, vertex)
			VerifyResourceExists(schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}, vpaName)
		}

		// make sure the original VPAs are removed once the pipeline is deleted
		originalPipelineName := fmt.Sprintf("%s-%d", pipelineRolloutName, pipelineIndex-1) // TODO: can we create a variable at the top and update it instead of repeating?
		vertices = []string{"in", "out"}
		for _, vertex := range vertices {
			// VPA is named with the pipeline name and vertex name as the suffix
			vpaName := fmt.Sprintf("my-vpa-%s-%s", originalPipelineName, vertex)
			VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}, vpaName)
		}

	})

	It("Should delete the VPA Rider", func() {
		UpdatePipelineRolloutInK8S(Namespace, pipelineRolloutName, func(rollout apiv1.PipelineRollout) (apiv1.PipelineRollout, error) {
			rollout.Spec.Riders = []apiv1.PipelineRider{}
			return rollout, nil
		})

		// Confirm the VPAs were deleted
		newPipelineName := fmt.Sprintf("%s-%d", pipelineRolloutName, pipelineIndex)
		vertices := []string{"in", "cat", "out"}
		for _, vertex := range vertices {
			// VPA is named with the pipeline name and vertex name as the suffix
			vpaName := fmt.Sprintf("my-vpa-%s-%s", newPipelineName, vertex)
			VerifyResourceDoesntExist(schema.GroupVersionResource{Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"}, vpaName)
		}
	})

	It("Should delete the PipelineRollout and ISBServiceRollout", func() {
		DeletePipelineRollout(pipelineRolloutName)
		DeleteISBServiceRollout(isbServiceRolloutName)
	})

	It("Should delete the NumaflowControllerRollout", func() {
		DeleteNumaflowControllerRollout()
	})
})
