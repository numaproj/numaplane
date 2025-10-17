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

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	. "github.com/numaproj/numaplane/tests/e2e"
)

func TestPipelineRiderE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	BeforeSuite(func() {
		BeforeSuiteSetup()
	})

	RunSpecs(t, "Pipeline Rider E2E Suite")
}

const (
	isbServiceRolloutName = "test-isbservice-rollout"
	pipelineRolloutName   = "test-pipeline-rollout"
)

var (
	pullPolicyAlways = corev1.PullAlways
	pipelineIndex    = 0

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
			Container: &numaflowv1.Container{
				Image:           "quay.io/numaio/numaflow-go/map-cat:stable",
				ImagePullPolicy: &pullPolicyAlways,
			},
		},
	}
	updatedPipelineSpec.Vertices = append(updatedPipelineSpec.Vertices, outVertex)
	updatedPipelineSpec.Edges = []numaflowv1.Edge{
		{From: "in", To: "cat"},
		{From: "cat", To: "out"},
	}

}

var _ = Describe("Pipeline Rider E2E", Serial, func() {

	It("Should create the ISBServiceRollout", func() {
		CreateISBServiceRollout(isbServiceRolloutName, isbServiceSpec)
	})

	It("Should create the PipelineRollout", func() {
		CreatePipelineRollout(pipelineRolloutName, Namespace, initialPipelineSpec, false, nil)
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
