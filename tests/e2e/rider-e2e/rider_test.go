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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
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
)

var (
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
		monoVertexName := fmt.Sprintf("%s-0", monoVertexRolloutName)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, configMapName)
	})

})
