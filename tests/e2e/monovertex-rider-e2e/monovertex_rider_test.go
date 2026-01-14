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
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

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

	configMapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	pdbGVR       = schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}

	defaultConfigMap = corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-configmap",
		},
		Data: map[string]string{
			"monovertex-namespace": "{{.monovertex-namespace}}",
			"monovertex-name":      "{{.monovertex-name}}",
		},
	}
	currentConfigMap = &defaultConfigMap

	defaultPDB = policyv1.PodDisruptionBudget{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "policy/v1",
			Kind:       "PodDisruptionBudget",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pdb",
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"numaflow.numaproj.io/monovertex-name": "{{.monovertex-name}}",
				},
			},
		},
	}
	currentPDB = &defaultPDB
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
		CreateMonoVertexRollout(monoVertexRolloutName, Namespace, monoVertexSpecWithoutRider, nil, apiv1.Metadata{})
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
					Progressive: true,
					Definition:  runtime.RawExtension{Raw: rawConfigMapSpec},
				},
			}
			return rollout, nil
		})

		monoVertexIndex++

		// verify ConfigMap is created (this causes a Progressive upgrade due to the change to the MonoVertex volumeMount)
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		mvOriginalName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex-1)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(configMapGVR, configMapName)
		VerifyResourceFieldMatchesRegex(configMapGVR, configMapName, Namespace, "data", "monovertex-namespace")
		VerifyResourceFieldMatchesRegex(configMapGVR, configMapName, monoVertexName, "data", "monovertex-name")
		VerifyResourceDoesntExist(numaflowv1.MonoVertexGroupVersionResource, mvOriginalName)
	})

	It("Should add PDB Rider to MonoVertexRollout", func() {

		// Add PDB Rider to existing ConfigMap Rider
		rawPDBSpec, err := json.Marshal(defaultPDB)
		Expect(err).ShouldNot(HaveOccurred())

		// update the MonoVertexRollout to include both riders
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders = append(rollout.Spec.Riders, apiv1.Rider{
				Progressive: false,
				Definition:  runtime.RawExtension{Raw: rawPDBSpec},
			})

			return rollout, nil
		})

		// verify PDB is created for the existing MonoVertex in place
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// PDB is named with the monovertex name as the suffix
		pdbName := fmt.Sprintf("pdb-%s", monoVertexName)
		VerifyResourceExists(pdbGVR, pdbName)
		VerifyResourceFieldMatchesRegex(pdbGVR, pdbName, monoVertexName, "spec", "selector", "matchLabels", "numaflow.numaproj.io/monovertex-name")
	})

	It("Should update the ConfigMap Rider as a Progressive rollout change", func() {
		// Update ConfigMap to add a new key/value pair
		currentConfigMap = currentConfigMap.DeepCopy()
		currentConfigMap.Data["my-key-2"] = "my-value-2"
		rawConfigMapSpec, err := json.Marshal(currentConfigMap)
		Expect(err).ShouldNot(HaveOccurred())

		// Keep PDB unchanged
		rawPDBSpec, err := json.Marshal(currentPDB)
		Expect(err).ShouldNot(HaveOccurred())

		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders[0].Definition = runtime.RawExtension{Raw: rawConfigMapSpec}
			rollout.Spec.Riders[1].Definition = runtime.RawExtension{Raw: rawPDBSpec}
			return rollout, nil
		})

		monoVertexIndex++

		// Verify that this caused a Progressive upgrade and generated new ConfigMap and PDB
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is named with the monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(configMapGVR, configMapName)
		VerifyResourceFieldMatchesRegex(configMapGVR, configMapName, "my-value-2", "data", "my-key-2")
		VerifyResourceFieldMatchesRegex(configMapGVR, configMapName, Namespace, "data", "monovertex-namespace")
		VerifyResourceFieldMatchesRegex(configMapGVR, configMapName, monoVertexName, "data", "monovertex-name")

		// PDB is named with the monovertex name as the suffix
		pdbName := fmt.Sprintf("pdb-%s", monoVertexName)
		VerifyResourceExists(pdbGVR, pdbName)
		VerifyResourceFieldMatchesRegex(pdbGVR, pdbName, monoVertexName, "spec", "selector", "matchLabels", "numaflow.numaproj.io/monovertex-name")

		// Now verify that with the Progressive upgrade, the original MonoVertex,
		// ConfigMap, and PDB get cleaned up
		mvOriginalName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex-1)
		originalConfigMap := fmt.Sprintf("my-configmap-%s", mvOriginalName)
		originalPDB := fmt.Sprintf("pdb-%s", mvOriginalName)
		VerifyResourceDoesntExist(numaflowv1.MonoVertexGroupVersionResource, mvOriginalName)
		VerifyResourceDoesntExist(configMapGVR, originalConfigMap)
		VerifyResourceDoesntExist(pdbGVR, originalPDB)
	})

	It("Should update the PDB Rider in place", func() {

		// Update PDB to change minAvailable
		currentPDB = currentPDB.DeepCopy()
		currentPDB.Spec.MinAvailable = &intstr.IntOrString{Type: intstr.Int, IntVal: 2}
		rawPDBSpec, err := json.Marshal(currentPDB)
		Expect(err).ShouldNot(HaveOccurred())

		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders[1].Definition = runtime.RawExtension{Raw: rawPDBSpec}
			return rollout, nil
		})

		// Verify that this caused an in place update of the PDB
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		// ConfigMap is still there and named with the same monovertex name as the suffix
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		VerifyResourceExists(configMapGVR, configMapName)
		// PDB is still there and named with the same monovertex name as the suffix
		pdbName := fmt.Sprintf("pdb-%s", monoVertexName)
		VerifyResourceExists(pdbGVR, pdbName)

		// Verify that the PDB content was updated to reflect the minAvailable change from 1 to 2
		CheckEventually(fmt.Sprintf("verifying PDB %s has minAvailable=2", pdbName), func() bool {
			pdbResource, err := GetResource(pdbGVR, Namespace, pdbName)
			if err != nil || pdbResource == nil {
				return false
			}

			// Extract minAvailable from the PDB spec
			spec, found, err := unstructured.NestedMap(pdbResource.Object, "spec")
			if err != nil || !found {
				return false
			}

			minAvailable, found, err := unstructured.NestedFieldNoCopy(spec, "minAvailable")
			if err != nil || !found {
				return false
			}

			// minAvailable can be an int or a string
			switch v := minAvailable.(type) {
			case int64:
				return v == 2
			case float64:
				return int(v) == 2
			}
			return false
		}).WithTimeout(DefaultTestTimeout).Should(BeTrue())
	})

	It("Should delete the ConfigMap and PDB Riders", func() {
		UpdateMonoVertexRolloutInK8S(monoVertexRolloutName, func(rollout apiv1.MonoVertexRollout) (apiv1.MonoVertexRollout, error) {
			rollout.Spec.Riders = []apiv1.Rider{}
			return rollout, nil
		})

		// Confirm the ConfigMap and PDB were deleted (but the monovertex is still present)
		monoVertexName := fmt.Sprintf("%s-%d", monoVertexRolloutName, monoVertexIndex)
		configMapName := fmt.Sprintf("my-configmap-%s", monoVertexName)
		pdbName := fmt.Sprintf("pdb-%s", monoVertexName)
		VerifyResourceDoesntExist(configMapGVR, configMapName)
		VerifyResourceDoesntExist(pdbGVR, pdbName)
		VerifyResourceExists(numaflowv1.MonoVertexGroupVersionResource, monoVertexName)
	})

	It("Should delete the MonoVertexRollout and child MonoVertex", func() {
		DeleteMonoVertexRollout(monoVertexRolloutName)
	})

	It("Should delete the NumaflowControllerRollout", func() {
		DeleteNumaflowControllerRollout()
	})
})
