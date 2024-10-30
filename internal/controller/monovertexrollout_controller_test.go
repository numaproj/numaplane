/*
Copyright 2023.

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

package controller

import (
	"context"
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var _ = Describe("MonoVertexRollout Controller", Ordered, func() {
	const (
		namespace             = "default"
		monoVertexRolloutName = "monovertexrollout-test"
		monoVertexName        = "monovertexrollout-test-0"
	)

	ctx := context.Background()

	monoVertexSpec := numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
				},
			},
			UDTransformer: &numaflowv1.UDTransformer{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				UDSink: &numaflowv1.UDSink{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-java/simple-sink:stable",
					},
				},
			},
		},
	}

	monoVertexSpecRaw, err := json.Marshal(monoVertexSpec)
	Expect(err).ToNot(HaveOccurred())

	monoVertexRollout := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      monoVertexRolloutName,
		},
		Spec: apiv1.MonoVertexRolloutSpec{
			MonoVertex: apiv1.MonoVertex{
				Spec: runtime.RawExtension{
					Raw: monoVertexSpecRaw,
				},
			},
		},
	}

	rolloutResourceLookupKey := types.NamespacedName{Name: monoVertexRolloutName, Namespace: namespace}
	mvResourceLookupKey := types.NamespacedName{Name: monoVertexName, Namespace: namespace}

	Context("When applying a MonoVertexRollout spec", func() {

		It("Should create the MonoVertexRollout if it does not exist", func() {

			Expect(k8sClient.Create(ctx, monoVertexRollout)).Should(Succeed())

			createdResource := &apiv1.MonoVertexRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, rolloutResourceLookupKey, createdResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			createdMonoVertexSpec := numaflowv1.MonoVertexSpec{}
			Expect(json.Unmarshal(createdResource.Spec.MonoVertex.Spec.Raw, &createdMonoVertexSpec)).ToNot(HaveOccurred())

			By("Verifying the content of the MonoVertexRollout spec field")
			Expect(createdMonoVertexSpec).Should(Equal(monoVertexSpec))
		})

		It("Should have created a MonoVertex", func() {
			createdMonoVertex := &numaflowv1.MonoVertex{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, mvResourceLookupKey, createdMonoVertex)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			By("Veryifying the content of the MonoVertex spec")
			Expect(createdMonoVertex.Spec).Should(Equal(monoVertexSpec))
		})

		It("Should have the MonoVertexRollout.Status.Phase as Deployed and ObservedGeneration matching Generation", func() {
			verifyStatusPhase(ctx, apiv1.MonoVertexRolloutGroupVersionKind, namespace, monoVertexRolloutName, apiv1.PhaseDeployed)
		})

		It("Should have the metrics updated", func() {
			By("Verifying the MonoVertex metrics")
			Expect(testutil.ToFloat64(customMetrics.MonoVertexRolloutsRunning.WithLabelValues(namespace))).Should(Equal(float64(1)))
			Expect(testutil.ToFloat64(customMetrics.MonoVertexROSyncs.WithLabelValues())).Should(BeNumerically(">", 1))
		})

		It("Should update the MonoVertexRollout and MonoVertex", func() {
			By("Updating the MonoVertexRollout")

			currentMonoVertexRollout := &apiv1.MonoVertexRollout{}
			Expect(k8sClient.Get(ctx, rolloutResourceLookupKey, currentMonoVertexRollout)).ToNot(HaveOccurred())

			newMonoVertexSpec := numaflowv1.MonoVertexSpec{
				Replicas: ptr.To(int32(1)),
				Source: &numaflowv1.Source{
					UDSource: &numaflowv1.UDSource{
						Container: &numaflowv1.Container{
							Image: "quay.io/numaio/numaflow-java/source-simple-source:v0.6.0",
						},
					},
					UDTransformer: &numaflowv1.UDTransformer{
						Container: &numaflowv1.Container{
							Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
						},
					},
				},
				Sink: &numaflowv1.Sink{
					AbstractSink: numaflowv1.AbstractSink{
						UDSink: &numaflowv1.UDSink{
							Container: &numaflowv1.Container{
								Image: "quay.io/numaio/numaflow-java/simple-sink:stable",
							},
						},
					},
				},
			}

			newMonoVertexSpecRaw, err := json.Marshal(newMonoVertexSpec)
			Expect(err).ToNot(HaveOccurred())

			currentMonoVertexRollout.Spec.MonoVertex.Spec.Raw = newMonoVertexSpecRaw

			Expect(k8sClient.Update(ctx, currentMonoVertexRollout)).ToNot(HaveOccurred())

			By("Verifying the content of the MonoVertexRollout")
			Eventually(func() (numaflowv1.MonoVertexSpec, error) {
				updatedResource := &apiv1.MonoVertexRollout{}
				err := k8sClient.Get(ctx, rolloutResourceLookupKey, updatedResource)
				if err != nil {
					return numaflowv1.MonoVertexSpec{}, err
				}

				createdMonoVertexSpec := numaflowv1.MonoVertexSpec{}
				Expect(json.Unmarshal(updatedResource.Spec.MonoVertex.Spec.Raw, &createdMonoVertexSpec)).ToNot(HaveOccurred())

				return createdMonoVertexSpec, nil
			}, timeout, interval).Should(Equal(newMonoVertexSpec))

			By("Verifying that the MonoVertexRollout.Status.Phase is Deployed and ObservedGeneration matches Generation")
			verifyStatusPhase(ctx, apiv1.MonoVertexRolloutGroupVersionKind, namespace, monoVertexRolloutName, apiv1.PhaseDeployed)
		})

		It("Should auto heal the MonoVertex when the spec is directly changed", func() {
			By("Updating the MonoVertex and verifying the changed field is the same")
			verifyAutoHealing(ctx, numaflowv1.MonoVertexGroupVersionKind, namespace, monoVertexName, "spec.source.udsource.container.image", "wrong-image")
		})

		It("Should delete the MonoVertexRollout and MonoVertex", func() {
			Expect(k8sClient.Delete(ctx, &apiv1.MonoVertexRollout{
				ObjectMeta: monoVertexRollout.ObjectMeta,
			})).Should(Succeed())

			deletedResource := &apiv1.MonoVertexRollout{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, rolloutResourceLookupKey, deletedResource)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			deletingChildResource := &numaflowv1.MonoVertex{}
			Eventually(func() bool {
				err := k8sClient.Get(ctx, mvResourceLookupKey, deletingChildResource)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(deletingChildResource.OwnerReferences).Should(HaveLen(1))
			Expect(deletedResource.UID).Should(Equal(deletingChildResource.OwnerReferences[0].UID))
		})

	})

	Context("When applying an invalid MonoVertexRollout spec", func() {
		It("Should not create the MonoVertexRollout", func() {
			Expect(k8sClient.Create(ctx, &apiv1.MonoVertexRollout{
				Spec: monoVertexRollout.Spec,
			}))

			Expect(k8sClient.Create(ctx, &apiv1.MonoVertexRollout{
				ObjectMeta: monoVertexRollout.ObjectMeta,
			}))

			Expect(k8sClient.Create(ctx, &apiv1.MonoVertexRollout{
				ObjectMeta: monoVertexRollout.ObjectMeta,
				Spec:       apiv1.MonoVertexRolloutSpec{},
			}))
		})
	})

})

func fakeMonoVertexSpec(t *testing.T) numaflowv1.MonoVertexSpec {
	t.Helper()
	return numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
				},
			},
			UDTransformer: &numaflowv1.UDTransformer{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				UDSink: &numaflowv1.UDSink{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-java/simple-sink:stable",
					},
				},
			},
		},
	}
}

func fakeGenericMonoVertex(t *testing.T, s numaflowv1.MonoVertexSpec) *kubernetes.GenericObject {
	t.Helper()
	monoVertexSpecRaw, _ := json.Marshal(s)
	return &kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "MonoVertex",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test--mvtx",
			Namespace: "test-ns",
		},
		Spec: runtime.RawExtension{
			Raw: monoVertexSpecRaw,
		},
	}
}

func Test_withExistingMvtxReplicas(t *testing.T) {
	tests := []struct {
		name             string
		existingReplicas *int32
		newReplicas      *int32
		expected         *int32
	}{
		{
			name:             "nil existing replicas",
			existingReplicas: nil,
			newReplicas:      ptr.To(int32(2)),
			expected:         ptr.To(int32(2)),
		},
		{
			name:             "both nil",
			existingReplicas: nil,
			newReplicas:      nil,
			expected:         nil,
		},
		{
			name:             "existing replicas not nil, new replicas not nil",
			existingReplicas: ptr.To(int32(2)),
			newReplicas:      ptr.To(int32(1)),
			expected:         ptr.To(int32(2)),
		},
		{
			name:             "existing replicas not nil, new replicas nil",
			existingReplicas: ptr.To(int32(2)),
			newReplicas:      nil,
			expected:         ptr.To(int32(2)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existingMvtxSpec := fakeMonoVertexSpec(t)
			existingMvtxSpec.Replicas = tt.existingReplicas
			existingGenericMvtx := fakeGenericMonoVertex(t, existingMvtxSpec)

			newMvtxSpec := fakeMonoVertexSpec(t)
			newMvtxSpec.Replicas = tt.newReplicas
			newGenericMvtx := fakeGenericMonoVertex(t, newMvtxSpec)

			result, err := withExistingMvtxReplicas(existingGenericMvtx, newGenericMvtx)
			assert.NoError(t, err)

			unstruc, err := kubernetes.ObjectToUnstructured(result)
			assert.NoError(t, err)

			expected, existing, err := unstructured.NestedFloat64(unstruc.Object, "spec", "replicas")
			assert.NoError(t, err)
			assert.Equal(t, tt.expected != nil, existing)
			if tt.expected != nil {
				assert.Equal(t, *tt.expected, int32(expected))
			}
		})
	}

}
