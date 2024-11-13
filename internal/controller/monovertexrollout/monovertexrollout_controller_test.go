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

package monovertexrollout

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/util"
)

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

func fakeGenericMonoVertex(t *testing.T, s numaflowv1.MonoVertexSpec) *unstructured.Unstructured {
	t.Helper()
	monoVertexDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	monoVertexDef.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)
	monoVertexDef.SetName("test--mvtx")
	monoVertexDef.SetNamespace("test-ns")
	var monoVertexSpec map[string]interface{}
	if err := util.StructToStruct(s, &monoVertexSpec); err != nil {
		log.Fatal(err)
	}
	monoVertexDef.Object["spec"] = monoVertexSpec
	return monoVertexDef
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

			expected, existing, err := unstructured.NestedFloat64(result.Object, "spec", "replicas")
			assert.NoError(t, err)
			assert.Equal(t, tt.expected != nil, existing)
			if tt.expected != nil {
				assert.Equal(t, *tt.expected, int32(expected))
			}
		})
	}

}
