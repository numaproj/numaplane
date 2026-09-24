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

package numaflowtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/common"
)

func TestWithControllerInstanceID(t *testing.T) {
	t.Run("sets annotation and label from selected instance", func(t *testing.T) {
		obj := &unstructured.Unstructured{}

		err := WithControllerInstanceID(obj, "controller-1")

		require.NoError(t, err)
		assert.Equal(t, "controller-1", obj.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID])
		assert.Equal(t, "controller-1", obj.GetLabels()[common.LabelKeyControllerInstanceID])
	})

	t.Run("preserves rollout annotation and derives label when selected instance is empty", func(t *testing.T) {
		obj := &unstructured.Unstructured{}
		obj.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "manual-instance"})

		err := WithControllerInstanceID(obj, "")

		require.NoError(t, err)
		assert.Equal(t, "manual-instance", obj.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID])
		assert.Equal(t, "manual-instance", obj.GetLabels()[common.LabelKeyControllerInstanceID])
	})

	t.Run("removes stale label when no annotation exists", func(t *testing.T) {
		obj := &unstructured.Unstructured{}
		obj.SetLabels(map[string]string{common.LabelKeyControllerInstanceID: "stale"})

		err := WithControllerInstanceID(obj, "")

		require.NoError(t, err)
		_, found := obj.GetLabels()[common.LabelKeyControllerInstanceID]
		assert.False(t, found)
	})

	t.Run("rejects instance IDs that cannot be labels", func(t *testing.T) {
		obj := &unstructured.Unstructured{}

		err := WithControllerInstanceID(obj, "region/controller")

		assert.ErrorContains(t, err, "not a valid Kubernetes label value")
	})
}

func TestHasStaleControllerInstanceBinding(t *testing.T) {
	t.Run("detects stale annotation when desired definition has none", func(t *testing.T) {
		stale := HasStaleControllerInstanceBinding(
			map[string]string{},
			map[string]string{},
			map[string]string{},
			map[string]string{common.AnnotationKeyNumaflowInstanceID: "old"},
		)
		assert.True(t, stale)
	})

	t.Run("detects stale label when desired definition has none", func(t *testing.T) {
		stale := HasStaleControllerInstanceBinding(
			map[string]string{},
			map[string]string{},
			map[string]string{common.LabelKeyControllerInstanceID: "old"},
			map[string]string{},
		)
		assert.True(t, stale)
	})

	t.Run("ignores extra unrelated metadata", func(t *testing.T) {
		stale := HasStaleControllerInstanceBinding(
			map[string]string{"app": "pipeline"},
			map[string]string{},
			map[string]string{"app": "pipeline", "extra": "ok"},
			map[string]string{"note": "ok"},
		)
		assert.False(t, stale)
	})
}
