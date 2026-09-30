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

package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	numaplanecommon "github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestGetControllerInstanceID(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	newClient := func(rollouts ...*apiv1.NumaflowControllerRollout) *fake.ClientBuilder {
		builder := fake.NewClientBuilder().WithScheme(scheme)
		for _, rollout := range rollouts {
			builder = builder.WithObjects(rollout)
		}
		return builder
	}

	t.Run("preserves legacy promoted instance", func(t *testing.T) {
		rollout := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
			Spec: apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{InstanceID: "legacy"},
			},
		}

		instanceID, err := GetPromotedControllerInstanceID(context.Background(), newClient(rollout).Build(), "test")

		require.NoError(t, err)
		assert.Equal(t, "legacy", instanceID)
	})

	t.Run("prefers promoted status instance", func(t *testing.T) {
		rollout := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
			Spec: apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{InstanceID: "legacy"},
			},
			Status: apiv1.NumaflowControllerRolloutStatus{
				ControllerInstances: []apiv1.ControllerInstanceRef{
					{InstanceID: "promoted-1", State: string(numaplanecommon.LabelValueUpgradePromoted)},
				},
			},
		}

		instanceID, err := GetPromotedControllerInstanceID(context.Background(), newClient(rollout).Build(), "test")

		require.NoError(t, err)
		assert.Equal(t, "promoted-1", instanceID)
	})

	t.Run("selects trial as desired instance", func(t *testing.T) {
		rollout := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
			Status: apiv1.NumaflowControllerRolloutStatus{
				ControllerInstances: []apiv1.ControllerInstanceRef{
					{InstanceID: "promoted-1", State: string(numaplanecommon.LabelValueUpgradePromoted)},
					{InstanceID: "trial-2", State: string(numaplanecommon.LabelValueUpgradeTrial)},
				},
			},
		}

		instanceID, err := GetDesiredControllerInstanceID(context.Background(), newClient(rollout).Build(), "test")

		require.NoError(t, err)
		assert.Equal(t, "trial-2", instanceID)
	})

	t.Run("rejects instance IDs that cannot be labels", func(t *testing.T) {
		rollout := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
			Spec: apiv1.NumaflowControllerRolloutSpec{
				Controller: apiv1.Controller{InstanceID: "invalid/id"},
			},
		}

		_, err := GetPromotedControllerInstanceID(context.Background(), newClient(rollout).Build(), "test")

		assert.ErrorContains(t, err, "not a valid Kubernetes label value")
	})

	t.Run("returns empty IDs when no controller rollout exists", func(t *testing.T) {
		promoted, trial, err := GetControllerInstanceIDs(context.Background(), newClient().Build(), "test")

		require.NoError(t, err)
		assert.Empty(t, promoted)
		assert.Empty(t, trial)
	})

	t.Run("rejects duplicate trial status entries", func(t *testing.T) {
		rollout := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
			Status: apiv1.NumaflowControllerRolloutStatus{
				ControllerInstances: []apiv1.ControllerInstanceRef{
					{InstanceID: "trial-1", State: string(numaplanecommon.LabelValueUpgradeTrial)},
					{InstanceID: "trial-2", State: string(numaplanecommon.LabelValueUpgradeTrial)},
				},
			},
		}

		_, _, err := GetControllerInstanceIDs(context.Background(), newClient(rollout).Build(), "test")

		assert.ErrorContains(t, err, `expected at most 1 "trial" controller instance`)
	})

	t.Run("rejects multiple controller rollouts in a namespace", func(t *testing.T) {
		first := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "test"},
		}
		second := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "test"},
		}

		_, _, err := GetControllerInstanceIDs(context.Background(), newClient(first, second).Build(), "test")

		assert.ErrorContains(t, err, "expected at most 1 NumaflowControllerRollout")
	})
}
