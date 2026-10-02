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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	numaplanecommon "github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestGetControllerInstanceID(t *testing.T) {
	const (
		namespace   = "test"
		rolloutName = "controller"
	)
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	newClient := func(objects ...client.Object) client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	}
	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: rolloutName, Namespace: namespace},
	}
	newController := func(name string, upgradeState numaplanecommon.UpgradeState, instanceID string) *apiv1.NumaflowController {
		return CreateTestNumaflowController(namespace, rolloutName, name, upgradeState, instanceID)
	}

	t.Run("returns empty promoted instance before the first child exists", func(t *testing.T) {
		instanceID, err := GetPromotedControllerInstanceID(context.Background(), newClient(controllerRollout), namespace)

		require.NoError(t, err)
		assert.Equal(t, "", instanceID)
	})

	t.Run("uses promoted child instance", func(t *testing.T) {
		c := newClient(controllerRollout, newController("controller-1", numaplanecommon.LabelValueUpgradePromoted, "promoted-1"))

		instanceID, err := GetPromotedControllerInstanceID(context.Background(), c, namespace)

		require.NoError(t, err)
		assert.Equal(t, "promoted-1", instanceID)
	})

	t.Run("selects trial child as desired instance", func(t *testing.T) {
		c := newClient(controllerRollout,
			newController("controller-1", numaplanecommon.LabelValueUpgradePromoted, "promoted-1"),
			newController("controller-2", numaplanecommon.LabelValueUpgradeTrial, "trial-2"))

		instanceID, err := GetDesiredControllerInstanceID(context.Background(), c, namespace)

		require.NoError(t, err)
		assert.Equal(t, "trial-2", instanceID)
	})

	t.Run("still resolves a failed trial child's instance, like a Pipeline resolving a trial ISBService", func(t *testing.T) {
		failedTrial := newController("controller-2", numaplanecommon.LabelValueUpgradeTrial, "trial-2")
		failedTrial.Labels[numaplanecommon.LabelKeyProgressiveResultState] = string(numaplanecommon.LabelValueResultStateFailed)
		c := newClient(controllerRollout,
			newController("controller-1", numaplanecommon.LabelValueUpgradePromoted, "promoted-1"),
			failedTrial)

		promoted, trial, err := GetControllerInstanceIDs(context.Background(), c, namespace)
		require.NoError(t, err)
		assert.Equal(t, "promoted-1", promoted)
		assert.Equal(t, "trial-2", trial)

		desired, err := GetDesiredControllerInstanceID(context.Background(), c, namespace)
		require.NoError(t, err)
		assert.Equal(t, "trial-2", desired)
	})

	t.Run("ignores recyclable children and children of other rollouts", func(t *testing.T) {
		otherRolloutChild := CreateTestNumaflowController(namespace, "other", "other-1", numaplanecommon.LabelValueUpgradeTrial, "other-1")
		c := newClient(controllerRollout,
			newController("controller-1", numaplanecommon.LabelValueUpgradeRecyclable, "recyclable-1"),
			newController("controller-2", numaplanecommon.LabelValueUpgradePromoted, "promoted-2"),
			otherRolloutChild)

		promoted, trial, err := GetControllerInstanceIDs(context.Background(), c, namespace)

		require.NoError(t, err)
		assert.Equal(t, "promoted-2", promoted)
		assert.Empty(t, trial)
	})

	t.Run("returns empty IDs when no controller rollout exists", func(t *testing.T) {
		promoted, trial, err := GetControllerInstanceIDs(context.Background(), newClient(), namespace)

		require.NoError(t, err)
		assert.Empty(t, promoted)
		assert.Empty(t, trial)
	})

	t.Run("uses the newest of duplicate promoted children and marks the older one recyclable", func(t *testing.T) {
		older := newController("controller-1", numaplanecommon.LabelValueUpgradePromoted, "promoted-1")
		older.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
		newer := newController("controller-2", numaplanecommon.LabelValueUpgradePromoted, "promoted-2")
		newer.CreationTimestamp = metav1.NewTime(time.Now())
		c := newClient(controllerRollout, older, newer)

		instanceID, err := GetPromotedControllerInstanceID(context.Background(), c, namespace)

		require.NoError(t, err)
		assert.Equal(t, "promoted-2", instanceID)
		updatedOlder := &apiv1.NumaflowController{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(older), updatedOlder))
		assert.Equal(t, string(numaplanecommon.LabelValueUpgradeRecyclable), updatedOlder.GetLabels()[numaplanecommon.LabelKeyUpgradeState])
	})

	t.Run("rejects multiple controller rollouts in a namespace", func(t *testing.T) {
		first := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: namespace},
		}
		second := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: namespace},
		}

		_, _, err := GetControllerInstanceIDs(context.Background(), newClient(first, second), namespace)

		assert.ErrorContains(t, err, "expected at most 1 NumaflowControllerRollout")
	})
}
