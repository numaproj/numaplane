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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestCreateUpgradingMonoVertexUsesTrialControllerInstance(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
	}
	promotedController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-1", common.LabelValueUpgradePromoted, "controller-1")
	trialController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-2", common.LabelValueUpgradeTrial, "controller-2")
	monoVertexRollout := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "mono-vertex", Namespace: "test"},
		Spec: apiv1.MonoVertexRolloutSpec{
			MonoVertex: apiv1.MonoVertex{
				Metadata: apiv1.Metadata{Labels: map[string]string{}, Annotations: map[string]string{}},
				Spec:     runtime.RawExtension{Raw: []byte(`{}`)},
			},
		},
	}
	reconciler := &MonoVertexRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerRollout, promotedController, trialController).Build(),
	}

	monoVertex, err := reconciler.CreateUpgradingChildDefinition(context.Background(), monoVertexRollout, "mono-vertex-1")

	require.NoError(t, err)
	assert.Equal(t, "controller-2", monoVertex.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID])
	assert.Equal(t, "controller-2", monoVertex.GetLabels()[common.LabelKeyControllerInstanceID])
}

func TestCreateUpgradingMonoVertexWithoutControllerRollout(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	newRollout := func(annotations map[string]string) *apiv1.MonoVertexRollout {
		return &apiv1.MonoVertexRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "mono-vertex", Namespace: "test"},
			Spec: apiv1.MonoVertexRolloutSpec{
				MonoVertex: apiv1.MonoVertex{
					Metadata: apiv1.Metadata{Labels: map[string]string{}, Annotations: annotations},
					Spec:     runtime.RawExtension{Raw: []byte(`{}`)},
				},
			},
		}
	}
	reconciler := &MonoVertexRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
	}

	t.Run("leaves the trial unbound when the rollout sets no instance", func(t *testing.T) {
		monoVertex, err := reconciler.CreateUpgradingChildDefinition(context.Background(), newRollout(map[string]string{}), "mono-vertex-1")

		require.NoError(t, err)
		assert.Equal(t, string(common.LabelValueUpgradeTrial), monoVertex.GetLabels()[common.LabelKeyUpgradeState])
		_, annFound := monoVertex.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID]
		_, labelFound := monoVertex.GetLabels()[common.LabelKeyControllerInstanceID]
		assert.False(t, annFound)
		assert.False(t, labelFound)
	})

	t.Run("keeps an instance annotation supplied on the rollout", func(t *testing.T) {
		monoVertex, err := reconciler.CreateUpgradingChildDefinition(context.Background(), newRollout(map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "cluster-controller",
		}), "mono-vertex-1")

		require.NoError(t, err)
		assert.Equal(t, "cluster-controller", monoVertex.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID])
		assert.Equal(t, "cluster-controller", monoVertex.GetLabels()[common.LabelKeyControllerInstanceID])
	})
}

func TestMapNumaflowControllerToMonoVertexRollouts(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	rolloutA := &apiv1.MonoVertexRollout{ObjectMeta: metav1.ObjectMeta{Name: "mv-a", Namespace: "test"}}
	rolloutB := &apiv1.MonoVertexRollout{ObjectMeta: metav1.ObjectMeta{Name: "mv-b", Namespace: "test"}}
	otherNamespaceRollout := &apiv1.MonoVertexRollout{ObjectMeta: metav1.ObjectMeta{Name: "mv-c", Namespace: "other"}}

	reconciler := &MonoVertexRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(rolloutA, rolloutB, otherNamespaceRollout).Build(),
	}

	trialController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-2", common.LabelValueUpgradeTrial, "controller-2")

	reqs := reconciler.mapNumaflowControllerToMonoVertexRollouts(context.Background(), trialController)

	// every MonoVertexRollout in the NumaflowController's namespace is enqueued, regardless of which controller
	// instance it is currently bound to - and no rollout from a different namespace is enqueued
	require.Len(t, reqs, 2)
	names := []string{reqs[0].Name, reqs[1].Name}
	assert.ElementsMatch(t, []string{"mv-a", "mv-b"}, names)
	for _, req := range reqs {
		assert.Equal(t, "test", req.Namespace)
	}
}

func TestMapNumaflowControllerToMonoVertexRollouts_NoRolloutsInNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	reconciler := &MonoVertexRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
	}

	trialController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-2", common.LabelValueUpgradeTrial, "controller-2")

	reqs := reconciler.mapNumaflowControllerToMonoVertexRollouts(context.Background(), trialController)

	assert.Empty(t, reqs)
}
