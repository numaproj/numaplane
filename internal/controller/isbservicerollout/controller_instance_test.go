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

package isbservicerollout

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

func TestCreateUpgradingISBServiceUsesTrialControllerInstance(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
	}
	promotedController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-1", common.LabelValueUpgradePromoted, "controller-1")
	trialController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-2", common.LabelValueUpgradeTrial, "controller-2")
	isbsvcRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "test"},
		Spec: apiv1.ISBServiceRolloutSpec{
			InterStepBufferService: apiv1.InterStepBufferService{
				Metadata: apiv1.Metadata{Labels: map[string]string{}, Annotations: map[string]string{}},
				Spec:     runtime.RawExtension{Raw: []byte(`{}`)},
			},
		},
	}
	reconciler := &ISBServiceRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerRollout, promotedController, trialController).Build(),
	}

	isbsvc, err := reconciler.CreateUpgradingChildDefinition(context.Background(), isbsvcRollout, "default-1")

	require.NoError(t, err)
	assert.Equal(t, "controller-2", isbsvc.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID])
	assert.Equal(t, "controller-2", isbsvc.GetLabels()[common.LabelKeyControllerInstanceID])
}

func TestMapNumaflowControllerToISBServiceRollouts(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	rolloutA := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Name: "isbsvc-a", Namespace: "test"}}
	rolloutB := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Name: "isbsvc-b", Namespace: "test"}}
	otherNamespaceRollout := &apiv1.ISBServiceRollout{ObjectMeta: metav1.ObjectMeta{Name: "isbsvc-c", Namespace: "other"}}

	reconciler := &ISBServiceRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(rolloutA, rolloutB, otherNamespaceRollout).Build(),
	}

	trialController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-2", common.LabelValueUpgradeTrial, "controller-2")

	reqs := reconciler.mapNumaflowControllerToISBServiceRollouts(context.Background(), trialController)

	// every ISBServiceRollout in the NumaflowController's namespace is enqueued, regardless of which controller
	// instance it is currently bound to - and no rollout from a different namespace is enqueued
	require.Len(t, reqs, 2)
	names := []string{reqs[0].Name, reqs[1].Name}
	assert.ElementsMatch(t, []string{"isbsvc-a", "isbsvc-b"}, names)
	for _, req := range reqs {
		assert.Equal(t, "test", req.Namespace)
	}
}

func TestMapNumaflowControllerToISBServiceRollouts_NoRolloutsInNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	reconciler := &ISBServiceRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).Build(),
	}

	trialController := ctlrcommon.CreateTestNumaflowController("test", "controller", "controller-2", common.LabelValueUpgradeTrial, "controller-2")

	reqs := reconciler.mapNumaflowControllerToISBServiceRollouts(context.Background(), trialController)

	assert.Empty(t, reqs)
}
