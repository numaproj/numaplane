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
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestCreateUpgradingISBServiceUsesTrialControllerInstance(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))

	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
		Status: apiv1.NumaflowControllerRolloutStatus{
			ControllerInstances: []apiv1.ControllerInstanceRef{
				{InstanceID: "controller-1", State: string(common.LabelValueUpgradePromoted)},
				{InstanceID: "controller-2", State: string(common.LabelValueUpgradeTrial)},
			},
		},
	}
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
		client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerRollout).Build(),
	}

	isbsvc, err := reconciler.CreateUpgradingChildDefinition(context.Background(), isbsvcRollout, "default-1")

	require.NoError(t, err)
	assert.Equal(t, "controller-2", isbsvc.GetAnnotations()[common.AnnotationKeyNumaflowInstanceID])
	assert.Equal(t, "controller-2", isbsvc.GetLabels()[common.LabelKeyControllerInstanceID])
}
