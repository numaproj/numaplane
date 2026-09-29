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

package pipelinerollout

import (
	"context"
	"encoding/json"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestGetTargetPipelineDependencies(t *testing.T) {
	const (
		namespace          = "test"
		isbsvcRolloutName  = "default"
		promotedInstanceID = "controller-1"
		trialInstanceID    = "controller-2"
	)

	pipelineSpec, err := json.Marshal(numaflowv1.PipelineSpec{InterStepBufferServiceName: isbsvcRolloutName})
	require.NoError(t, err)
	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{Raw: pipelineSpec},
			},
		},
	}
	isbsvcRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: isbsvcRolloutName, Namespace: namespace},
	}
	newISBSvc := func(name string, state common.UpgradeState, instanceID string) *numaflowv1.InterStepBufferService {
		isbsvc := &numaflowv1.InterStepBufferService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					common.LabelKeyParentRollout: isbsvcRolloutName,
					common.LabelKeyUpgradeState:  string(state),
				},
			},
		}
		if instanceID != "" {
			isbsvc.Annotations = map[string]string{common.AnnotationKeyNumaflowInstanceID: instanceID}
		}
		return isbsvc
	}

	tests := []struct {
		name                 string
		isbServices          []client.Object
		expectedISBSvcName   string
		expectedControllerID string
	}{
		{
			name:                 "uses promoted ISBService and its controller instance",
			isbServices:          []client.Object{newISBSvc("default-0", common.LabelValueUpgradePromoted, promotedInstanceID)},
			expectedISBSvcName:   "default-0",
			expectedControllerID: promotedInstanceID,
		},
		{
			name: "prefers trial ISBService and follows its controller instance",
			isbServices: []client.Object{
				newISBSvc("default-0", common.LabelValueUpgradePromoted, promotedInstanceID),
				newISBSvc("default-1", common.LabelValueUpgradeTrial, trialInstanceID),
			},
			expectedISBSvcName:   "default-1",
			expectedControllerID: trialInstanceID,
		},
		{
			name: "uses trial ISBService on the promoted controller instance for an ISBService-only upgrade",
			isbServices: []client.Object{
				newISBSvc("default-0", common.LabelValueUpgradePromoted, promotedInstanceID),
				newISBSvc("default-1", common.LabelValueUpgradeTrial, promotedInstanceID),
			},
			expectedISBSvcName:   "default-1",
			expectedControllerID: promotedInstanceID,
		},
		{
			name:                 "uses the default controller instance when the ISBService has no instance annotation",
			isbServices:          []client.Object{newISBSvc("default-0", common.LabelValueUpgradePromoted, "")},
			expectedISBSvcName:   "default-0",
			expectedControllerID: "",
		},
		{
			name:        "resolves nothing when there is no ISBService",
			isbServices: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, apiv1.AddToScheme(scheme))
			require.NoError(t, numaflowv1.AddToScheme(scheme))
			objects := append([]client.Object{isbsvcRollout}, tt.isbServices...)
			client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			reconciler := &PipelineRolloutReconciler{client: client}

			isbsvc, controllerInstanceID, err := reconciler.getTargetPipelineDependencies(context.Background(), pipelineRollout)

			require.NoError(t, err)
			if tt.expectedISBSvcName == "" {
				assert.Nil(t, isbsvc)
				assert.Empty(t, controllerInstanceID)
				return
			}
			require.NotNil(t, isbsvc)
			assert.Equal(t, tt.expectedISBSvcName, isbsvc.GetName())
			assert.Equal(t, tt.expectedControllerID, controllerInstanceID)
		})
	}
}
