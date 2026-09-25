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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestGetTargetPipelineDependenciesConsistencyGating(t *testing.T) {
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
		return &numaflowv1.InterStepBufferService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					common.LabelKeyParentRollout: isbsvcRolloutName,
					common.LabelKeyUpgradeState:  string(state),
				},
				Annotations: map[string]string{
					common.AnnotationKeyNumaflowInstanceID: instanceID,
				},
			},
		}
	}

	tests := []struct {
		name                  string
		hasTrialController    bool
		trialISBSvcInstanceID string
		expectedISBSvcName    string
		expectedControllerID  string
	}{
		{
			name:                  "retains promoted pair while controller trial is ahead",
			hasTrialController:    true,
			trialISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:    "default-0",
			expectedControllerID:  promotedInstanceID,
		},
		{
			name:                  "selects trial ISBService with promoted controller for ISBService-only upgrade",
			hasTrialController:    false,
			trialISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:    "default-1",
			expectedControllerID:  promotedInstanceID,
		},
		{
			name:                  "selects trial pair after both lookups agree",
			hasTrialController:    true,
			trialISBSvcInstanceID: trialInstanceID,
			expectedISBSvcName:    "default-1",
			expectedControllerID:  trialInstanceID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controllerInstances := []apiv1.ControllerInstanceRef{
				{InstanceID: promotedInstanceID, State: string(common.LabelValueUpgradePromoted)},
			}
			if tt.hasTrialController {
				controllerInstances = append(controllerInstances, apiv1.ControllerInstanceRef{
					InstanceID: trialInstanceID,
					State:      string(common.LabelValueUpgradeTrial),
				})
			}
			controllerRollout := &apiv1.NumaflowControllerRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
				Status: apiv1.NumaflowControllerRolloutStatus{
					ControllerInstances: controllerInstances,
				},
			}

			scheme := runtime.NewScheme()
			require.NoError(t, apiv1.AddToScheme(scheme))
			require.NoError(t, numaflowv1.AddToScheme(scheme))
			client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
				isbsvcRollout,
				controllerRollout,
				newISBSvc("default-0", common.LabelValueUpgradePromoted, promotedInstanceID),
				newISBSvc("default-1", common.LabelValueUpgradeTrial, tt.trialISBSvcInstanceID),
			).Build()
			reconciler := &PipelineRolloutReconciler{client: client}

			isbsvc, controllerInstanceID, err := reconciler.getTargetPipelineDependencies(context.Background(), pipelineRollout)

			require.NoError(t, err)
			require.NotNil(t, isbsvc)
			assert.Equal(t, tt.expectedISBSvcName, isbsvc.GetName())
			assert.Equal(t, tt.expectedControllerID, controllerInstanceID)
		})
	}
}
