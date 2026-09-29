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
		name                     string
		hasTrialController       bool
		hasTrialISBSvc           bool
		trialISBSvcInstanceID    string
		promotedISBSvcInstanceID string
		expectedISBSvcName       string
		expectedControllerID     string
	}{
		{
			name:                     "selects trial ISBService with its own controller while controller trial is ahead",
			hasTrialController:       true,
			hasTrialISBSvc:           true,
			trialISBSvcInstanceID:    promotedInstanceID,
			promotedISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:       "default-1",
			expectedControllerID:     promotedInstanceID,
		},
		{
			name:                     "selects trial ISBService with promoted controller for ISBService-only upgrade",
			hasTrialController:       false,
			hasTrialISBSvc:           true,
			trialISBSvcInstanceID:    promotedInstanceID,
			promotedISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:       "default-1",
			expectedControllerID:     promotedInstanceID,
		},
		{
			name:                     "selects trial pair after both lookups agree",
			hasTrialController:       true,
			hasTrialISBSvc:           true,
			trialISBSvcInstanceID:    trialInstanceID,
			promotedISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:       "default-1",
			expectedControllerID:     trialInstanceID,
		},
		{
			name:                     "follows trial ISBService controller after the controller trial is gone",
			hasTrialController:       false,
			hasTrialISBSvc:           true,
			trialISBSvcInstanceID:    trialInstanceID,
			promotedISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:       "default-1",
			expectedControllerID:     trialInstanceID,
		},
		{
			name:                     "selects promoted pair while controller trial has no trial ISBService yet",
			hasTrialController:       true,
			hasTrialISBSvc:           false,
			promotedISBSvcInstanceID: promotedInstanceID,
			expectedISBSvcName:       "default-0",
			expectedControllerID:     promotedInstanceID,
		},
		{
			name:                     "resolves nothing when promoted ISBService is not on the promoted controller",
			hasTrialController:       true,
			hasTrialISBSvc:           false,
			promotedISBSvcInstanceID: trialInstanceID,
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
			objects := []client.Object{
				isbsvcRollout,
				controllerRollout,
				newISBSvc("default-0", common.LabelValueUpgradePromoted, tt.promotedISBSvcInstanceID),
			}
			if tt.hasTrialISBSvc {
				objects = append(objects, newISBSvc("default-1", common.LabelValueUpgradeTrial, tt.trialISBSvcInstanceID))
			}
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

func TestGetTargetPipelineDependenciesWithoutControllerBinding(t *testing.T) {
	const (
		namespace         = "test"
		isbsvcRolloutName = "default"
		rolloutInstanceID = "rollout-supplied"
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
	newISBSvc := func(name string, state common.UpgradeState) *numaflowv1.InterStepBufferService {
		return &numaflowv1.InterStepBufferService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels: map[string]string{
					common.LabelKeyParentRollout: isbsvcRolloutName,
					common.LabelKeyUpgradeState:  string(state),
				},
				Annotations: map[string]string{
					common.AnnotationKeyNumaflowInstanceID: rolloutInstanceID,
				},
			},
		}
	}
	unsetController := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
	}

	tests := []struct {
		name               string
		objects            []client.Object
		expectedISBSvcName string
	}{
		{
			name: "uses promoted ISBService effective ID when no controller rollout exists",
			objects: []client.Object{
				isbsvcRollout,
				newISBSvc("default-0", common.LabelValueUpgradePromoted),
			},
			expectedISBSvcName: "default-0",
		},
		{
			name: "uses trial ISBService effective ID for an ISBService-only upgrade",
			objects: []client.Object{
				isbsvcRollout,
				newISBSvc("default-0", common.LabelValueUpgradePromoted),
				newISBSvc("default-1", common.LabelValueUpgradeTrial),
			},
			expectedISBSvcName: "default-1",
		},
		{
			name: "uses promoted ISBService effective ID when controller instanceID is unset",
			objects: []client.Object{
				isbsvcRollout,
				unsetController,
				newISBSvc("default-0", common.LabelValueUpgradePromoted),
			},
			expectedISBSvcName: "default-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, apiv1.AddToScheme(scheme))
			require.NoError(t, numaflowv1.AddToScheme(scheme))
			client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.objects...).Build()
			reconciler := &PipelineRolloutReconciler{client: client}

			isbsvc, controllerInstanceID, err := reconciler.getTargetPipelineDependencies(context.Background(), pipelineRollout)

			require.NoError(t, err)
			require.NotNil(t, isbsvc)
			assert.Equal(t, tt.expectedISBSvcName, isbsvc.GetName())
			assert.Equal(t, rolloutInstanceID, controllerInstanceID)
		})
	}
}
