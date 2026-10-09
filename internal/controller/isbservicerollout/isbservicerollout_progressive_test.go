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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestAssessISBServiceHealth(t *testing.T) {
	healthyConditions := []metav1.Condition{
		{Type: string(numaflowv1.ISBSvcConditionConfigured), Status: metav1.ConditionTrue, Reason: "Successful"},
		{Type: string(numaflowv1.ISBSvcConditionDeployed), Status: metav1.ConditionTrue, Reason: "Successful"},
		{Type: string(numaflowv1.ISBSvcConditionChildrenResourcesHealthy), Status: metav1.ConditionTrue, Reason: "RolloutFinished"},
	}

	tests := []struct {
		name       string
		isbsvc     *numaflowv1.InterStepBufferService
		wantResult apiv1.AssessmentResult
		wantReason string
	}{
		{
			name:       "running, reconciled, and all conditions true",
			isbsvc:     testISBService(numaflowv1.ISBSvcPhaseRunning, "", 1, 1, healthyConditions),
			wantResult: apiv1.AssessmentResultSuccess,
		},
		{
			name:       "pending is not yet a failure",
			isbsvc:     testISBService(numaflowv1.ISBSvcPhasePending, "", 1, 1, nil),
			wantResult: apiv1.AssessmentResultUnknown,
		},
		{
			name:       "not reconciled yet",
			isbsvc:     testISBService(numaflowv1.ISBSvcPhaseRunning, "", 2, 1, healthyConditions),
			wantResult: apiv1.AssessmentResultUnknown,
		},
		{
			name:       "progressing children stay unknown",
			isbsvc:     testISBService(numaflowv1.ISBSvcPhaseRunning, "", 1, 1, []metav1.Condition{{Type: string(numaflowv1.ISBSvcConditionChildrenResourcesHealthy), Status: metav1.ConditionFalse, Reason: "Progressing", Message: "rollout in progress"}}),
			wantResult: apiv1.AssessmentResultUnknown,
		},
		{
			name:       "failed phase",
			isbsvc:     testISBService(numaflowv1.ISBSvcPhaseFailed, "jetstream image not found", 1, 1, nil),
			wantResult: apiv1.AssessmentResultFailure,
			wantReason: "InterstepBufferService phase is Failed: jetstream image not found",
		},
		{
			name:       "unhealthy children",
			isbsvc:     testISBService(numaflowv1.ISBSvcPhaseRunning, "", 1, 1, []metav1.Condition{{Type: string(numaflowv1.ISBSvcConditionChildrenResourcesHealthy), Status: metav1.ConditionFalse, Reason: "StatefulSetFailed", Message: "pod crash"}}),
			wantResult: apiv1.AssessmentResultFailure,
			wantReason: `condition ChildrenResourcesHealthy is False for Reason "StatefulSetFailed": pod crash`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, reasons, err := assessISBServiceHealth(toUnstructured(t, tc.isbsvc))
			require.NoError(t, err)
			assert.Equal(t, tc.wantResult, result)
			if tc.wantReason == "" {
				assert.Empty(t, reasons)
			} else {
				require.Len(t, reasons, 1)
				assert.Equal(t, tc.wantReason, reasons[0])
			}
		})
	}
}

func TestAssessUpgradingChildRequiresISBServiceHealthAndPipelines(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	openWindow := config.AssessmentSchedule{End: time.Hour, Period: time.Minute, Interval: 10 * time.Second}

	t.Run("healthy once is not enough until the consecutive period elapses", func(t *testing.T) {
		rollout := upgradingISBServiceRollout(now, nil, "")
		result, _, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, healthyISBService()), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultUnknown, result)
		require.NotNil(t, rollout.GetUpgradingChildStatus().TrialWindowStartTime)
		assert.Empty(t, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("consecutive healthy period promotes when there are no pipelines", func(t *testing.T) {
		trialStart := now.Add(-time.Minute)
		rollout := upgradingISBServiceRollout(now.Add(-2*time.Minute), &trialStart, "")
		result, _, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, healthyISBService()), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultSuccess, result)
		assert.Equal(t, apiv1.AssessmentResultSuccess, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
		assert.NotNil(t, rollout.GetUpgradingChildStatus().BasicAssessmentEndTime)
	})

	t.Run("a non-success observation restarts the consecutive healthy period", func(t *testing.T) {
		trialStart := now.Add(-time.Minute)
		rollout := upgradingISBServiceRollout(now, &trialStart, "")
		pending := testISBService(numaflowv1.ISBSvcPhasePending, "", 1, 1, nil)
		result, _, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, pending), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultUnknown, result)
		assert.Nil(t, rollout.GetUpgradingChildStatus().TrialWindowStartTime)
		assert.Empty(t, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("a failed phase retries inside the window", func(t *testing.T) {
		trialStart := now.Add(-10 * time.Second)
		rollout := upgradingISBServiceRollout(now, &trialStart, "")
		failed := testISBService(numaflowv1.ISBSvcPhaseFailed, "boom", 1, 1, nil)
		result, _, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, failed), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultUnknown, result)
		assert.Nil(t, rollout.GetUpgradingChildStatus().TrialWindowStartTime)
		assert.Equal(t, []string{"InterstepBufferService phase is Failed: boom"}, rollout.GetUpgradingChildStatus().FailureReasons)
		assert.NotEmpty(t, rollout.GetUpgradingChildStatus().ChildStatus.Raw)
	})

	t.Run("an end of zero fails once the assessment start time is past", func(t *testing.T) {
		rollout := upgradingISBServiceRollout(now.Add(-time.Second), nil, "")
		schedule := config.AssessmentSchedule{End: 0, Period: 0, Interval: 10 * time.Second}
		result, _, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, healthyISBService()), schedule)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultFailure, result)
		assert.Equal(t, apiv1.AssessmentResultFailure, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("a failed InterstepBufferService fails the upgrade when the window ends", func(t *testing.T) {
		rollout := upgradingISBServiceRollout(now.Add(-2*time.Hour), nil, "")
		failed := testISBService(numaflowv1.ISBSvcPhaseFailed, "jetstream image not found", 1, 1, nil)
		schedule := config.AssessmentSchedule{End: time.Minute, Period: time.Minute, Interval: 10 * time.Second}
		result, message, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, failed), schedule)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultFailure, result)
		assert.Equal(t, "Basic Resource Health Check failed", message)
		assert.Equal(t, apiv1.AssessmentResultFailure, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("the window expiring without sustained health fails the upgrade", func(t *testing.T) {
		rollout := upgradingISBServiceRollout(now.Add(-2*time.Hour), nil, "")
		pending := testISBService(numaflowv1.ISBSvcPhasePending, "", 1, 0, nil)
		schedule := config.AssessmentSchedule{End: time.Minute, Period: time.Minute, Interval: 10 * time.Second}
		result, message, err := newAssessmentReconciler().AssessUpgradingChild(ctx, rollout, toUnstructured(t, pending), schedule)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultFailure, result)
		assert.Equal(t, "Basic Resource Health Check failed", message)
		assert.Equal(t, apiv1.AssessmentResultFailure, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("a failed pipeline fails the upgrade before the health window finishes", func(t *testing.T) {
		rollout := upgradingISBServiceRollout(now, nil, "")
		pending := testISBService(numaflowv1.ISBSvcPhasePending, "", 1, 0, nil)
		pipeline := pipelineRolloutUsingISBService("default", "default-1", "trial-pipeline", apiv1.AssessmentResultFailure)
		result, _, err := newAssessmentReconciler(pipeline).AssessUpgradingChild(ctx, rollout, toUnstructured(t, pending), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultFailure, result)
		assert.Equal(t, []string{"Pipeline trial-pipeline failed"}, rollout.GetUpgradingChildStatus().FailureReasons)
		assert.Empty(t, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("pipelines that have not moved onto the new isbsvc keep the result unknown", func(t *testing.T) {
		trialStart := now.Add(-time.Minute)
		rollout := upgradingISBServiceRollout(now.Add(-2*time.Minute), &trialStart, "")
		pipeline := pipelineRolloutUsingISBService("default", "some-other-isbsvc", "trial-pipeline", apiv1.AssessmentResultSuccess)
		result, _, err := newAssessmentReconciler(pipeline).AssessUpgradingChild(ctx, rollout, toUnstructured(t, healthyISBService()), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultUnknown, result)
		assert.Equal(t, apiv1.AssessmentResultSuccess, rollout.GetUpgradingChildStatus().BasicAssessmentResult)
	})

	t.Run("after the isbsvc health window succeeds, pipeline success promotes", func(t *testing.T) {
		rollout := upgradingISBServiceRollout(now.Add(-time.Hour), nil, apiv1.AssessmentResultSuccess)
		pipeline := pipelineRolloutUsingISBService("default", "default-1", "trial-pipeline", apiv1.AssessmentResultSuccess)
		result, _, err := newAssessmentReconciler(pipeline).AssessUpgradingChild(ctx, rollout, toUnstructured(t, healthyISBService()), openWindow)
		require.NoError(t, err)
		assert.Equal(t, apiv1.AssessmentResultSuccess, result)
		assert.Empty(t, rollout.GetUpgradingChildStatus().FailureReasons)
	})
}

func newAssessmentReconciler(objects ...client.Object) *ISBServiceRolloutReconciler {
	scheme := runtime.NewScheme()
	_ = apiv1.AddToScheme(scheme)
	return &ISBServiceRolloutReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
	}
}

func upgradingISBServiceRollout(assessmentStart time.Time, trialStart *time.Time, basicResult apiv1.AssessmentResult) *apiv1.ISBServiceRollout {
	return &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "test"},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     "default-1",
						BasicAssessmentStartTime: &metav1.Time{Time: assessmentStart},
						TrialWindowStartTime:     timePtr(trialStart),
						BasicAssessmentResult:    basicResult,
					},
				},
			},
		},
	}
}

func timePtr(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	mt := metav1.NewTime(*t)
	return &mt
}

func healthyISBService() *numaflowv1.InterStepBufferService {
	return testISBService(numaflowv1.ISBSvcPhaseRunning, "", 1, 1, []metav1.Condition{
		{Type: string(numaflowv1.ISBSvcConditionConfigured), Status: metav1.ConditionTrue, Reason: "Successful"},
		{Type: string(numaflowv1.ISBSvcConditionDeployed), Status: metav1.ConditionTrue, Reason: "Successful"},
		{Type: string(numaflowv1.ISBSvcConditionChildrenResourcesHealthy), Status: metav1.ConditionTrue, Reason: "RolloutFinished"},
	})
}

func testISBService(phase numaflowv1.ISBSvcPhase, message string, generation, observedGeneration int64, conditions []metav1.Condition) *numaflowv1.InterStepBufferService {
	return &numaflowv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "default-1",
			Namespace:  "test",
			Generation: generation,
			Labels: map[string]string{
				common.LabelKeyParentRollout: "default",
			},
		},
		Status: numaflowv1.InterStepBufferServiceStatus{
			Status:             numaflowv1.Status{Conditions: conditions},
			Phase:              phase,
			Message:            message,
			ObservedGeneration: observedGeneration,
		},
	}
}

func toUnstructured(t *testing.T, isbsvc *numaflowv1.InterStepBufferService) *unstructured.Unstructured {
	t.Helper()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(isbsvc)
	require.NoError(t, err)
	return &unstructured.Unstructured{Object: obj}
}

func pipelineRolloutUsingISBService(rolloutName, childName, pipelineName string, result apiv1.AssessmentResult) *apiv1.PipelineRollout {
	spec, _ := json.Marshal(map[string]string{"interStepBufferServiceName": rolloutName})
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: "test"},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{Raw: spec},
			},
		},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:             pipelineName,
							AssessmentResult: result,
						},
					},
					InterStepBufferServiceName: childName,
				},
			},
		},
	}
}
