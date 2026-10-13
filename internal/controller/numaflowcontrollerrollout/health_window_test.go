package numaflowcontrollerrollout

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/numaproj/numaplane/internal/controller/config"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestAssessNumaflowControllerHealthWindow(t *testing.T) {
	now := time.Now()
	schedule := config.AssessmentSchedule{End: 3 * time.Minute}

	failed := mustUnstructuredController(t, apiv1.PhaseFailed, nil)
	healthy := mustUnstructuredController(t, apiv1.PhaseDeployed, healthyChildResourcesCondition())

	t.Run("failure inside the window stays unknown", func(t *testing.T) {
		status := &apiv1.UpgradingChildStatus{BasicAssessmentStartTime: &metav1.Time{Time: now.Add(-time.Minute)}}
		result, _, err := assessNumaflowControllerHealthWindow(context.Background(), status, failed, schedule, now)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultUnknown, result)
		require.NotEmpty(t, status.FailureReasons)
	})

	t.Run("failure after the window fails the resource", func(t *testing.T) {
		status := &apiv1.UpgradingChildStatus{BasicAssessmentStartTime: &metav1.Time{Time: now.Add(-4 * time.Minute)}}
		result, reason, err := assessNumaflowControllerHealthWindow(context.Background(), status, failed, schedule, now)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultFailure, result)
		require.NotEmpty(t, reason)
	})

	t.Run("first healthy observation succeeds when the period is zero", func(t *testing.T) {
		status := &apiv1.UpgradingChildStatus{BasicAssessmentStartTime: &metav1.Time{Time: now.Add(-time.Minute)}}
		result, _, err := assessNumaflowControllerHealthWindow(context.Background(), status, healthy, schedule, now)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultSuccess, result)
		require.Equal(t, apiv1.AssessmentResultSuccess, status.BasicAssessmentResult)
	})

	t.Run("an unknown observation fails once the window has ended", func(t *testing.T) {
		starting := mustUnstructuredController(t, apiv1.PhaseDeployed, nil)
		status := &apiv1.UpgradingChildStatus{BasicAssessmentStartTime: &metav1.Time{Time: now.Add(-4 * time.Minute)}}
		result, _, err := assessNumaflowControllerHealthWindow(context.Background(), status, starting, schedule, now)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultFailure, result)
		require.Equal(t, apiv1.AssessmentResultFailure, status.BasicAssessmentResult)
	})

	t.Run("a recorded success is not assessed again", func(t *testing.T) {
		rollout := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "test"},
			Status: apiv1.NumaflowControllerRolloutStatus{
				ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
					UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                  "controller-1",
							BasicAssessmentResult: apiv1.AssessmentResultSuccess,
						},
					},
				},
			},
		}
		result, _, err := (&NumaflowControllerRolloutReconciler{}).AssessUpgradingChild(context.Background(), rollout, failed, schedule)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultSuccess, result)
	})
}

func mustUnstructuredController(t *testing.T, phase apiv1.Phase, conditions []metav1.Condition) *unstructured.Unstructured {
	t.Helper()
	controller := &apiv1.NumaflowController{
		ObjectMeta: metav1.ObjectMeta{Name: "controller-1", Namespace: "test", Generation: 1},
		Status: apiv1.NumaflowControllerStatus{
			Status: apiv1.Status{
				Phase:              phase,
				Conditions:         conditions,
				ObservedGeneration: 1,
			},
		},
	}
	raw, err := json.Marshal(controller)
	require.NoError(t, err)
	obj := &unstructured.Unstructured{}
	require.NoError(t, json.Unmarshal(raw, &obj.Object))
	return obj
}
