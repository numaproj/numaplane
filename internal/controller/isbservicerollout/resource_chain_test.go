package isbservicerollout

import (
	"context"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestAssessISBServiceHealth(t *testing.T) {
	isbsvc := func(phase numaflowv1.ISBSvcPhase, condition *metav1.Condition) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetName("isbsvc-1")
		obj.SetNamespace("test")
		require.NoError(t, unstructured.SetNestedField(obj.Object, string(phase), "status", "phase"))
		if condition != nil {
			require.NoError(t, unstructured.SetNestedSlice(obj.Object, []interface{}{
				map[string]interface{}{
					"type":   condition.Type,
					"status": string(condition.Status),
					"reason": condition.Reason,
				},
			}, "status", "conditions"))
		}
		return obj
	}

	t.Run("running and healthy", func(t *testing.T) {
		result, reasons, err := assessISBServiceHealth(isbsvc(numaflowv1.ISBSvcPhaseRunning, nil))
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultSuccess, result)
		require.Empty(t, reasons)
	})

	t.Run("failed phase", func(t *testing.T) {
		result, reasons, err := assessISBServiceHealth(isbsvc(numaflowv1.ISBSvcPhaseFailed, nil))
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultFailure, result)
		require.NotEmpty(t, reasons)
	})

	t.Run("pending is unknown", func(t *testing.T) {
		result, _, err := assessISBServiceHealth(isbsvc(numaflowv1.ISBSvcPhasePending, nil))
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultUnknown, result)
	})

	t.Run("progressing children stay unknown", func(t *testing.T) {
		result, _, err := assessISBServiceHealth(isbsvc(numaflowv1.ISBSvcPhaseRunning, &metav1.Condition{
			Type:   "ChildrenResourcesHealthy",
			Status: metav1.ConditionFalse,
			Reason: "Progressing",
		}))
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultUnknown, result)
	})

	t.Run("unhealthy children fail", func(t *testing.T) {
		result, reasons, err := assessISBServiceHealth(isbsvc(numaflowv1.ISBSvcPhaseRunning, &metav1.Condition{
			Type:   "ChildrenResourcesHealthy",
			Status: metav1.ConditionFalse,
			Reason: "StatefulSetFailed",
		}))
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultFailure, result)
		require.NotEmpty(t, reasons)
	})
}

func TestISBServiceAssessResourceChain(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	child := &unstructured.Unstructured{Object: map[string]interface{}{}}
	child.SetNamespace(namespace)
	child.SetName("isbsvc-1")
	child.SetLabels(map[string]string{common.LabelKeyParentRollout: "isb"})

	isbRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     "isbsvc-1",
						ResourceAssessmentResult: apiv1.AssessmentResultSuccess,
					},
				},
			},
		},
	}
	failedPipeline := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{Raw: []byte(`{"interStepBufferServiceName":"isb"}`)},
			},
		},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					InterStepBufferServiceName: "isbsvc-1",
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "pipeline-1",
							ResourceAssessmentResult: apiv1.AssessmentResultFailure,
							// A successful promotion decision must not hide the failed resource assessment.
							AssessmentResult: apiv1.AssessmentResultSuccess,
						},
					},
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(isbRollout, failedPipeline).Build()

	result, reasons, err := (&ISBServiceRolloutReconciler{client: c}).AssessResourceChain(ctx, isbRollout, child)
	require.NoError(t, err)
	require.Equal(t, apiv1.AssessmentResultFailure, result)
	require.Contains(t, reasons, "Pipeline pipeline-1 failed")
}
