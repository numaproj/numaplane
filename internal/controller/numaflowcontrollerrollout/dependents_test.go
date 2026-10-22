package numaflowcontrollerrollout

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
	"github.com/numaproj/numaplane/internal/controller/progressive"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestAssessDependentsFailsBeforeAllDependentsStart(t *testing.T) {
	const namespace = "test"

	failedMonoVertex := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "mv", Namespace: namespace},
		Status: apiv1.MonoVertexRolloutStatus{
			ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
				UpgradingMonoVertexStatus: &apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "mv-1",
							ResourceAssessmentResult: apiv1.AssessmentResultFailure,
						},
					},
				},
			},
		},
	}
	monoVertex := &unstructured.Unstructured{Object: map[string]interface{}{}}
	monoVertex.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)
	monoVertex.SetNamespace(namespace)
	monoVertex.SetName("mv-1")
	monoVertex.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "trial"})

	notStartedPipeline := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
	}
	// An existing Pipeline that has not started its trial yet: it has a promoted child.
	promotedPipeline := &unstructured.Unstructured{Object: map[string]interface{}{}}
	promotedPipeline.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	promotedPipeline.SetNamespace(namespace)
	promotedPipeline.SetName("pipeline-0")
	promotedPipeline.SetLabels(map[string]string{
		common.LabelKeyParentRollout: "pipeline",
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
	})

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(failedMonoVertex, monoVertex, notStartedPipeline, promotedPipeline).Build()

	members, reasons, err := (&NumaflowControllerRolloutReconciler{client: c}).assessDependents(context.Background(), namespace, "trial")
	require.NoError(t, err)
	require.Equal(t, apiv1.AssessmentResultFailure, progressive.CombineMemberAssessments(members))
	require.Equal(t, []string{"MonoVertexRollout mv failed"}, reasons)
	require.Equal(t, []apiv1.ResourceChainMemberStatus{
		{Kind: "PipelineRollout", Name: "pipeline", AssessmentResult: apiv1.AssessmentResultUnknown},
		{Kind: "MonoVertexRollout", Name: "mv", ChildName: "mv-1", AssessmentResult: apiv1.AssessmentResultFailure},
	}, members)
}

func TestAssessDependentsCountsPromotedChildAlreadyOnTrialInstance(t *testing.T) {
	const namespace = "test"

	healthyISB := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			Status: apiv1.Status{
				Conditions: []metav1.Condition{{
					Type:   string(apiv1.ConditionChildResourceHealthy),
					Status: metav1.ConditionTrue,
				}},
			},
		},
	}
	promotedISB := &unstructured.Unstructured{Object: map[string]interface{}{}}
	promotedISB.SetGroupVersionKind(numaflowv1.ISBGroupVersionKind)
	promotedISB.SetNamespace(namespace)
	promotedISB.SetName("isb-0")
	promotedISB.SetLabels(map[string]string{
		common.LabelKeyParentRollout: "isb",
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
	})
	promotedISB.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "trial"})

	notOnTrialPipeline := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
	}
	promotedPipeline := &unstructured.Unstructured{Object: map[string]interface{}{}}
	promotedPipeline.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	promotedPipeline.SetNamespace(namespace)
	promotedPipeline.SetName("pipeline-0")
	promotedPipeline.SetLabels(map[string]string{
		common.LabelKeyParentRollout: "pipeline",
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
	})
	promotedPipeline.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "promoted"})

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(healthyISB, promotedISB, notOnTrialPipeline, promotedPipeline).Build()

	members, reasons, err := (&NumaflowControllerRolloutReconciler{client: c}).assessDependents(context.Background(), namespace, "trial")
	require.NoError(t, err)
	require.Empty(t, reasons)
	require.Equal(t, apiv1.AssessmentResultUnknown, progressive.CombineMemberAssessments(members))
	require.Equal(t, []apiv1.ResourceChainMemberStatus{
		{Kind: "ISBServiceRollout", Name: "isb", ChildName: "isb-0", AssessmentResult: apiv1.AssessmentResultSuccess},
		{Kind: "PipelineRollout", Name: "pipeline", AssessmentResult: apiv1.AssessmentResultUnknown},
	}, members)
}
