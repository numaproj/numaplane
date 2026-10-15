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

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(failedMonoVertex, monoVertex, notStartedPipeline).Build()

	result, failed, err := (&NumaflowControllerRolloutReconciler{client: c}).assessDependents(context.Background(), namespace, "trial")
	require.NoError(t, err)
	require.Equal(t, apiv1.AssessmentResultFailure, result)
	require.Equal(t, []string{"MonoVertexRollout mv"}, failed)
}
