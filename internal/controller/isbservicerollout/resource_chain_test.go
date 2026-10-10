package isbservicerollout

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

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
