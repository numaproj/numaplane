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
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
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

func TestISBServiceAssessResourceChainIncludesControllerWorkloads(t *testing.T) {
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
	controllerRollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
		Status: apiv1.NumaflowControllerRolloutStatus{
			ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
				UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     "controller-1",
						ResourceAssessmentResult: apiv1.AssessmentResultSuccess,
					},
				},
			},
		},
	}
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
	monoVertex.SetLabels(map[string]string{
		common.LabelKeyUpgradeState: string(common.LabelValueUpgradeTrial),
	})
	monoVertex.SetAnnotations(map[string]string{
		common.AnnotationKeyNumaflowInstanceID: "trial",
	})

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		isbRollout,
		controllerRollout,
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		failedMonoVertex,
		monoVertex,
	).Build()

	result, reasons, err := (&ISBServiceRolloutReconciler{client: c}).AssessResourceChain(ctx, isbRollout, child)
	require.NoError(t, err)
	require.Equal(t, apiv1.AssessmentResultFailure, result)
	require.Contains(t, reasons[0], "MonoVertexRollout mv trial mv-1 resource assessment failed")
}
