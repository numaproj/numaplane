package monovertexrollout

import (
	"context"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestMonoVertexAssessResourceChain(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	child := &unstructured.Unstructured{}
	child.SetNamespace(namespace)
	child.SetName("mv-1")

	mvRollout := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "mv", Namespace: namespace},
		Status: apiv1.MonoVertexRolloutStatus{
			ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
				UpgradingMonoVertexStatus: &apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "mv-1",
							ResourceAssessmentResult: apiv1.AssessmentResultSuccess,
						},
					},
				},
			},
		},
	}

	newClient := func(objects ...client.Object) client.Client {
		scheme := runtime.NewScheme()
		require.NoError(t, apiv1.AddToScheme(scheme))
		require.NoError(t, numaflowv1.AddToScheme(scheme))
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	}

	t.Run("healthy monovertex with no trial controller succeeds", func(t *testing.T) {
		result, reasons, err := (&MonoVertexRolloutReconciler{client: newClient()}).AssessResourceChain(ctx, mvRollout, child)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultSuccess, result)
		require.Empty(t, reasons)
	})

	t.Run("unhealthy trial controller blocks a healthy monovertex", func(t *testing.T) {
		controller := &apiv1.NumaflowControllerRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
			Status: apiv1.NumaflowControllerRolloutStatus{
				ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
					UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "controller-1",
							ResourceAssessmentResult: apiv1.AssessmentResultFailure,
						},
					},
				},
			},
		}
		trial := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial")

		result, reasons, err := (&MonoVertexRolloutReconciler{client: newClient(controller, trial)}).AssessResourceChain(ctx, mvRollout, child)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultFailure, result)
		require.NotEmpty(t, reasons)
	})

	t.Run("failed pipeline on the trial controller blocks a healthy monovertex", func(t *testing.T) {
		controller := &apiv1.NumaflowControllerRollout{
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
		trial := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial")
		failedPipeline := &apiv1.PipelineRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
			Status: apiv1.PipelineRolloutStatus{
				ProgressiveStatus: apiv1.PipelineProgressiveStatus{
					UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
						UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{
								Name:                     "pipeline-1",
								ResourceAssessmentResult: apiv1.AssessmentResultFailure,
							},
						},
					},
				},
			},
		}
		pipeline := &numaflowv1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pipeline-1",
				Namespace: namespace,
				Labels: map[string]string{
					common.LabelKeyParentRollout: "pipeline",
					common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
				},
				Annotations: map[string]string{
					common.AnnotationKeyNumaflowInstanceID: "trial",
				},
			},
		}

		result, reasons, err := (&MonoVertexRolloutReconciler{client: newClient(controller, trial, failedPipeline, pipeline)}).AssessResourceChain(ctx, mvRollout, child)
		require.NoError(t, err)
		require.Equal(t, apiv1.AssessmentResultFailure, result)
		require.NotEmpty(t, reasons)
	})
}
