package pipelinerollout

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

func TestPipelineAssessResourceChain(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	pipelineChild := &unstructured.Unstructured{Object: map[string]interface{}{
		"spec": map[string]interface{}{"interStepBufferServiceName": "isbsvc-1"},
	}}
	pipelineChild.SetNamespace(namespace)
	pipelineChild.SetName("pipeline-1")

	pipelineRollout := func(own apiv1.AssessmentResult) *apiv1.PipelineRollout {
		return &apiv1.PipelineRollout{
			ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
			Status: apiv1.PipelineRolloutStatus{
				ProgressiveStatus: apiv1.PipelineProgressiveStatus{
					UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
						InterStepBufferServiceName: "isbsvc-1",
						UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{
								Name:                     "pipeline-1",
								ResourceAssessmentResult: own,
							},
						},
					},
				},
			},
		}
	}

	trialController := func(result apiv1.AssessmentResult) []client.Object {
		return []client.Object{
			&apiv1.NumaflowControllerRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
				Status: apiv1.NumaflowControllerRolloutStatus{
					ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
						UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{
								Name:                     "controller-1",
								ResourceAssessmentResult: result,
								FailureReasons:           []string{"crash loop"},
							},
						},
					},
				},
			},
			ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		}
	}

	trialISB := func(result apiv1.AssessmentResult) []client.Object {
		return []client.Object{
			&apiv1.ISBServiceRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
				Status: apiv1.ISBServiceRolloutStatus{
					ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
						UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{
								Name:                     "isbsvc-1",
								ResourceAssessmentResult: result,
								FailureReasons:           []string{"phase Failed"},
							},
						},
					},
				},
			},
			&numaflowv1.InterStepBufferService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "isbsvc-1",
					Namespace: namespace,
					Labels: map[string]string{
						common.LabelKeyParentRollout: "isb",
						common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
					},
				},
			},
		}
	}

	siblingOnTrial := func(isbsvcName, instanceID string) []client.Object {
		return []client.Object{
			&apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "sibling", Namespace: namespace},
				Status: apiv1.PipelineRolloutStatus{
					ProgressiveStatus: apiv1.PipelineProgressiveStatus{
						UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
							InterStepBufferServiceName: isbsvcName,
							UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
								UpgradingChildStatus: apiv1.UpgradingChildStatus{
									Name:                     "sibling-1",
									ResourceAssessmentResult: apiv1.AssessmentResultFailure,
								},
							},
						},
					},
				},
			},
			&numaflowv1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sibling-1",
					Namespace: namespace,
					Labels: map[string]string{
						common.LabelKeyParentRollout: "sibling",
						common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
					},
					Annotations: map[string]string{
						common.AnnotationKeyNumaflowInstanceID: instanceID,
					},
				},
			},
		}
	}

	testCases := []struct {
		name            string
		own             apiv1.AssessmentResult
		objects         []client.Object
		expected        apiv1.AssessmentResult
		expectReasonSub string
	}{
		{
			name:     "pipeline alone succeeds",
			own:      apiv1.AssessmentResultSuccess,
			expected: apiv1.AssessmentResultSuccess,
		},
		{
			name:     "healthy trial controller does not block a healthy pipeline",
			own:      apiv1.AssessmentResultSuccess,
			objects:  trialController(apiv1.AssessmentResultSuccess),
			expected: apiv1.AssessmentResultSuccess,
		},
		{
			name:            "unhealthy trial controller blocks a healthy pipeline",
			own:             apiv1.AssessmentResultSuccess,
			objects:         trialController(apiv1.AssessmentResultFailure),
			expected:        apiv1.AssessmentResultFailure,
			expectReasonSub: "NumaflowControllerRollout controller trial controller-1 resource assessment failed",
		},
		{
			name:            "unhealthy trial ISBService blocks a healthy pipeline",
			own:             apiv1.AssessmentResultSuccess,
			objects:         trialISB(apiv1.AssessmentResultFailure),
			expected:        apiv1.AssessmentResultFailure,
			expectReasonSub: "ISBServiceRollout isb trial isbsvc-1 resource assessment failed",
		},
		{
			name:     "pipeline waits while the trial controller is still unknown",
			own:      apiv1.AssessmentResultSuccess,
			objects:  trialController(apiv1.AssessmentResultUnknown),
			expected: apiv1.AssessmentResultUnknown,
		},
		{
			name:            "a failed sibling on the trial controller blocks this pipeline",
			own:             apiv1.AssessmentResultSuccess,
			objects:         append(trialController(apiv1.AssessmentResultSuccess), siblingOnTrial("other-isb", "trial")...),
			expected:        apiv1.AssessmentResultFailure,
			expectReasonSub: "PipelineRollout sibling trial sibling-1 resource assessment failed",
		},
		{
			name:            "a failed sibling on the trial ISBService blocks this pipeline",
			own:             apiv1.AssessmentResultSuccess,
			objects:         append(trialISB(apiv1.AssessmentResultSuccess), siblingOnTrial("isbsvc-1", "promoted")...),
			expected:        apiv1.AssessmentResultFailure,
			expectReasonSub: "PipelineRollout sibling trial sibling-1 resource assessment failed",
		},
		{
			name:     "a failed pipeline on another controller instance does not block this pipeline",
			own:      apiv1.AssessmentResultSuccess,
			objects:  append(trialController(apiv1.AssessmentResultSuccess), siblingOnTrial("other-isb", "other")...),
			expected: apiv1.AssessmentResultSuccess,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, apiv1.AddToScheme(scheme))
			require.NoError(t, numaflowv1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()

			result, reasons, err := (&PipelineRolloutReconciler{client: c}).AssessResourceChain(ctx, pipelineRollout(tc.own), pipelineChild)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
			if tc.expectReasonSub == "" {
				require.Empty(t, reasons)
			} else {
				require.NotEmpty(t, reasons)
				require.Contains(t, reasons[0], tc.expectReasonSub)
			}
		})
	}
}
