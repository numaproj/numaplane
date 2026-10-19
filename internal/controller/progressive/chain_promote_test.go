package progressive

import (
	"context"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestChainPromoteAnnotationMatchesExactChild(t *testing.T) {
	child := &unstructured.Unstructured{}
	child.SetName("isb-1")
	child.SetUID("uid-1")
	child.SetAnnotations(map[string]string{
		common.AnnotationKeyChainPromote: ChainPromoteAnnotationValue("isb-1", "uid-1"),
	})
	assert.True(t, ChainPromoteAnnotationMatches(child))
	assert.True(t, ChainPromoteApplies(child, "isb-1"))
	assert.False(t, ChainPromoteApplies(child, "isb-2"))

	replaced := child.DeepCopy()
	replaced.SetUID("uid-2")
	assert.False(t, ChainPromoteAnnotationMatches(replaced))
}

func TestControllerChainFailed(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	rollout := &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
		Status: apiv1.NumaflowControllerRolloutStatus{
			ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
				UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                          "controller-1",
						ResourceAssessmentResult:      apiv1.AssessmentResultSuccess,
						ResourceChainAssessmentResult: apiv1.AssessmentResultFailure,
					},
				},
			},
		},
	}
	trial := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial")

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rollout, trial).Build()

	failed, err := ControllerChainFailed(ctx, c, namespace)
	require.NoError(t, err)
	assert.True(t, failed)

	rollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus.ResourceChainAssessmentResult = apiv1.AssessmentResultSuccess
	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(rollout, trial).Build()
	failed, err = ControllerChainFailed(ctx, c, namespace)
	require.NoError(t, err)
	assert.False(t, failed)
}

func TestStoredTrialResourceAssessmentsSkipsDiscontinuedPipeline(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

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
	isbRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     "isb-1",
						ResourceAssessmentResult: apiv1.AssessmentResultSuccess,
					},
				},
			},
		},
	}
	// The reverted Pipeline is discontinued, so it is not a current trial member.
	pipelineRollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                     "pipeline-1",
							ResourceAssessmentResult: apiv1.AssessmentResultFailure,
							Discontinued:             true,
						},
					},
				},
			},
		},
	}
	trialController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial")
	trialISB := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-1", "isb", "uid-isb")

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerRollout, isbRollout, pipelineRollout, trialController, trialISB).Build()

	results, err := StoredTrialResourceAssessments(ctx, c, namespace, "ISBServiceRollout", "isb", apiv1.AssessmentResultSuccess)
	require.NoError(t, err)
	assert.Equal(t, apiv1.AssessmentResultSuccess, CombineAssessmentResults(results))
	assert.NotContains(t, results, apiv1.AssessmentResultFailure)
}

func TestStoredTrialResourceAssessmentsKeepsFailure(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

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
	monoVertexRollout := &apiv1.MonoVertexRollout{
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
	trialController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial")
	trialMV := trialChild(numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", "mv", "uid-mv")

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerRollout, monoVertexRollout, trialController, trialMV).Build()

	results, err := StoredTrialResourceAssessments(ctx, c, namespace, "NumaflowControllerRollout", "controller", apiv1.AssessmentResultSuccess)
	require.NoError(t, err)
	assert.Equal(t, apiv1.AssessmentResultFailure, CombineAssessmentResults(results))
}

func TestAnnotateCurrentTrialChildrenRecordsNameAndUID(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	isbRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     "isb-1",
						ResourceAssessmentResult: apiv1.AssessmentResultSuccess,
					},
				},
			},
		},
	}
	trialISB := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-1", "isb", "uid-isb")

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(isbRollout, trialISB).Build()

	require.NoError(t, AnnotateCurrentTrialChildren(ctx, c, namespace, nil))

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(numaflowv1.ISBGroupVersionKind)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "isb-1"}, got))
	assert.Equal(t, ChainPromoteAnnotationValue("isb-1", "uid-isb"), got.GetAnnotations()[common.AnnotationKeyChainPromote])
	assert.True(t, ChainPromoteAnnotationMatches(got))
}

func TestAnnotateCurrentTrialChildrenSkipsOutsidersAndFailures(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	isbRollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: apiv1.UpgradingChildStatus{
						Name:                     "isb-1",
						ResourceAssessmentResult: apiv1.AssessmentResultSuccess,
					},
				},
			},
		},
	}
	pipelineRollout := &apiv1.PipelineRollout{
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
	monoVertexRollout := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "monovertex", Namespace: namespace},
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
	isbTrial := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-1", "isb", "uid-isb")
	pipelineTrial := trialChild(numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", "pipeline", "uid-pipeline")
	monoVertexTrial := trialChild(numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", "monovertex", "uid-mv")

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(isbRollout, pipelineRollout, monoVertexRollout, isbTrial, pipelineTrial, monoVertexTrial).Build()

	require.NoError(t, AnnotateCurrentTrialChildren(ctx, c, namespace, []apiv1.ResourceChainMemberStatus{
		{Kind: "ISBServiceRollout", Name: "isb", AssessmentResult: apiv1.AssessmentResultSuccess},
		{Kind: "PipelineRollout", Name: "pipeline", AssessmentResult: apiv1.AssessmentResultFailure},
	}))

	assertChainPromote(t, c, numaflowv1.ISBGroupVersionKind, namespace, "isb-1", true)
	assertChainPromote(t, c, numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", false)
	assertChainPromote(t, c, numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", false)
}

func TestCurrentTrialMembersBlocksPromotedChildOnOlderController(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	for _, tc := range []struct {
		name           string
		instanceID     string
		expectISBBlock bool
	}{
		{name: "promoted child is on an older controller", instanceID: "old", expectISBBlock: true},
		{name: "promoted child is already on the trial controller", instanceID: "new", expectISBBlock: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controllerRollout := &apiv1.NumaflowControllerRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
				Status: apiv1.NumaflowControllerRolloutStatus{
					ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
						UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "controller-1"},
						},
					},
				},
			}
			trialController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "new")
			isbRollout := &apiv1.ISBServiceRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "isb", Namespace: namespace},
				Status: apiv1.ISBServiceRolloutStatus{
					ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
						UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "isb-0"},
						},
					},
				},
			}
			promoted := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-0", "isb", "uid-promoted")
			promoted.SetLabels(map[string]string{
				common.LabelKeyParentRollout: "isb",
				common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
			})
			promoted.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: tc.instanceID})

			scheme := runtime.NewScheme()
			require.NoError(t, apiv1.AddToScheme(scheme))
			require.NoError(t, numaflowv1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerRollout, trialController, isbRollout, promoted).Build()

			members, err := currentTrialMembers(ctx, c, namespace)
			require.NoError(t, err)
			var isb *trialMember
			for i := range members {
				if members[i].Kind == "ISBServiceRollout" {
					isb = &members[i]
				}
			}
			if tc.expectISBBlock {
				require.NotNil(t, isb)
				assert.Equal(t, apiv1.AssessmentResultUnknown, isb.Assessment)
				assert.Nil(t, isb.Child)
			} else {
				assert.Nil(t, isb)
			}
		})
	}
}

func assertChainPromote(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string, want bool) {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, got))
	_, found := got.GetAnnotations()[common.AnnotationKeyChainPromote]
	assert.Equal(t, want, found)
}

func TestRemoveTrialDependentsOnInstanceDiscontinuesRollouts(t *testing.T) {
	const (
		namespace = "test"
		instance  = "trial-controller"
	)
	ctx := context.Background()

	isbRollout := failedISBServiceRollout(namespace, "isb", "isb-1", "isb-0")
	pipelineRollout := failedPipelineRollout(namespace, "pipeline", "pipeline-1")
	monoVertexRollout := failedMonoVertexRollout(namespace, "monovertex", "mv-1")
	otherRollout := failedISBServiceRollout(namespace, "other-isb", "other-isb-1", "other-isb-0")

	isbTrial := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-1", "isb", "uid-isb")
	isbTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: instance})
	isbPromoted := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-0", "isb", "uid-isb-promoted")
	isbPromoted.SetLabels(map[string]string{
		common.LabelKeyParentRollout: "isb",
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
	})
	isbPromoted.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "promoted-controller"})

	pipelineTrial := trialChild(numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", "pipeline", "uid-pipeline")
	pipelineTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: instance})
	monoVertexTrial := trialChild(numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", "monovertex", "uid-mv")
	monoVertexTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: instance})
	otherTrial := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "other-isb-1", "other-isb", "uid-other")
	otherTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "other-instance"})

	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(isbRollout, pipelineRollout, monoVertexRollout, otherRollout).
		WithObjects(isbRollout, pipelineRollout, monoVertexRollout, otherRollout, isbTrial, isbPromoted, pipelineTrial, monoVertexTrial, otherTrial).
		Build()

	require.NoError(t, RemoveTrialDependentsOnInstance(ctx, c, namespace, instance))

	assertUpgradeState(t, c, numaflowv1.ISBGroupVersionKind, namespace, "isb-1", common.LabelValueUpgradeRecyclable, common.LabelValueDiscontinueProgressive)
	assertUpgradeState(t, c, numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", common.LabelValueUpgradeRecyclable, common.LabelValueDiscontinueProgressive)
	assertUpgradeState(t, c, numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", common.LabelValueUpgradeRecyclable, common.LabelValueDiscontinueProgressive)
	assertUpgradeState(t, c, numaflowv1.ISBGroupVersionKind, namespace, "isb-0", common.LabelValueUpgradePromoted, "")
	assertUpgradeState(t, c, numaflowv1.ISBGroupVersionKind, namespace, "other-isb-1", common.LabelValueUpgradeTrial, "")

	assertDiscontinued(t, c, isbRollout)
	assertDiscontinued(t, c, pipelineRollout)
	assertDiscontinued(t, c, monoVertexRollout)

	untouched := &apiv1.ISBServiceRollout{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: otherRollout.Name}, untouched))
	condition := untouched.Status.GetCondition(apiv1.ConditionProgressiveUpgradeSucceeded)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.False(t, untouched.Status.ProgressiveStatus.UpgradingISBServiceStatus.Discontinued)
}

func failedISBServiceRollout(namespace, name, upgradingChild, promotedChild string) *apiv1.ISBServiceRollout {
	rollout := &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Generation: 4},
		Status: apiv1.ISBServiceRolloutStatus{
			ProgressiveStatus: apiv1.ISBServiceProgressiveStatus{
				UpgradingISBServiceStatus: &apiv1.UpgradingISBServiceStatus{
					UpgradingChildStatus: failedUpgradingChild(upgradingChild),
				},
				PromotedISBServiceStatus: &apiv1.PromotedISBServiceStatus{
					PromotedChildStatus: apiv1.PromotedChildStatus{Name: promotedChild},
				},
			},
		},
	}
	rollout.Status.MarkProgressiveUpgradeFailed("progressive upgrade failed", rollout.Generation)
	return rollout
}

func failedPipelineRollout(namespace, name, upgradingChild string) *apiv1.PipelineRollout {
	rollout := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Generation: 4},
		Status: apiv1.PipelineRolloutStatus{
			ProgressiveStatus: apiv1.PipelineProgressiveStatus{
				UpgradingPipelineStatus: &apiv1.UpgradingPipelineStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: failedUpgradingChild(upgradingChild),
					},
				},
			},
		},
	}
	rollout.Status.MarkProgressiveUpgradeFailed("progressive upgrade failed", rollout.Generation)
	return rollout
}

func failedMonoVertexRollout(namespace, name, upgradingChild string) *apiv1.MonoVertexRollout {
	rollout := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Generation: 4},
		Status: apiv1.MonoVertexRolloutStatus{
			ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
				UpgradingMonoVertexStatus: &apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: failedUpgradingChild(upgradingChild),
					},
				},
			},
		},
	}
	rollout.Status.MarkProgressiveUpgradeFailed("progressive upgrade failed", rollout.Generation)
	return rollout
}

func failedUpgradingChild(name string) apiv1.UpgradingChildStatus {
	return apiv1.UpgradingChildStatus{
		Name:                          name,
		AssessmentResult:              apiv1.AssessmentResultFailure,
		ResourceAssessmentResult:      apiv1.AssessmentResultFailure,
		ResourceChainAssessmentResult: apiv1.AssessmentResultFailure,
		InitializationComplete:        true,
	}
}

func assertUpgradeState(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string, state common.UpgradeState, reason common.UpgradeStateReason) {
	t.Helper()
	child := &unstructured.Unstructured{}
	child.SetGroupVersionKind(gvk)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, child))
	assert.Equal(t, string(state), child.GetLabels()[common.LabelKeyUpgradeState])
	assert.Equal(t, string(reason), child.GetLabels()[common.LabelKeyUpgradeStateReason])
}

func assertDiscontinued(t *testing.T, c client.Client, rollout client.Object) {
	t.Helper()
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: rollout.GetNamespace(), Name: rollout.GetName()}, rollout))
	progressive, ok := rollout.(ProgressiveRolloutObject)
	require.True(t, ok)
	upgrading := progressive.GetUpgradingChildStatus()
	require.NotNil(t, upgrading)
	assert.True(t, upgrading.Discontinued)
	condition := progressive.GetRolloutStatus().GetCondition(apiv1.ConditionProgressiveUpgradeSucceeded)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, "Succeeded", condition.Reason)
	assert.Equal(t, "Discontinued", condition.Message)
	assert.Equal(t, apiv1.PhaseDeployed, progressive.GetRolloutStatus().Phase)
}

func trialChild(gvk schema.GroupVersionKind, namespace, name, parent, uid string) *unstructured.Unstructured {
	child := &unstructured.Unstructured{}
	child.SetGroupVersionKind(gvk)
	child.SetNamespace(namespace)
	child.SetName(name)
	child.SetUID(types.UID(uid))
	child.SetLabels(map[string]string{
		common.LabelKeyParentRollout: parent,
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradeTrial),
	})
	return child
}
