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

	for _, tc := range []struct {
		name        string
		strategy    apiv1.UpgradeStrategy
		chainResult apiv1.AssessmentResult
		resultState common.ResultState
		expected    bool
	}{
		{name: "progressive upgrade with a failed chain", strategy: apiv1.UpgradeStrategyProgressive, chainResult: apiv1.AssessmentResultFailure, expected: true},
		{name: "progressive upgrade with a successful chain", strategy: apiv1.UpgradeStrategyProgressive, chainResult: apiv1.AssessmentResultSuccess, expected: false},
		{name: "chain still being assessed", strategy: apiv1.UpgradeStrategyProgressive, chainResult: apiv1.AssessmentResultUnknown, expected: false},
		{name: "chain reassessed after a failed member was replaced", strategy: apiv1.UpgradeStrategyProgressive, chainResult: apiv1.AssessmentResultUnknown, resultState: common.LabelValueResultStateFailed, expected: true},
		{name: "no progressive upgrade in progress", strategy: apiv1.UpgradeStrategyNoOp, chainResult: apiv1.AssessmentResultFailure, expected: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rollout := &apiv1.NumaflowControllerRollout{
				ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
				Status: apiv1.NumaflowControllerRolloutStatus{
					Status: apiv1.Status{UpgradeInProgress: tc.strategy},
					ProgressiveStatus: apiv1.NumaflowControllerProgressiveStatus{
						UpgradingNumaflowControllerStatus: &apiv1.UpgradingNumaflowControllerStatus{
							UpgradingChildStatus: apiv1.UpgradingChildStatus{
								Name:                          "controller-1",
								ResourceAssessmentResult:      apiv1.AssessmentResultSuccess,
								ResourceChainAssessmentResult: tc.chainResult,
							},
						},
					},
				},
			}
			trial := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial")
			if tc.resultState != "" {
				trial.Labels[common.LabelKeyProgressiveResultState] = string(tc.resultState)
			}
			c := newChainTestClient(t, rollout, trial)

			failed, err := ControllerChainFailed(ctx, c, namespace)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, failed)
		})
	}
}

func TestOutsideTrialChain(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()
	rollout := &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace}}

	t.Run("discontinued trial", func(t *testing.T) {
		outside, err := OutsideTrialChain(ctx, newChainTestClient(t), rollout, &apiv1.UpgradingChildStatus{Name: "pipeline-1", Discontinued: true})
		require.NoError(t, err)
		assert.True(t, outside)
	})
	t.Run("current trial", func(t *testing.T) {
		outside, err := OutsideTrialChain(ctx, newChainTestClient(t), rollout, &apiv1.UpgradingChildStatus{Name: "pipeline-1"})
		require.NoError(t, err)
		assert.False(t, outside)
	})
	t.Run("new rollout held Pending", func(t *testing.T) {
		outside, err := OutsideTrialChain(ctx, newChainTestClient(t), rollout, nil)
		require.NoError(t, err)
		assert.True(t, outside)
	})
	t.Run("existing rollout that has not started its trial", func(t *testing.T) {
		promoted := promotedChild(numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-0", "pipeline", "uid-pipeline")
		outside, err := OutsideTrialChain(ctx, newChainTestClient(t, promoted), rollout, nil)
		require.NoError(t, err)
		assert.False(t, outside)
	})
}

func TestAnnotateCurrentTrialChildrenAnnotatesSuccessfulChainMembers(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	isbTrial := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-1", "isb", "uid-isb")
	pipelineTrial := trialChild(numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", "pipeline", "uid-pipeline")
	monoVertexTrial := trialChild(numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", "monovertex", "uid-mv")
	// A member's recorded child that has already been promoted is no longer its trial.
	promotedPipeline := promotedChild(numaflowv1.PipelineGroupVersionKind, namespace, "other-pipeline-0", "other-pipeline", "uid-other")
	c := newChainTestClient(t, isbTrial, pipelineTrial, monoVertexTrial, promotedPipeline)

	require.NoError(t, AnnotateCurrentTrialChildren(ctx, c, namespace, []apiv1.ResourceChainMemberStatus{
		NewChainMember("ISBServiceRollout", "isb", "isb-1", apiv1.AssessmentResultSuccess),
		NewChainMember("PipelineRollout", "pipeline", "pipeline-1", apiv1.AssessmentResultFailure),
		NewChainMember("PipelineRollout", "other-pipeline", "other-pipeline-0", apiv1.AssessmentResultSuccess),
	}))

	got := getChild(t, c, numaflowv1.ISBGroupVersionKind, namespace, "isb-1")
	assert.Equal(t, ChainPromoteAnnotationValue("isb-1", "uid-isb"), got.GetAnnotations()[common.AnnotationKeyChainPromote])
	assert.True(t, ChainPromoteAnnotationMatches(got))
	assertChainPromote(t, c, numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", false)
	assertChainPromote(t, c, numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", false)
	assertChainPromote(t, c, numaflowv1.PipelineGroupVersionKind, namespace, "other-pipeline-0", false)
}

func TestStampForcePromoteChainStampsOnlyChainMembers(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	controllerRollout := &apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace}}
	trialController := ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "new")
	isbTrial := trialChild(numaflowv1.ISBGroupVersionKind, namespace, "isb-1", "isb", "uid-isb")
	isbTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "new"})
	// This Pipeline's trial is still on the older controller, so it is not part of the trial stack.
	pipelineTrial := trialChild(numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", "pipeline", "uid-pipeline")
	pipelineTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "old"})
	// This MonoVertex trial belongs to another chain.
	monoVertexTrial := trialChild(numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", "monovertex", "uid-mv")
	monoVertexTrial.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "new"})
	c := newChainTestClient(t, controllerRollout, trialController, isbTrial, pipelineTrial, monoVertexTrial)

	require.NoError(t, StampForcePromoteChain(ctx, c, namespace, []apiv1.ResourceChainMemberStatus{
		NewChainMember("NumaflowControllerRollout", "controller", "controller-1", apiv1.AssessmentResultSuccess),
		NewChainMember("ISBServiceRollout", "isb", "isb-1", apiv1.AssessmentResultSuccess),
		NewChainMember("PipelineRollout", "pipeline", "pipeline-1", apiv1.AssessmentResultUnknown),
	}))

	assertForcePromoteChain(t, c, apiv1.NumaflowControllerGroupVersionKind, namespace, "controller-1", true)
	assertForcePromoteChain(t, c, numaflowv1.ISBGroupVersionKind, namespace, "isb-1", true)
	assertForcePromoteChain(t, c, numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-1", false)
	assertForcePromoteChain(t, c, numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-1", false)
}

func newChainTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func getChild(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, got))
	return got
}

func assertChainPromote(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string, want bool) {
	t.Helper()
	_, found := getChild(t, c, gvk, namespace, name).GetAnnotations()[common.AnnotationKeyChainPromote]
	assert.Equal(t, want, found)
}

func assertForcePromoteChain(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string, want bool) {
	t.Helper()
	assert.Equal(t, want, getChild(t, c, gvk, namespace, name).GetLabels()[common.LabelKeyForcePromoteChain] == "true")
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

func promotedChild(gvk schema.GroupVersionKind, namespace, name, parent, uid string) *unstructured.Unstructured {
	child := trialChild(gvk, namespace, name, parent, uid)
	child.SetLabels(map[string]string{
		common.LabelKeyParentRollout: parent,
		common.LabelKeyUpgradeState:  string(common.LabelValueUpgradePromoted),
	})
	return child
}
