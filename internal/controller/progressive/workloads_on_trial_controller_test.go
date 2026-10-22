package progressive

import (
	"context"
	"errors"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func TestWorkloadsOnTrialControllerCountsPromotedChild(t *testing.T) {
	const namespace = "test"

	healthyISB := isbRollout(namespace, "isb", metav1.ConditionTrue)
	unhealthyPipeline := &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline", Namespace: namespace},
		Status: apiv1.PipelineRolloutStatus{
			Status: apiv1.Status{
				Conditions: []metav1.Condition{{
					Type:   string(apiv1.ConditionChildResourceHealthy),
					Status: metav1.ConditionFalse,
				}},
			},
		},
	}
	otherMonoVertex := &apiv1.MonoVertexRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "mv", Namespace: namespace},
	}

	c := fakeClient(t,
		trialControllerRollout(namespace),
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		healthyISB,
		promotedChildOnInstance(numaflowv1.ISBGroupVersionKind, namespace, "isb-0", "isb", "trial"),
		unhealthyPipeline,
		promotedChildOnInstance(numaflowv1.PipelineGroupVersionKind, namespace, "pipeline-0", "pipeline", "trial"),
		otherMonoVertex,
		promotedChildOnInstance(numaflowv1.MonoVertexGroupVersionKind, namespace, "mv-0", "mv", "other"),
	)

	members, reasons, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "", "")
	require.NoError(t, err)
	require.True(t, present)
	require.Empty(t, reasons)
	require.Equal(t, []apiv1.ResourceChainMemberStatus{
		{Kind: "ISBServiceRollout", Name: "isb", ChildName: "isb-0", AssessmentResult: apiv1.AssessmentResultSuccess},
		{Kind: "PipelineRollout", Name: "pipeline", ChildName: "pipeline-0", AssessmentResult: apiv1.AssessmentResultUnknown},
		{Kind: "MonoVertexRollout", Name: "mv", AssessmentResult: apiv1.AssessmentResultUnknown},
	}, members)
}

func TestWorkloadsOnTrialControllerSkipsTheExcludedRollout(t *testing.T) {
	const namespace = "test"
	c := fakeClient(t,
		trialControllerRollout(namespace),
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		isbRollout(namespace, "isb", metav1.ConditionTrue),
		promotedChildOnInstance(numaflowv1.ISBGroupVersionKind, namespace, "isb-0", "isb", "trial"),
	)

	members, reasons, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "ISBServiceRollout", "isb")
	require.NoError(t, err)
	require.True(t, present)
	require.Empty(t, reasons)
	require.Empty(t, members)
}

func TestWorkloadsOnTrialControllerPromotedChildLookupFails(t *testing.T) {
	const namespace = "test"
	base := fakeClient(t,
		trialControllerRollout(namespace),
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		isbRollout(namespace, "isb", metav1.ConditionTrue),
		promotedChildOnInstance(numaflowv1.ISBGroupVersionKind, namespace, "isb-0", "isb", "trial"),
	)
	// OutsideTrialChain lists the promoted child once. The assessment lists it again.
	c := &failNthKindList{Client: base, kind: numaflowv1.ISBGroupVersionKind.Kind, failOn: 2}

	_, _, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "", "")
	require.Error(t, err)
	require.True(t, present)
}

func TestWorkloadsOnTrialControllerUpgradingChildMissing(t *testing.T) {
	const namespace = "test"
	isb := isbRollout(namespace, "isb", metav1.ConditionTrue)
	isb.Status.ProgressiveStatus.UpgradingISBServiceStatus = &apiv1.UpgradingISBServiceStatus{
		UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "isb-1"},
	}
	c := fakeClient(t,
		trialControllerRollout(namespace),
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		isb,
	)

	members, reasons, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "", "")
	require.NoError(t, err)
	require.True(t, present)
	require.Empty(t, reasons)
	require.Equal(t, []apiv1.ResourceChainMemberStatus{
		{Kind: "ISBServiceRollout", Name: "isb", ChildName: "isb-1", AssessmentResult: apiv1.AssessmentResultUnknown},
	}, members)
}

func TestWorkloadsOnTrialControllerUpgradingChildGetFails(t *testing.T) {
	const namespace = "test"
	isb := isbRollout(namespace, "isb", metav1.ConditionTrue)
	isb.Status.ProgressiveStatus.UpgradingISBServiceStatus = &apiv1.UpgradingISBServiceStatus{
		UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "isb-1"},
	}
	c := failGetClient{Client: fakeClient(t,
		trialControllerRollout(namespace),
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
		isb,
	)}

	_, _, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "", "")
	require.Error(t, err)
	require.True(t, present)
}

func TestWorkloadsOnTrialControllerListingFails(t *testing.T) {
	const namespace = "test"
	withTrial := fakeClient(t,
		trialControllerRollout(namespace),
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
	)

	testCases := []struct {
		name    string
		c       client.Client
		present bool
	}{
		{
			name:    "controller rollouts",
			c:       failTypedList{Client: fakeClient(t), listType: "*v1alpha1.NumaflowControllerRolloutList"},
			present: false,
		},
		{
			name:    "ISBService rollouts",
			c:       failTypedList{Client: withTrial, listType: "*v1alpha1.ISBServiceRolloutList"},
			present: true,
		},
		{
			name:    "pipeline rollouts",
			c:       failTypedList{Client: withTrial, listType: "*v1alpha1.PipelineRolloutList"},
			present: true,
		},
		{
			name:    "monovertex rollouts",
			c:       failTypedList{Client: withTrial, listType: "*v1alpha1.MonoVertexRolloutList"},
			present: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, present, err := WorkloadsOnTrialController(context.Background(), tc.c, namespace, "", "")
			require.Error(t, err)
			require.Equal(t, tc.present, present)
		})
	}
}

func TestWorkloadsOnTrialControllerRejectsMultipleControllers(t *testing.T) {
	const namespace = "test"
	c := fakeClient(t,
		trialControllerRollout(namespace),
		&apiv1.NumaflowControllerRollout{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: namespace}},
	)

	_, _, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "", "")
	require.Error(t, err)
	require.False(t, present)
}

func TestWorkloadsOnTrialControllerRejectsNonStringInstanceID(t *testing.T) {
	const namespace = "test"
	c := badInstanceIDClient{Client: fakeClient(t, trialControllerRollout(namespace))}

	_, _, present, err := WorkloadsOnTrialController(context.Background(), c, namespace, "", "")
	require.Error(t, err)
	require.True(t, present)
}

func TestTrialControllerResourceAssessmentWithoutMatchingUpgradeStatus(t *testing.T) {
	const namespace = "test"
	rollout := trialControllerRollout(namespace)
	rollout.Status.ProgressiveStatus.UpgradingNumaflowControllerStatus = &apiv1.UpgradingNumaflowControllerStatus{
		UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "controller-0"},
	}
	c := fakeClient(t,
		rollout,
		ctlrcommon.CreateTestNumaflowController(namespace, "controller", "controller-1", common.LabelValueUpgradeTrial, "trial"),
	)

	member, inChain, reason, err := TrialControllerResourceAssessment(context.Background(), c, namespace)
	require.NoError(t, err)
	require.True(t, inChain)
	require.Contains(t, reason, "has no resource assessment yet")
	require.Equal(t, apiv1.AssessmentResultUnknown, member.AssessmentResult)
	require.Equal(t, "controller-1", member.ChildName)
}

func fakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, numaflowv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func trialControllerRollout(namespace string) *apiv1.NumaflowControllerRollout {
	return &apiv1.NumaflowControllerRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: namespace},
	}
}

func isbRollout(namespace, name string, healthy metav1.ConditionStatus) *apiv1.ISBServiceRollout {
	return &apiv1.ISBServiceRollout{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status: apiv1.ISBServiceRolloutStatus{
			Status: apiv1.Status{
				Conditions: []metav1.Condition{{
					Type:   string(apiv1.ConditionChildResourceHealthy),
					Status: healthy,
				}},
			},
		},
	}
}

func promotedChildOnInstance(gvk schema.GroupVersionKind, namespace, name, parent, instanceID string) *unstructured.Unstructured {
	child := promotedChild(gvk, namespace, name, parent, name)
	child.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: instanceID})
	return child
}

// failNthKindList fails the failOn-th List of an unstructured collection of kind.
type failNthKindList struct {
	client.Client
	kind   string
	failOn int
	seen   int
}

func (f *failNthKindList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if unstructuredList, ok := list.(*unstructured.UnstructuredList); ok && unstructuredList.GetKind() == f.kind {
		f.seen++
		if f.seen == f.failOn {
			return errors.New("list failed")
		}
	}
	return f.Client.List(ctx, list, opts...)
}

type failTypedList struct {
	client.Client
	listType string
}

func (f failTypedList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if typeName(list) == f.listType {
		return errors.New("list failed")
	}
	return f.Client.List(ctx, list, opts...)
}

// badInstanceIDClient returns a trial controller whose spec.instanceID is not a string.
type badInstanceIDClient struct {
	client.Client
}

func (c badInstanceIDClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if unstructuredList, ok := list.(*unstructured.UnstructuredList); ok && unstructuredList.GetKind() == "NumaflowController" {
		child := unstructured.Unstructured{Object: map[string]interface{}{
			"spec": map[string]interface{}{"instanceID": true},
		}}
		child.SetName("controller-1")
		child.SetNamespace("test")
		unstructuredList.Items = []unstructured.Unstructured{child}
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

type failGetClient struct {
	client.Client
}

func (f failGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return errors.New("get failed")
}

func typeName(list client.ObjectList) string {
	switch list.(type) {
	case *apiv1.NumaflowControllerRolloutList:
		return "*v1alpha1.NumaflowControllerRolloutList"
	case *apiv1.ISBServiceRolloutList:
		return "*v1alpha1.ISBServiceRolloutList"
	case *apiv1.PipelineRolloutList:
		return "*v1alpha1.PipelineRolloutList"
	case *apiv1.MonoVertexRolloutList:
		return "*v1alpha1.MonoVertexRolloutList"
	default:
		return ""
	}
}
