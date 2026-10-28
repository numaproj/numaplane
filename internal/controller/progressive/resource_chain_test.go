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

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

const (
	trialISBName    = "isb-1"
	promotedISBName = "isb-0"
)

func TestCombineAssessmentResults(t *testing.T) {
	success := apiv1.AssessmentResultSuccess
	failure := apiv1.AssessmentResultFailure
	unknown := apiv1.AssessmentResultUnknown

	testCases := []struct {
		name     string
		results  []apiv1.AssessmentResult
		expected apiv1.AssessmentResult
	}{
		{name: "empty is unknown", results: nil, expected: unknown},
		{name: "single success", results: []apiv1.AssessmentResult{success}, expected: success},
		{name: "all success", results: []apiv1.AssessmentResult{success, success}, expected: success},
		{name: "unknown blocks success", results: []apiv1.AssessmentResult{success, unknown}, expected: unknown},
		{name: "empty member blocks success", results: []apiv1.AssessmentResult{success, ""}, expected: unknown},
		{name: "failure wins over unknown", results: []apiv1.AssessmentResult{unknown, failure, success}, expected: failure},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CombineAssessmentResults(tc.results))
		})
	}
}

func TestEffectiveResourceAssessment(t *testing.T) {
	withResource := &apiv1.UpgradingChildStatus{
		AssessmentResult:         apiv1.AssessmentResultSuccess,
		ResourceAssessmentResult: apiv1.AssessmentResultFailure,
	}
	assert.Equal(t, apiv1.AssessmentResultFailure, withResource.EffectiveResourceAssessment())

	legacy := &apiv1.UpgradingChildStatus{AssessmentResult: apiv1.AssessmentResultSuccess}
	assert.Equal(t, apiv1.AssessmentResultSuccess, legacy.EffectiveResourceAssessment())
	assert.Equal(t, apiv1.AssessmentResultUnknown, (*apiv1.UpgradingChildStatus)(nil).EffectiveResourceAssessment())
}

func TestAppendChainMember(t *testing.T) {
	failed := NewChainMember("NumaflowControllerRollout", "nc", "nc-1", apiv1.AssessmentResultFailure)
	members, reasons := AppendChainMember(nil, nil, failed, "controller failed")
	assert.Equal(t, []apiv1.ResourceChainMemberStatus{failed}, members)
	assert.Equal(t, []string{"controller failed"}, reasons)

	success := NewChainMember("PipelineRollout", "p", "p-1", apiv1.AssessmentResultSuccess)
	members, reasons = AppendChainMember(members, reasons, success, "")
	assert.Equal(t, []apiv1.ResourceChainMemberStatus{failed, success}, members)
	assert.Equal(t, []string{"controller failed"}, reasons)
}

func TestCombineMemberAssessments(t *testing.T) {
	assert.Equal(t, apiv1.AssessmentResultUnknown, CombineMemberAssessments(nil))
	assert.Equal(t, apiv1.AssessmentResultSuccess, CombineMemberAssessments([]apiv1.ResourceChainMemberStatus{
		NewChainMember("PipelineRollout", "p", "p-1", apiv1.AssessmentResultSuccess),
	}))
	assert.Equal(t, apiv1.AssessmentResultFailure, CombineMemberAssessments([]apiv1.ResourceChainMemberStatus{
		NewChainMember("PipelineRollout", "p", "p-1", apiv1.AssessmentResultSuccess),
		NewChainMember("ISBServiceRollout", "isb", "isb-1", apiv1.AssessmentResultFailure),
	}))
}

func TestPipelinesOnTrialISBCountsPromotedChild(t *testing.T) {
	const namespace = "test"

	testCases := []struct {
		name        string
		healthy     metav1.ConditionStatus
		promotedISB string
		want        apiv1.ResourceChainMemberStatus
	}{
		{
			name:        "healthy promoted child on the trial ISBService",
			healthy:     metav1.ConditionTrue,
			promotedISB: trialISBName,
			want:        apiv1.ResourceChainMemberStatus{Kind: "PipelineRollout", Name: "pipeline", ChildName: "pipeline-0", AssessmentResult: apiv1.AssessmentResultSuccess},
		},
		{
			name:        "unhealthy promoted child on the trial ISBService",
			healthy:     metav1.ConditionFalse,
			promotedISB: trialISBName,
			want:        apiv1.ResourceChainMemberStatus{Kind: "PipelineRollout", Name: "pipeline", ChildName: "pipeline-0", AssessmentResult: apiv1.AssessmentResultUnknown},
		},
		{
			name:        "healthy promoted child still on the promoted ISBService",
			healthy:     metav1.ConditionTrue,
			promotedISB: promotedISBName,
			want:        apiv1.ResourceChainMemberStatus{Kind: "PipelineRollout", Name: "pipeline", AssessmentResult: apiv1.AssessmentResultUnknown},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := fakeClient(t,
				trialChild(numaflowv1.ISBGroupVersionKind, namespace, trialISBName, "isb", trialISBName),
				pipelineRolloutUsingISB(namespace, "pipeline", "isb", tc.healthy),
				promotedPipelineOnISB(namespace, "pipeline-0", "pipeline", tc.promotedISB),
			)

			members, reasons, err := PipelinesOnTrialISB(context.Background(), c, namespace, trialISBName, "")
			require.NoError(t, err)
			require.Empty(t, reasons)
			require.Equal(t, []apiv1.ResourceChainMemberStatus{tc.want}, members)
		})
	}
}

func TestPipelinesOnTrialISBPromotedChildLookupFails(t *testing.T) {
	const namespace = "test"
	base := fakeClient(t,
		trialChild(numaflowv1.ISBGroupVersionKind, namespace, trialISBName, "isb", trialISBName),
		pipelineRolloutUsingISB(namespace, "pipeline", "isb", metav1.ConditionTrue),
		promotedPipelineOnISB(namespace, "pipeline-0", "pipeline", trialISBName),
	)
	// OutsideTrialChain lists the promoted child once. The assessment lists it again.
	c := &failNthKindList{Client: base, kind: numaflowv1.PipelineGroupVersionKind.Kind, failOn: 2}

	_, _, err := PipelinesOnTrialISB(context.Background(), c, namespace, trialISBName, "")
	require.Error(t, err)
}

func pipelineRolloutUsingISB(namespace, name, isbRolloutName string, healthy metav1.ConditionStatus) *apiv1.PipelineRollout {
	return &apiv1.PipelineRollout{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: apiv1.PipelineRolloutSpec{
			Pipeline: apiv1.Pipeline{
				Spec: runtime.RawExtension{Raw: []byte(`{"interStepBufferServiceName":"` + isbRolloutName + `"}`)},
			},
		},
		Status: apiv1.PipelineRolloutStatus{
			Status: apiv1.Status{
				Conditions: []metav1.Condition{{
					Type:   string(apiv1.ConditionChildResourceHealthy),
					Status: healthy,
				}},
			},
		},
	}
}

func promotedPipelineOnISB(namespace, name, parent, isbsvcName string) *unstructured.Unstructured {
	child := promotedChild(numaflowv1.PipelineGroupVersionKind, namespace, name, parent, name)
	child.Object["spec"] = map[string]interface{}{"interStepBufferServiceName": isbsvcName}
	return child
}
