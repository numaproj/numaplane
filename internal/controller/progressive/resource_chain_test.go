package progressive

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
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
	results, reasons := AppendChainMember(nil, nil, apiv1.AssessmentResultSuccess, false, "ignored")
	assert.Empty(t, results)
	assert.Empty(t, reasons)

	results, reasons = AppendChainMember(results, reasons, apiv1.AssessmentResultFailure, true, "controller failed")
	assert.Equal(t, []apiv1.AssessmentResult{apiv1.AssessmentResultFailure}, results)
	assert.Equal(t, []string{"controller failed"}, reasons)
}
