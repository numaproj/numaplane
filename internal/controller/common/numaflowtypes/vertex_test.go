package numaflowtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func Test_JsonStringToScaleDef(t *testing.T) {
	int64Ptr := func(v int64) *int64 { return &v }

	testCases := []struct {
		name        string
		jsonString  string
		expected    *apiv1.ScaleDefinition
		expectError bool
	}{
		{
			name:        "null string returns empty ScaleDefinition",
			jsonString:  "null",
			expected:    nil,
			expectError: false,
		},
		{
			name:       "scale with min and max",
			jsonString: `{"min": 1, "max": 10}`,
			expected: &apiv1.ScaleDefinition{
				Min: int64Ptr(1),
				Max: int64Ptr(10),
			},
			expectError: false,
		},
		{
			name:       "scale with only min",
			jsonString: `{"min": 2}`,
			expected: &apiv1.ScaleDefinition{
				Min: int64Ptr(2),
			},
			expectError: false,
		},
		{
			name:       "scale with only max",
			jsonString: `{"max": 5}`,
			expected: &apiv1.ScaleDefinition{
				Max: int64Ptr(5),
			},
			expectError: false,
		},
		{
			name:       "scale with disabled true",
			jsonString: `{"disabled": true}`,
			expected: &apiv1.ScaleDefinition{
				Disabled: true,
			},
			expectError: false,
		},
		{
			name:       "scale with all fields",
			jsonString: `{"min": 0, "max": 100, "disabled": true}`,
			expected: &apiv1.ScaleDefinition{
				Min:      int64Ptr(0),
				Max:      int64Ptr(100),
				Disabled: true,
			},
			expectError: false,
		},
		{
			name:        "empty object returns empty ScaleDefinition",
			jsonString:  `{}`,
			expected:    &apiv1.ScaleDefinition{},
			expectError: false,
		},
		{
			name:        "invalid JSON returns error",
			jsonString:  `{invalid}`,
			expected:    nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := JsonStringToScaleDef(tc.jsonString)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
