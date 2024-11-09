package common

import (
	"fmt"
	"testing"
)

func TestGetRolloutParentName(t *testing.T) {
	tests := []struct {
		childName      string
		expectedParent string
		expectError    bool
	}{
		{"parent-123", "parent", false}, // Valid case
		{"parent-child", "", true},      // Invalid case: no number
		{"parent-", "", true},           // Edge case: hyphen without number
		{"parent-0", "parent", false},   // Valid case: zero as number
		{"parent-abc", "", true},        // Invalid case: non-numeric suffix
		{"-123", "", true},              // Invalid case: no parent name
		{"parent-123-extra", "", true},  // Invalid case: extra suffix
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("childName=%s", tt.childName), func(t *testing.T) {
			parentName, err := GetRolloutParentName(tt.childName)
			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
			if parentName != tt.expectedParent {
				t.Errorf("expected parent name: %q, got: %q", tt.expectedParent, parentName)
			}
		})
	}
}
