/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
