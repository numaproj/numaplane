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

package util

import (
	"reflect"
	"testing"
)

func TestMergeMaps(t *testing.T) {
	tests := []struct {
		oldMap      map[string]string
		newMap      map[string]string
		expectedMap map[string]string
	}{
		{nil, map[string]string{"numaflow.numaproj.io/instance": "0"}, map[string]string{"numaflow.numaproj.io/instance": "0"}},                                                                                 // initial map is empty
		{map[string]string{"numaflow.numaproj.io/instance": "0"}, map[string]string{"numaflow.numaproj.io/instance": "1"}, map[string]string{"numaflow.numaproj.io/instance": "1"}},                             // maps have only same keys
		{map[string]string{"numaflow.numaproj.io/instance": "0", "foo": "bar"}, map[string]string{"numaflow.numaproj.io/instance": "1"}, map[string]string{"numaflow.numaproj.io/instance": "1", "foo": "bar"}}, // initial map has key that new map doesn't update
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			resultMap := MergeMaps(tt.oldMap, tt.newMap)
			if !reflect.DeepEqual(resultMap, tt.expectedMap) {
				t.Errorf("expected map: %v, got: %v", tt.expectedMap, resultMap)
			}
		})
	}
}

func TestCompareMaps(t *testing.T) {
	tests := []struct {
		mapA           map[string]string
		mapB           map[string]string
		expectedResult bool
	}{
		{nil, map[string]string{"numaflow.numaproj.io/instance": "0"}, false},
		{map[string]string{"numaflow.numaproj.io/instance": "0"}, map[string]string{"numaflow.numaproj.io/instance": "1"}, false},
		{nil, map[string]string{}, true},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := CompareMaps(tt.mapA, tt.mapB)
			if result != tt.expectedResult {
				t.Errorf("expected result: %v, got: %v", tt.expectedResult, result)
			}
		})
	}
}
