package util

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type msa = map[string]any

const pathSeparator = "."

var simpleMap = msa{
	"map": msa{
		"field": msa{
			"inner":  123,
			"inner2": "inner2val",
		},
		"field2": msa{
			"x": 324,
		},
	},
}

var complexMap = msa{
	"address": msa{
		"city":    "New York",
		"country": "USA",
		"postal": msa{
			"code":   "10001",
			"region": "NY",
			"something": msa{
				"abc": "123",
			},
		},
	},
	"age":     30,
	"name":    "John",
	"primArr": []any{1, 2, 3, 4, 5, 6},
	"projects": []msa{
		{"name": "Project2", "other": []msa{{"x": "x2"}, {"y": "y2"}}, "status": "completed", "vals": []any{1, 2, 3}},
		{"name": "Project3", "other": []msa{{"y": "y3"}}},
		{"name": "Project4", "other": []msa{{"z": "z4"}}},
		{"name": "Project1", "other": []msa{{"x": "x1"}, {"w": "w1", "y": "y1"}, {"t": "t1"}, {"z": "z1"}}, "status": "in progress", "vals": []any{4, 5, 6, 7}},
	},
}

func compactJSON(t *testing.T, jsonObj []byte) *bytes.Buffer {
	compactedObj := new(bytes.Buffer)
	if err := json.Compact(compactedObj, jsonObj); err != nil {
		assert.NoError(t, err)
	}

	return compactedObj
}

func mapToBytesBuffer(t *testing.T, obj map[string]any) *bytes.Buffer {
	raw, err := json.Marshal(obj)
	assert.NoError(t, err)

	return compactJSON(t, raw)
}

func Test_SplitMap(t *testing.T) {
	testCases := []struct {
		name                 string
		inputMap             msa
		paths                []string
		excludedPaths        []string
		expectedOnlyPaths    msa
		expectedWithoutPaths msa
	}{
		{
			name: "real case",
			inputMap: msa{
				"lifecycle": msa{
					"desiredPhase": "paused",
					"timeout":      123,
				},
				"vertices": []msa{
					{"name": "v1"},
					{"name": "v2"},
				},
			},
			paths:             []string{"lifecycle"},
			excludedPaths:     []string{"lifecycle.desiredPhase"},
			expectedOnlyPaths: msa{"lifecycle": msa{"timeout": 123}},
			expectedWithoutPaths: msa{
				"vertices": []msa{
					{"name": "v1"},
					{"name": "v2"},
				},
			},
		},
		{
			name:                 "simple map - empty paths slice",
			inputMap:             simpleMap,
			paths:                []string{},
			excludedPaths:        nil,
			expectedOnlyPaths:    msa{},
			expectedWithoutPaths: simpleMap,
		},
		{
			name:                 "simple map - nil paths slice",
			inputMap:             simpleMap,
			paths:                nil,
			excludedPaths:        nil,
			expectedOnlyPaths:    msa{},
			expectedWithoutPaths: simpleMap,
		},
		{
			name:                 "simple map - single path - level 3",
			inputMap:             simpleMap,
			paths:                []string{"map.field.inner"},
			excludedPaths:        nil,
			expectedOnlyPaths:    msa{"map": msa{"field": msa{"inner": 123}}},
			expectedWithoutPaths: msa{"map": msa{"field": msa{"inner2": "inner2val"}, "field2": msa{"x": 324}}},
		},
		{
			name:                 "simple map - single path - level 2",
			inputMap:             simpleMap,
			paths:                []string{"map.field"},
			excludedPaths:        nil,
			expectedOnlyPaths:    msa{"map": msa{"field": msa{"inner": 123, "inner2": "inner2val"}}},
			expectedWithoutPaths: msa{"map": msa{"field2": msa{"x": 324}}},
		},
		{
			name:                 "simple map - single path - level 1",
			inputMap:             simpleMap,
			paths:                []string{"map"},
			excludedPaths:        nil,
			expectedOnlyPaths:    simpleMap,
			expectedWithoutPaths: msa{},
		},
		{
			name:                 "simple map - multiple paths - root",
			inputMap:             simpleMap,
			paths:                []string{"map.field.inner2", "map", "map.field"},
			excludedPaths:        nil,
			expectedOnlyPaths:    simpleMap,
			expectedWithoutPaths: msa{},
		},
		{
			name:                 "simple map - multiple paths - level 1",
			inputMap:             simpleMap,
			paths:                []string{"map.field.inner2", "map.field"},
			excludedPaths:        nil,
			expectedOnlyPaths:    msa{"map": msa{"field": msa{"inner": 123, "inner2": "inner2val"}}},
			expectedWithoutPaths: msa{"map": msa{"field2": msa{"x": 324}}},
		},
		{
			name:          "complex map - multiple paths - level 1",
			inputMap:      complexMap,
			paths:         []string{"name", "age", "address"},
			excludedPaths: nil,
			expectedOnlyPaths: msa{
				"address": msa{
					"city":    "New York",
					"country": "USA",
					"postal": msa{
						"code":   "10001",
						"region": "NY",
						"something": msa{
							"abc": "123",
						},
					},
				},
				"age":  30,
				"name": "John",
			},
			expectedWithoutPaths: msa{
				"primArr": []any{1, 2, 3, 4, 5, 6},
				"projects": []msa{
					{"name": "Project2", "other": []msa{{"x": "x2"}, {"y": "y2"}}, "status": "completed", "vals": []any{1, 2, 3}},
					{"name": "Project3", "other": []msa{{"y": "y3"}}},
					{"name": "Project4", "other": []msa{{"z": "z4"}}},
					{"name": "Project1", "other": []msa{{"x": "x1"}, {"w": "w1", "y": "y1"}, {"t": "t1"}, {"z": "z1"}}, "status": "in progress", "vals": []any{4, 5, 6, 7}},
				},
			},
		},
		{
			name:     "complex map - multiple paths - various levels and types",
			inputMap: complexMap,
			paths: []string{"name", "address.city", "address.postal.something", "projects.status", "projects.other.y",
				"address.postal.region", "projects.vals", "projects.name"},
			excludedPaths: nil,
			expectedOnlyPaths: msa{
				"address": msa{
					"city": "New York",
					"postal": msa{
						"region": "NY",
						"something": msa{
							"abc": "123",
						},
					},
				},
				"name": "John",
				"projects": []msa{
					{"name": "Project2", "other": []msa{{}, {"y": "y2"}}, "status": "completed", "vals": []any{1, 2, 3}},
					{"name": "Project3", "other": []msa{{"y": "y3"}}},
					{"name": "Project4", "other": []msa{{}}},
					{"name": "Project1", "other": []msa{{}, {"y": "y1"}, {}, {}}, "status": "in progress", "vals": []any{4, 5, 6, 7}},
				},
			},
			expectedWithoutPaths: msa{
				"address": msa{
					"country": "USA",
					"postal": msa{
						"code": "10001",
					},
				},
				"age":     30,
				"primArr": []any{1, 2, 3, 4, 5, 6},
				"projects": []msa{
					{"other": []msa{{"x": "x2"}, {}}},
					{"other": []msa{{}}},
					{"other": []msa{{"z": "z4"}}},
					{"other": []msa{{"x": "x1"}, {"w": "w1"}, {"t": "t1"}, {"z": "z1"}}},
				},
			},
		},
		{
			name:     "complex map - multiple paths - various levels, types, and excluded paths",
			inputMap: complexMap,
			paths: []string{"name", "address.city", "address.postal.something", "projects.status", "projects.other.y",
				"address.postal.region", "projects.vals", "projects.name"},
			excludedPaths: []string{"address.city", "projects.other.w"},
			expectedOnlyPaths: msa{
				"address": msa{
					"postal": msa{
						"region": "NY",
						"something": msa{
							"abc": "123",
						},
					},
				},
				"name": "John",
				"projects": []msa{
					{"name": "Project2", "other": []msa{{}, {"y": "y2"}}, "status": "completed", "vals": []any{1, 2, 3}},
					{"name": "Project3", "other": []msa{{"y": "y3"}}},
					{"name": "Project4", "other": []msa{{}}},
					{"name": "Project1", "other": []msa{{}, {"y": "y1"}, {}, {}}, "status": "in progress", "vals": []any{4, 5, 6, 7}},
				},
			},
			expectedWithoutPaths: msa{
				"address": msa{
					"country": "USA",
					"postal": msa{
						"code": "10001",
					},
				},
				"age":     30,
				"primArr": []any{1, 2, 3, 4, 5, 6},
				"projects": []msa{
					{"other": []msa{{"x": "x2"}, {}}},
					{"other": []msa{{}}},
					{"other": []msa{{"z": "z4"}}},
					{"other": []msa{{"x": "x1"}, {}, {"t": "t1"}, {"z": "z1"}}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitMap(tc.inputMap, tc.paths, tc.excludedPaths, pathSeparator)
			assert.NoError(t, err)

			actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
			actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

			expectedOnlyPaths := mapToBytesBuffer(t, tc.expectedOnlyPaths)
			expectedWithoutPaths := mapToBytesBuffer(t, tc.expectedWithoutPaths)

			assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
			assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
		})
	}
}
