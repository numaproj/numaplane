package util

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

var inputJson string = `
{
	"mapKey": 
	{
		"empty": null,
		"scalarInt": 5,
		"scalarString": "blah",
		"emptyMap": {},
		"emptyArray": [],
		"nonEmptyArray":
		[
			"a",
			"b"
		],
		"arrayOfEmptyMaps":
		[
			{
				"emptyMap1": {}
			},
			{
				"emptyMap2": {}
			}
		],
		"nestedMapKeep":
		{
			"innerMapKeep":
			{
				"a": "b"
			},
			"innerMapDelete": {}
		},
		"nestedMapDelete":
		{
			"innerMapDelete": {}
		},
		"stringKeyWithEmptyValue": "",
		"intKeyWithZeroValue": 0,
		"boolKeyWithZeroValue": false
	}

}
`

var outputJson string = `
{
	"mapKey": 
	{
		"scalarInt": 5,
		"scalarString": "blah",
		"nonEmptyArray":
		[
			"a",
			"b"
		],
		"nestedMapKeep":
		{
			"innerMapKeep":
			{
				"a": "b"
			}
		}
	}
}

`

func Test_removeNullValuesFromMap(t *testing.T) {
	inputMap := make(map[string]interface{})
	_ = json.Unmarshal([]byte(inputJson), &inputMap)
	removeNullValuesFromJSONMap(inputMap)

	outputMap := make(map[string]interface{})
	_ = json.Unmarshal([]byte(outputJson), &outputMap)
	assert.True(t, reflect.DeepEqual(inputMap, outputMap))
}

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
	"age":      30,
	"lastname": nil,
	"name":     "John",
	"primArr":  []any{1, 2, 3, 4, 5, 6},
	"projects": []msa{
		{"name": "Project2", "nothing": nil, "other": []msa{{"x": "x2"}, {"y": "y2"}}, "status": "completed", "vals": []any{1, 2, 3}},
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
		expectedOnlyPaths    msa
		expectedWithoutPaths msa
	}{
		{
			name:                 "simple map - empty paths slice",
			inputMap:             simpleMap,
			paths:                []string{},
			expectedOnlyPaths:    msa{},
			expectedWithoutPaths: simpleMap,
		},
		{
			name:                 "simple map - nil paths slice",
			inputMap:             simpleMap,
			paths:                nil,
			expectedOnlyPaths:    msa{},
			expectedWithoutPaths: simpleMap,
		},
		{
			name:                 "simple map - single path - level 3",
			inputMap:             simpleMap,
			paths:                []string{"map.field.inner"},
			expectedOnlyPaths:    msa{"map": msa{"field": msa{"inner": 123}}},
			expectedWithoutPaths: msa{"map": msa{"field": msa{"inner2": "inner2val"}, "field2": msa{"x": 324}}},
		},
		{
			name:                 "simple map - single path - level 2",
			inputMap:             simpleMap,
			paths:                []string{"map.field"},
			expectedOnlyPaths:    msa{"map": msa{"field": msa{"inner": 123, "inner2": "inner2val"}}},
			expectedWithoutPaths: msa{"map": msa{"field2": msa{"x": 324}}},
		},
		{
			name:                 "simple map - single path - level 1",
			inputMap:             simpleMap,
			paths:                []string{"map"},
			expectedOnlyPaths:    simpleMap,
			expectedWithoutPaths: msa{},
		},
		{
			name:                 "simple map - multiple paths - root",
			inputMap:             simpleMap,
			paths:                []string{"map.field.inner2", "map", "map.field"},
			expectedOnlyPaths:    simpleMap,
			expectedWithoutPaths: msa{},
		},
		{
			name:                 "simple map - multiple paths - level 1",
			inputMap:             simpleMap,
			paths:                []string{"map.field.inner2", "map.field"},
			expectedOnlyPaths:    msa{"map": msa{"field": msa{"inner": 123, "inner2": "inner2val"}}},
			expectedWithoutPaths: msa{"map": msa{"field2": msa{"x": 324}}},
		},
		{
			name:     "complex map - multiple paths - level 1",
			inputMap: complexMap,
			paths:    []string{"name", "age", "address"},
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
					{"name": "Project2", "other": []msa{{"y": "y2"}}, "status": "completed", "vals": []any{1, 2, 3}},
					{"name": "Project3", "other": []msa{{"y": "y3"}}},
					{"name": "Project4"},
					{"name": "Project1", "other": []msa{{"y": "y1"}}, "status": "in progress", "vals": []any{4, 5, 6, 7}},
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
					{"other": []msa{{"x": "x2"}}},
					{"other": []msa{{"z": "z4"}}},
					{"other": []msa{{"x": "x1"}, {"w": "w1"}, {"t": "t1"}, {"z": "z1"}}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitMap(tc.inputMap, tc.paths, pathSeparator)
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
