package util

import (
	"bytes"
	"encoding/json"
	"reflect"
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

func Test_removeNullValuesFromMap(t *testing.T) {
	testCases := []struct {
		name     string
		input    msa
		expected msa
	}{
		{
			name:     "nested map with empty map",
			input:    msa{"foo": msa{"bar": msa{}, "baz": "qux"}, "hello": "world"},
			expected: msa{"foo": msa{"baz": "qux"}, "hello": "world"},
		},
		{
			name:     "array with nil elements",
			input:    msa{"foo": []any{nil, "bar", nil, "baz"}, "hello": "world"},
			expected: msa{"foo": []any{"bar", "baz"}, "hello": "world"},
		},
		{
			name:     "array with empty element map",
			input:    msa{"foo": []any{"bar", msa{}, "baz", nil}, "hello": "world"},
			expected: msa{"foo": []any{"bar", "baz"}, "hello": "world"},
		},
		{
			name:     "map with nil value",
			input:    msa{"foo": nil, "bar": "baz"},
			expected: msa{"bar": "baz"},
		},
		{
			name:     "map with empty string, false boolean, and 0 numeric value",
			input:    msa{"foo": "", "bar": "baz", "bool": false, "number": 0.0, "boolt": true, "num": 123},
			expected: msa{"bar": "baz", "boolt": true, "num": 123},
		},
		{
			name: "complex map with many cases",
			input: msa{
				"mapKey": msa{
					"empty":         nil,
					"scalarInt":     5,
					"scalarString":  "blah",
					"emptyMap":      msa{},
					"emptyArray":    []any{},
					"nonEmptyArray": []any{"a", "b"},
					"arrayOfEmptyMaps": []any{
						msa{"emptyMap1": msa{}},
						msa{"emptyMap2": msa{}},
					},
					"arrayOfEmptyAndNonEmptyMaps": []any{
						msa{"emptyMap1": msa{}},
						msa{"emptyMap2": msa{}},
						msa{"nonEmptyMap": msa{"x": 123}},
					},
					"nestedMapKeep": msa{
						"innerMapKeep":   msa{"a": "b"},
						"innerMapDelete": msa{},
					},
					"nestedMapDelete": msa{
						"innerMapDelete": msa{},
					},
					"stringKeyWithEmptyValue": "",
					"intKeyWithZeroValue":     0,
					"boolKeyWithZeroValue":    false,
				},
			},
			expected: msa{
				"mapKey": msa{
					"scalarInt":     5,
					"scalarString":  "blah",
					"nonEmptyArray": []any{"a", "b"},
					"arrayOfEmptyAndNonEmptyMaps": []any{
						msa{"nonEmptyMap": msa{"x": 123}},
					},
					"nestedMapKeep": msa{
						"innerMapKeep": msa{"a": "b"},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			removeNullValuesFromMap(tc.input)

			if !reflect.DeepEqual(tc.expected, tc.input) {
				t.Errorf("\nexpected:\t%+v\nactual:\t%+v", tc.expected, tc.input)
			}
		})
	}
}

// START of JSON Splitter logic tests

const pathSeparator = "."

var jsonObjSimple = []byte(`{
	"map": {
		"field": {
			"inner": 123,
			"inner2": "inner2val"
		},
		"field2": {
			"x": 324
		}
	}
}`)

var jsonObjComplex = []byte(`{
	"address": {
		"city": "New York",
		"country": "USA",
		"postal": {
			"code": "10001",
			"region": "NY",
			"something": {
				"abc": "123"
			}
		}
	},
	"age": 30,
	"arrArrObjs": [[
		{"p": 3, "t": 1},
		{"q": 4, "s": 2}
	],[
		{"l": 30, "t": 10}
	],[
		{"fa": 100, "la": 300}
	]],
	"lastname": null,
	"name": "John",
	"primArr": [1,2,3,4,5,6],
	"projects": [
		{"name": "Project2", "nothing": null, "other": [{"x": "x2"}, {"y": "y2"}], "status": "completed", "vals": [1,2,3]},
		{"name": "Project3", "other": [{"y": "y3"}]},
		{"name": "Project4", "other": [{"z": "z4"}]},
		{"name": "Project1", "other": [{"x": "x1"}, {"w": "w1", "y": "y1"}, {"t": "t1"}, {"z": "z1"}], "status": "in progress", "vals": [4,5,6,7]}
	]
}`)

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

func Test_SplitObject_Simple_EmptyPathsSlice(t *testing.T) {
	paths := []string{}

	expectedOnlyPaths := compactJSON(t, []byte(`{}`))

	expectedWithoutPaths := compactJSON(t, jsonObjSimple)

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

func Test_SplitObject_Simple_NilPathsSlice(t *testing.T) {
	var paths []string

	expectedOnlyPaths := compactJSON(t, []byte(`{}`))

	expectedWithoutPaths := compactJSON(t, jsonObjSimple)

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

func Test_SplitObject_Simple_SinglePath_Level3(t *testing.T) {
	paths := []string{"map.field.inner"}

	expectedOnlyPaths := compactJSON(t, []byte(`{
		"map": {
			"field": {
				"inner": 123
			}
		}
	}`))

	expectedWithoutPaths := compactJSON(t, []byte(`{
		"map": {
			"field": {
				"inner2": "inner2val"
			},
			"field2": {
				"x": 324
			}
		}
	}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

func Test_SplitObject_Simple_SinglePath_Level2(t *testing.T) {
	paths := []string{"map.field"}

	expectedOnlyPaths := compactJSON(t, []byte(`{
		"map": {
			"field": {
				"inner": 123,
				"inner2": "inner2val"
			}
		}
	}`))

	expectedWithoutPaths := compactJSON(t, []byte(`{
		"map": {
			"field2": {
				"x": 324
			}
		}
	}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

func Test_SplitObject_Simple_SinglePath_Level1(t *testing.T) {
	paths := []string{"map"}

	expectedOnlyPaths := compactJSON(t, jsonObjSimple)

	expectedWithoutPaths := compactJSON(t, []byte(`{}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

// TTODO: rename this cause there is no more "merging"
func Test_SplitObject_Simple_MultiplePaths_ShouldMerge(t *testing.T) {
	// TTODO: maybe do not allow users to do this or start from the right first by sorting the array???
	paths := []string{"map.field.inner2", "map", "map.field"}

	expectedOnlyPaths := compactJSON(t, jsonObjSimple)

	expectedWithoutPaths := compactJSON(t, []byte(`{}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

// TTODO: do this
func Test_SplitObject_Simple_MultiplePaths_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX(t *testing.T) {
	// TTODO: maybe do not allow users to do this or start from the right first by sorting the array???
	paths := []string{"map.field.inner2", "map.field"}

	expectedOnlyPaths := compactJSON(t, []byte(`{
		"map": {
			"field": {
				"inner": 123,
				"inner2": "inner2val"
			}
		}
	}`))

	expectedWithoutPaths := compactJSON(t, []byte(`{
		"map": {
			"field2": {
				"x": 324
			}
		}
	}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjSimple, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

func Test_SplitObject_Complex_MultiplePaths_Level1(t *testing.T) {
	paths := []string{"name", "age", "address"}

	expectedOnlyPaths := compactJSON(t, []byte(`{
		"address": {
			"city": "New York",
			"country": "USA",
			"postal": {
				"code": "10001",
				"region": "NY",
				"something": {
					"abc": "123"
				}
			}
		},
		"age": 30,
		"name": "John"
	}`))

	expectedWithoutPaths := compactJSON(t, []byte(`{
		"arrArrObjs": [[
			{"p": 3, "t": 1},
			{"q": 4, "s": 2}
		],[
			{"l": 30, "t": 10}
		],[
			{"fa": 100, "la": 300}
		]],
		"primArr": [1,2,3,4,5,6],
		"projects": [
			{"name": "Project2", "other": [{"x": "x2"}, {"y": "y2"}], "status": "completed", "vals": [1,2,3]},
			{"name": "Project3", "other": [{"y": "y3"}]},
			{"name": "Project4", "other": [{"z": "z4"}]},
			{"name": "Project1",  "other": [{"x": "x1"}, {"w": "w1", "y": "y1"}, {"t": "t1"}, {"z": "z1"}],"status": "in progress", "vals": [4,5,6,7]}
		]
	}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjComplex, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

func Test_SplitObject_Complex_MultiplePaths_LevelN(t *testing.T) {
	paths := []string{"name", "address.city", "address.postal.something", "projects.status", "projects.other.y", "address.postal.region", "projects.vals", "projects.name"}

	expectedOnlyPaths := compactJSON(t, []byte(`{
		"address": {
			"city": "New York",
			"postal": {
			  "region": "NY",
				"something": {
					"abc": "123"
				}
			}
		},
		"name": "John",
		"projects": [
			{"name": "Project2", "other": [{"y": "y2"}], "status": "completed", "vals": [1,2,3]},
			{"name": "Project3", "other": [{"y": "y3"}]},
			{"name": "Project4"},
			{"name": "Project1", "other": [{"y": "y1"}], "status": "in progress", "vals": [4,5,6,7]}
		]
	}`))

	expectedWithoutPaths := compactJSON(t, []byte(`{
		"address": {
			"country": "USA",
			"postal": {
				"code": "10001"
			}
		},
		"age": 30,
		"arrArrObjs": [[
			{"p": 3, "t": 1},
			{"q": 4, "s": 2}
		],[
			{"l": 30, "t": 10}
		],[
			{"fa": 100, "la": 300}
		]],
		"primArr": [1,2,3,4,5,6],
		"projects": [
			{"other": [{"x": "x2"}]},
			{"other": [{"z": "z4"}]},
			{"other": [{"x": "x1"}, {"w": "w1"}, {"t": "t1"}, {"z": "z1"}]}
		]
	}`))

	actualOnlyPathsAsMap, actualWithoutPathsAsMap, err := SplitObject(jsonObjComplex, paths, pathSeparator)
	assert.NoError(t, err)

	actualOnlyPaths := mapToBytesBuffer(t, actualOnlyPathsAsMap)
	actualWithoutPaths := mapToBytesBuffer(t, actualWithoutPathsAsMap)

	assert.Equal(t, expectedOnlyPaths.String(), actualOnlyPaths.String())
	assert.Equal(t, expectedWithoutPaths.String(), actualWithoutPaths.String())
}

// END of JSON Splitter logic tests

func Test_mergeMaps(t *testing.T) {
	// TTODO: use below way to do tests above
	tests := []struct {
		name     string
		left     map[string]any
		right    map[string]any
		expected map[string]any
	}{
		{
			name:     "both empty",
			left:     map[string]any{},
			right:    map[string]any{},
			expected: map[string]any{},
		},
		{
			name:     "right empty",
			left:     map[string]any{"a": 1, "b": 2},
			right:    map[string]any{},
			expected: map[string]any{"a": 1, "b": 2},
		},
		{
			name:     "left empty",
			left:     map[string]any{},
			right:    map[string]any{"a": 1, "b": 2},
			expected: map[string]any{"a": 1, "b": 2},
		},
		{
			name:     "no overlap",
			left:     map[string]any{"a": 1},
			right:    map[string]any{"b": 2},
			expected: map[string]any{"a": 1, "b": 2},
		},
		{
			name:     "with overlap",
			left:     map[string]any{"a": 1},
			right:    map[string]any{"a": 2, "b": 3},
			expected: map[string]any{"a": 2, "b": 3},
		},
		{
			name:     "nested maps",
			left:     map[string]any{"a": map[string]any{"c": 1}, "b": 2},
			right:    map[string]any{"a": map[string]any{"c": 2, "d": 3}},
			expected: map[string]any{"a": map[string]any{"c": 2, "d": 3}, "b": 2},
		},
		{
			name:  "with arrays",
			left:  map[string]any{"a": map[string]any{"c": 1}, "b": 2, "arr": []any{1, 2, 3}},
			right: map[string]any{"a": map[string]any{"c": 2, "d": 3}, "arr": []any{3, 4, 5}},
			// expected: map[string]any{"a": map[string]any{"c": 2, "d": 3}, "b": 2, "arr": []any{1, 2, 3, 3, 4, 5}},
			expected: map[string]any{"a": map[string]any{"c": 2, "d": 3}, "b": 2, "arr": []any{3, 4, 5}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := mergeMaps(tc.left, tc.right)
			assert.NoError(t, err)
			assert.Equal(t, tc.left, tc.expected)
		})
	}
}
