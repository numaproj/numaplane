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

// JSON Splitter logic tests below this line
// TODO: add more tests, cleanup, change as needed

const pathSeparator = "."

var jsonObj = []byte(`{
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

var jsonObjCompl = []byte(`{
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

func Test_SplitObject_1(t *testing.T) {
	paths := []string{"map.field.inner"}

	expectedOnlyPaths := []byte(`{
		"map": {
			"field": {
				"inner": 123
			}
		}
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{
		"map": {
			"field": {
				"inner2": 456
			},
			"field2": {
				"x": 324
			}
		}
	}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObj, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}

func Test_SplitObject_2(t *testing.T) {
	paths := []string{"map.field"}

	expectedOnlyPaths := []byte(`{
		"map": {
			"field": {
				"inner": 123,
				"inner2": 456
			}
		}
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{
		"map": {
			"field2": {
				"x": 324
			}
		}
	}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObj, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}

func Test_SplitObject_3(t *testing.T) {
	paths := []string{"map"}

	expectedOnlyPaths := []byte(`{
		"map": {
			"field": {
				"inner": 123,
				"inner2": 456
			},
			"field2": {
				"x": 324
			}
		}
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObj, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}

func Test_SplitObject_4(t *testing.T) {
	paths := []string{"map", "map.field"}

	expectedOnlyPaths := []byte(`{
		"map": {
			"field": {
				"inner": 123,
				"inner2": 456
			},
			"field2": {
				"x": 324
			}
		}
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObj, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}

func Test_SplitObject_5(t *testing.T) {
	paths := []string{"map.field.inner2", "map"}

	expectedOnlyPaths := []byte(`{
		"map": {
			"field": {
				"inner": 123,
				"inner2": 456
			},
			"field2": {
				"x": 324
			}
		}
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObj, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}

func Test_SplitObject_6(t *testing.T) {
	paths := []string{"name", "age", "address"}

	expectedOnlyPaths := []byte(`{
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
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{
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
			{"name": "Project3", "other": [{"z": "z4"}]},
			{"name": "Project1",  "other": [{"x": "x1"}, {"w": "w1", "y": "y1"}, {"t": "t1"}, {"z": "z1"}],"status": "in progress", "vals": [4,5,6,7]}
		]
	}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObjCompl, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}

func Test_SplitObject_7(t *testing.T) {
	paths := []string{"name", "address.city", "address.postal.something", "projects.status", "projects.other.y", "address.postal.region", "projects.vals", "projects.name"}

	expectedOnlyPaths := []byte(`{
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
			{"name": "Project3"},
			{"name": "Project1", "other": [{"y": "y1"}], "status": "in progress", "vals": [4,5,6,7]}
		]
	}`)
	expectedOnlyPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedOnlyPathsClean, expectedOnlyPaths); err != nil {
		assert.NoError(t, err)
	}

	expectedWithoutPaths := []byte(`{
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
	}`)
	expectedWithoutPathsClean := new(bytes.Buffer)
	if err := json.Compact(expectedWithoutPathsClean, expectedWithoutPaths); err != nil {
		assert.NoError(t, err)
	}

	actualOnlyPaths, actualWithoutPaths, err := SplitObject(jsonObjCompl, paths, pathSeparator)
	assert.NoError(t, err)

	assert.Equal(t, expectedOnlyPathsClean, actualOnlyPaths)
	assert.Equal(t, expectedWithoutPathsClean, actualWithoutPaths)
}
