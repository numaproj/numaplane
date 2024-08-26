package util

import (
	"encoding/json"
	"fmt"
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
		"keyWithEmptyValue": ""
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
	fmt.Printf("before removing nulls: %v", inputMap)
	removeNullValuesFromMap(inputMap)
	fmt.Printf("after removing nulls: %v", inputMap)

	outputMap := make(map[string]interface{})
	_ = json.Unmarshal([]byte(outputJson), &outputMap)
	assert.True(t, reflect.DeepEqual(inputMap, outputMap))
}
