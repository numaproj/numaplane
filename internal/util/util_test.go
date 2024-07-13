package util

import (
	"encoding/json"
	"fmt"
	"testing"
)

var testJson string = `
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
		}
	}

}

`

func Test_removeNullValuesFromMap(t *testing.T) {
	m := make(map[string]interface{})
	json.Unmarshal([]byte(testJson), &m)
	fmt.Println(m)
	removeNullValuesFromMap(m)
	fmt.Println(m)
}
