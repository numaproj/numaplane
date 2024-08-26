package util

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
)

func StructToStruct(src any, dest any) error {
	jsonBytes, err := json.Marshal(src)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonBytes, dest)
	if err != nil {
		return fmt.Errorf("failed to convert json %s: err=%s", string(jsonBytes), err)
	}

	return nil
}

// compare 2 maps for equality, ignoring any null values, empty maps, and empty arrays
func CompareMapsIgnoringNulls(a map[string]interface{}, b map[string]interface{}) bool {
	aNoNulls := make(map[string]interface{})
	maps.Copy(aNoNulls, a)

	bNoNulls := make(map[string]interface{})
	maps.Copy(bNoNulls, b)

	removeNullValuesFromJSONMap(aNoNulls)
	removeNullValuesFromJSONMap(bNoNulls)
	return reflect.DeepEqual(aNoNulls, bNoNulls)
}

// recursively remove any nulls, empty strings, empty maps, and empty arrays from the map
// the types that we look for are the ones that json.Unmarshal() uses
func removeNullValuesFromJSONMap(m map[string]interface{}) bool {

	for k, v := range m {
		if v == nil {
			delete(m, k)
		} else if stringValue, ok := v.(string); ok {
			if stringValue == "" {
				delete(m, k)
			}
			// scalar numbers seem to use float64:
		} else if floatValue, ok := v.(float64); ok {
			if floatValue == 0 {
				delete(m, k)
			}
		} else if boolValue, ok := v.(bool); ok {
			if !boolValue {
				delete(m, k)
			}
		} else if nestedMap, ok := v.(map[string]interface{}); ok {
			removeNullValuesFromJSONMap(nestedMap)
			if len(nestedMap) == 0 {
				delete(m, k)
			}
		} else if nestedSlice, ok := v.([]interface{}); ok {
			allMapsEmpty := true
			for _, sliceElem := range nestedSlice {
				if asMap, ok := sliceElem.(map[string]interface{}); ok {
					removeNullValuesFromJSONMap(asMap)
					if len(asMap) != 0 {
						allMapsEmpty = false
					}
				} else {
					allMapsEmpty = false
				}
			}
			if allMapsEmpty {
				delete(m, k)
			}

		}
	}
	return false
}
