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

func CompareMapsIgnoringNulls(a map[string]interface{}, b map[string]interface{}) bool {
	aNoNulls := make(map[string]interface{})
	maps.Copy(aNoNulls, a)

	bNoNulls := make(map[string]interface{})
	maps.Copy(bNoNulls, b)

	removeNullValuesFromMap(aNoNulls)
	removeNullValuesFromMap(bNoNulls)

	//aAsJson, _ := json.Marshal(aNoNulls)
	//bAsJson, _ := json.Marshal(bNoNulls)

	//fmt.Printf("deletethis: aAsJson=%v, bAsJson=%v\n", string(aAsJson), string(bAsJson))
	return reflect.DeepEqual(aNoNulls, bNoNulls)
}

// recursively remove any nulls, empty maps, and empty arrays from the map
func removeNullValuesFromMap(m map[string]interface{}) bool {

	for k, v := range m {
		if v == nil {
			delete(m, k)
		} else if nestedMap, ok := v.(map[string]interface{}); ok {
			removeNullValuesFromMap(nestedMap)
			if len(nestedMap) == 0 {
				delete(m, k)
			}
		} else if nestedSlice, ok := v.([]interface{}); ok {
			allMapsEmpty := true
			for _, sliceElem := range nestedSlice {
				if asMap, ok := sliceElem.(map[string]interface{}); ok {
					removeNullValuesFromMap(asMap)
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
