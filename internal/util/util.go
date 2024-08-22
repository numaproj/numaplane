package util

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
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

// recursively remove any zero values from the map including null references, empty strings, numbers that are zero,
// empty maps, and empty arrays from the map
// (the types that we look for are the ones that json.Unmarshal() uses)
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

// TODO: fix, cleanup, improve errors, and test everything below this line

func SplitObject(obj []byte, paths []string, pathSeparator string) (map[string]any, map[string]any, error) {
	var objAsMap map[string]any
	if err := json.Unmarshal(obj, &objAsMap); err != nil {
		return nil, nil, err
	}

	onlyPaths := make(map[string]any)
	withoutPaths, err := clone(objAsMap)
	if err != nil {
		return nil, nil, err
	}

	for _, path := range paths {
		pathTokens := strings.Split(path, pathSeparator)

		if _, err := extractPath(onlyPaths, withoutPaths, pathTokens); err != nil {
			return nil, nil, err
		}
	}

	cleanUp(onlyPaths)
	cleanUp(withoutPaths)

	return onlyPaths, withoutPaths, nil
}

func clone(obj map[string]any) (map[string]any, error) {
	var clone map[string]any

	if err := StructToStruct(obj, &clone); err != nil {
		return nil, err
	}

	return clone, nil
}

func cleanUp(obj map[string]any) {
	for key, val := range obj {
		switch typedVal := val.(type) {
		case map[string]any:
			cleanUp(typedVal)

			if len(typedVal) == 0 {
				delete(obj, key)
			}

		case []any:
			for i := 0; i < len(typedVal); i++ {
				if elemMap, ok := typedVal[i].(map[string]any); ok {
					cleanUp(elemMap)

					if len(elemMap) == 0 {
						typedVal = append(typedVal[:i], typedVal[i+1:]...)
						i--
					}
				} else if typedVal[i] == nil {
					typedVal = append(typedVal[:i], typedVal[i+1:]...)
					i--
				}
			}

			if len(typedVal) == 0 {
				delete(obj, key)
			} else {
				obj[key] = typedVal
			}

		case nil:
			delete(obj, key)

		}
	}
}

func extractPath(currDest, currSrc map[string]any, pathTokens []string) (bool, error) {
	if len(pathTokens) == 0 {
		return false, nil
	}

	key := pathTokens[0]

	val, exists := currSrc[key]
	if !exists {
		return true, nil
	}

	switch nextSrc := val.(type) {
	case map[string]any:
		if _, exists := currDest[key]; !exists {
			currDest[key] = make(map[string]any)
		}

		if len(pathTokens) == 1 {
			currDest[key] = nextSrc
			delete(currSrc, key)
			// TODO: should we do this?
			// if len(currSrc) == 0 {
			// 	return true, nil
			// }
			return false, nil
		}

		canDeleteParent, err := extractPath(currDest[key].(map[string]any), nextSrc, pathTokens[1:])
		if err != nil {
			return false, err
		}

		// delete parent recursively if empty
		if canDeleteParent {
			delete(currSrc, key)

			if len(currSrc) == 0 {
				return true, nil
			}
		}

	case []any:
		if _, exists := currDest[key]; !exists {
			currDest[key] = make([]any, len(nextSrc))
		}

		for i := range nextSrc {
			switch nextSrcElem := nextSrc[i].(type) {
			case map[string]any:
				nextDestArr := currDest[key].([]any)
				if nextDestArr[i] == nil {
					nextDestArr[i] = make(map[string]any)
				}

				if len(pathTokens) == 1 {
					nextDestArr[i] = nextSrcElem
					// TODO: should we do this?
					// delete(currSrc, key)
					// if len(currSrc) == 0 {
					// 	return true, nil
					// }
					return false, nil
				}

				_, err := extractPath(nextDestArr[i].(map[string]any), nextSrcElem, pathTokens[1:])
				if err != nil {
					return false, err
				}

			case []any:
				// TODO: implement for completenes, but it should not be necessary in this context

			default:
				currDest[key] = nextSrc
				return false, nil
			}
		}

	default:
		currDest[key] = val
		delete(currSrc, key)

		// If empty, let previous caller know that the parent can be deleted
		if len(currSrc) == 0 {
			return true, nil
		}
	}

	return false, nil
}
