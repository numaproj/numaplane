package util

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"sort"
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

// SplitObject returns 2 maps from a given object as bytes array and a slice of paths.
// One of the 2 output maps will include only the paths from the slice while the second returned map will include all other paths.
func SplitObject(obj []byte, paths []string, pathSeparator string) (map[string]any, map[string]any, error) {
	var objAsMap map[string]any
	if err := json.Unmarshal(obj, &objAsMap); err != nil {
		return nil, nil, err
	}

	return SplitMap(objAsMap, paths, pathSeparator)
}

// SplitMap returns 2 maps from a given map and a slice of paths.
// One of the 2 output maps will include only the paths from the slice while the second returned map will include all other paths.
func SplitMap(obj map[string]any, paths []string, pathSeparator string) (onlyPaths map[string]any, withoutPaths map[string]any, err error) {
	onlyPaths = make(map[string]any)
	withoutPaths, err = clone(obj)
	if err != nil {
		return nil, nil, err
	}

	// In case there are no paths, return an empty map and the original map.
	// This is in place so that this logic can be bypassed at runtime by providing
	// an empty slice of paths.
	if len(paths) == 0 {
		return onlyPaths, withoutPaths, nil
	}

	// Sort the paths slice before using it to avoid maps merging in case the slice were to
	// include deeply nested fields and then the higher level fields (ex: []string{"map.field.inner2", "map"})
	sort.Strings(paths)

	for _, path := range paths {
		pathTokens := strings.Split(path, pathSeparator)

		if err := extractPath(withoutPaths, onlyPaths, pathTokens); err != nil {
			return nil, nil, err
		}
	}

	cleanup(onlyPaths)
	cleanup(withoutPaths)

	return onlyPaths, withoutPaths, nil
}

// clone returns a clone of the given map
func clone(obj map[string]any) (map[string]any, error) {
	var clone map[string]any

	if err := StructToStruct(obj, &clone); err != nil {
		return nil, err
	}

	return clone, nil
}

// cleanup removes nil values, empty maps, and empty arrays from the given object
func cleanup(obj map[string]any) {
	for key, val := range obj {
		switch typedVal := val.(type) {
		case map[string]any:
			cleanup(typedVal)

			if len(typedVal) == 0 {
				delete(obj, key)
			}

		case []any:
			for i := 0; i < len(typedVal); i++ {
				if elemMap, ok := typedVal[i].(map[string]any); ok {
					cleanup(elemMap)

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

// // sliceToInterfaceSlice converts a typed slice into a slice of any type
// func sliceToInterfaceSlice(slice any) ([]any, error) {
// 	s := reflect.ValueOf(slice)
// 	if s.Kind() != reflect.Slice {
// 		return nil, errors.New("not a slice type")
// 	}

// 	newSlice := make([]any, s.Len())
// 	for i := 0; i < s.Len(); i++ {
// 		newSlice[i] = s.Index(i).Interface()
// 	}

// 	return newSlice, nil
// }

// mergeMaps recursively merge the right map into the left map without replacing any key that already exists in the left map
func mergeMaps(a, b map[string]any) error {
	for key, bVal := range b {
		if aVal, ok := a[key]; ok {
			if reflect.TypeOf(aVal).Kind() == reflect.Map && reflect.TypeOf(bVal).Kind() == reflect.Map {
				valAMap := aVal.(map[string]any)
				valBMap := bVal.(map[string]any)

				err := mergeMaps(valAMap, valBMap)
				if err != nil {
					return err
				}

				// } else if reflect.TypeOf(aVal).Kind() == reflect.Slice && reflect.TypeOf(bVal).Kind() == reflect.Slice {
				// 	valASlice, err := sliceToInterfaceSlice(aVal)
				// 	if err != nil {
				// 		return err
				// 	}

				// 	valBSlice, err := sliceToInterfaceSlice(bVal)
				// 	if err != nil {
				// 		return err
				// 	}

				// 	// Slice b is appended to slice a (duplicates and sub-values/maps are not considered for merging)
				// 	a[key] = append(valASlice, valBSlice...)
			} else {
				// Overwrite the primitive type or any other type in a with value from b
				a[key] = bVal
			}
		} else {
			a[key] = bVal
		}
	}

	return nil
}

// extractPath extracts a path from the source map into the destination path based on a slice of token representing the path
func extractPath(src, dst map[string]any, pathTokens []string) error {
	// panic guardrail (this condition should never be reached and true)
	if len(pathTokens) == 0 {
		return nil
	}

	key := pathTokens[0]

	srcVal, exists := src[key]
	if !exists {
		return nil
	}

	// Last path token sets the value or merges maps
	if len(pathTokens) == 1 {
		// switch dst[key].(type) {
		// case map[string]any:
		// 	// TTODO: why is this necessary???
		// 	err := mergeMaps(dst[key].(map[string]any), src[key].(map[string]any))
		// 	if err != nil {
		// 		return err
		// 	}
		// default:
		// 	dst[key] = src[key]
		// }

		dst[key] = src[key]

		delete(src, key)
		return nil
	}

	switch nextSrc := srcVal.(type) {
	case map[string]any:
		if _, exists := dst[key]; !exists {
			dst[key] = make(map[string]any)
		}

		if err := extractPath(nextSrc, dst[key].(map[string]any), pathTokens[1:]); err != nil {
			return err
		}

	case []any:
		if _, exists := dst[key]; !exists {
			dst[key] = make([]any, len(nextSrc))
		}

		// Loop throught each slice element to extract paths inside slice of objects
		for i := range nextSrc {
			switch nextSrcElem := nextSrc[i].(type) {
			case map[string]any:
				nextDestArr := dst[key].([]any)
				if nextDestArr[i] == nil {
					nextDestArr[i] = make(map[string]any)
				}

				if err := extractPath(nextSrcElem, nextDestArr[i].(map[string]any), pathTokens[1:]); err != nil {
					return err
				}

			case []any:
				// TODO: this should not be necessary in this context (not many array of arrays in k8s yaml definitions),
				// but implement it for completeness (low priority)

			default:
				dst[key] = nextSrc
				return nil
			}
		}

	default:
		dst[key] = srcVal
		delete(src, key)
	}

	return nil
}
