package util

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// StructToStruct converts a struct type (src) into another (dst)
func StructToStruct(src any, dst any) error {
	jsonBytes, err := json.Marshal(src)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonBytes, dst)
	if err != nil {
		return fmt.Errorf("failed to convert json %s: err=%s", string(jsonBytes), err)
	}

	return nil
}

// SplitObject returns 2 maps from a given object as bytes array and a slice of paths.
// One of the 2 output maps will include only the paths from the slice while the second returned map will include all other paths.
func SplitObject(obj any, paths []string, excludedPaths []string, pathSeparator string) (map[string]any, map[string]any, error) {
	var objAsMap map[string]any
	if err := StructToStruct(obj, &objAsMap); err != nil {
		return nil, nil, err
	}

	return SplitMap(objAsMap, paths, excludedPaths, pathSeparator)
}

// SplitMap returns 2 maps from a given map and a slice of paths.
// One of the 2 output maps will include only the paths from the slice while the second returned map will include all other paths.
// NOTE: any path in "paths" which is not found in m will have an associated key in "onlyPaths", which is "{}"
// If the caller is calling this function on 2 maps for the purpose of comparing them, and if the key is not found in either one,
// then 'key: {}' will be returned for both, and they will be deemed equal
//
// Also ignores any paths in the excludedPaths list which are each demarcated by "pathSeparator" value
func SplitMap(m map[string]any, paths []string, excludedPaths []string, pathSeparator string) (onlyPaths map[string]any, withoutPaths map[string]any, err error) {
	onlyPaths = make(map[string]any)
	withoutPaths, err = cloneMap(m)
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
	sort.Strings(excludedPaths)

	// Split by paths
	for _, path := range paths {
		pathTokens := strings.Split(path, pathSeparator)

		err = extractPath(withoutPaths, onlyPaths, pathTokens)
		if err != nil {
			return nil, nil, err
		}
	}

	// Remove the excluded paths from the 2 output maps
	RemovePaths(onlyPaths, excludedPaths, pathSeparator)
	RemovePaths(withoutPaths, excludedPaths, pathSeparator)

	return onlyPaths, withoutPaths, nil
}

// cloneMap returns a clone of the given map
func cloneMap(m map[string]any) (map[string]any, error) {
	var clone map[string]any

	if err := StructToStruct(m, &clone); err != nil {
		return nil, err
	}

	return clone, nil
}

// extractPath extracts a path from the source map into the destination map based on a slice of token representing the path
// NOTE: if the path is not found in m, it will have an associated empty key in "dst"
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

		// Loop through each slice element to extract paths inside slice of objects
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

// RemovePaths removes all of the excludedPaths passed in from m, where each excludedPath is a string
// representation of the path, demarcated by pathSeparator
func RemovePaths(m map[string]any, excludedPaths []string, pathSeparator string) {
	for _, path := range excludedPaths {
		pathTokens := strings.Split(path, pathSeparator)

		removePath(m, pathTokens)
	}
}

// removePath removes a path (given as a slice of strings) from the given map
func removePath(m map[string]any, pathTokens []string) {
	curr := m
	for i, key := range pathTokens {
		if i == len(pathTokens)-1 {
			delete(curr, key)
		} else {
			v, ok := curr[key]
			if !ok {
				return // Key not found in the map
			}

			switch vTyped := v.(type) {
			case map[string]any:
				curr = vTyped

			case []any:
				for j := range vTyped {
					switch vTypedElem := vTyped[j].(type) {
					case map[string]any:
						removePath(vTypedElem, pathTokens[i+1:])

					case []any:
						// TODO: this should not be necessary in this context (not many array of arrays in k8s yaml definitions),
						// but implement it for completeness (low priority)

					default:
						return
					}
				}

			default:
				return
			}

		}
	}
}
