package util

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
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

// CompareStructNumTypeAgnostic compares two structs, ignoring the type of numbers
// (e.g., int, float32, float64) and treating them as equal if their values are equal.
func CompareStructNumTypeAgnostic(src, dst any) bool {
	numberComparer := cmp.Comparer(func(x, y any) bool {
		vx, _ := toFloat64(x)
		vy, _ := toFloat64(y)
		return vx == vy
	})

	// Apply this custom comparison only to pairs of values where both are numbers
	filterNumber := cmp.FilterValues(func(x, y any) bool {
		return isNumber(x) && isNumber(y)
	}, numberComparer)

	equal := cmp.Equal(src, dst, filterNumber)
	return equal
}

func isNumber(value any) bool {
	v := reflect.TypeOf(value)
	if v == nil {
		return false
	}
	kind := v.Kind()
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func toFloat64(value any) (float64, bool) {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	default:
		return 0, false
	}
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

// ExtractPath extracts the fields specified by the path (given as slice of strings) from the given data.
//
// The function takes two arguments:
//   - data: The input data, which can be a map, a slice, or a nested combination of both.
//   - path: A slice of strings representing the path to the desired fields.
//
// It returns three values:
//   - any: The extracted data, which can be a map, a slice, or a single value (including nil).
//   - bool: A boolean indicating whether last field in the path is a map.
//   - error: An error if any occurred during the extraction process.
//
// The function works by recursively traversing the input data according to the
// provided path. It handles maps and slices, extracting the corresponding values
// at each level. If the path leads to a value that is neither a map nor a slice,
// it returns that value.
func ExtractPath(data any, path []string) (any, bool, error) {
	if len(path) == 0 || data == nil {
		isMap := false
		if data != nil {
			isMap = reflect.TypeOf(data).Kind() == reflect.Map
		}

		return data, isMap, nil
	}

	v := reflect.ValueOf(data)
	key := path[0]

	switch v.Kind() {
	case reflect.Map:
		m := make(map[string]any)
		for _, k := range v.MapKeys() {
			if k.String() == key {
				extracted, isMap, err := ExtractPath(v.MapIndex(k).Interface(), path[1:])
				if err != nil {
					return nil, false, err
				}
				m[key] = extracted
				return m, isMap, nil
			}
		}
		return nil, false, nil

	case reflect.Slice:
		s := make([]any, v.Len())
		atLeastOneIsMap := false
		for i := 0; i < v.Len(); i++ {
			extracted, isMap, err := ExtractPath(v.Index(i).Interface(), path)
			if err != nil {
				return nil, false, err
			}

			s[i] = extracted

			if isMap {
				atLeastOneIsMap = isMap
			}
		}
		return s, atLeastOneIsMap, nil

	default:
		return nil, false, fmt.Errorf("invalid type encountered: %s", v.Kind().String())
	}
}
