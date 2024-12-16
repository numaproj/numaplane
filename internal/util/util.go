package util

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func JsonUnmarshaler(src any, dst any) error {
	jsonBytes, err := json.Marshal(src)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(jsonBytes, dst); err != nil {
		return fmt.Errorf("failed to convert json %s: err=%s", string(jsonBytes), err)
	}
	return nil
}

// StructToStruct converts a struct to a map[string]interface{} by marshalling the struct to JSON and then
// unmarshalling it to a map. It also converts all json.Number values to int64 or float64.
func StructToStruct(src any) (map[string]any, error) {
	jsonBytes, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}

	var parsedValue map[string]any
	d := json.NewDecoder(strings.NewReader(string(jsonBytes)))
	d.UseNumber()
	err = d.Decode(&parsedValue)
	if err != nil {
		return parsedValue, fmt.Errorf("failed to convert json %s: err=%s", string(jsonBytes), err)
	}
	convertNumbers(parsedValue)

	return parsedValue, nil
}

func convertNumbers(parsedValue map[string]any) {
	for k, v := range parsedValue {
		switch t := v.(type) {
		case json.Number:
			parsedValue[k] = convertNumber(t)
		case map[string]any:
			convertNumbers(t)
		case []any:
			convertNumbersArray(t)
		}
	}
}

func convertNumbersArray(arr []any) {
	for i, v := range arr {
		switch t := v.(type) {
		case json.Number:
			arr[i] = convertNumber(t)
		case map[string]any:
			convertNumbers(t)
		case []any:
			convertNumbersArray(t)
		}
	}
}

func convertNumber(value json.Number) any {
	i64, err := value.Int64()
	if err == nil {
		return i64
	}
	f64, err := value.Float64()
	if err == nil {
		return f64
	}
	return value.String()
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
