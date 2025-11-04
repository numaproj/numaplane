/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"maps"
	"strings"

	"github.com/google/go-cmp/cmp"
)

func MergeMaps(existing, new map[string]string) map[string]string {
	merged := make(map[string]string)
	if existing != nil {
		merged = existing
	}

	for key, val := range new {
		merged[key] = val
	}

	return merged
}

func CompareMaps(existing, new map[string]string) bool {
	if existing == nil || new == nil {
		return len(existing) == len(new)
	}
	return cmp.Equal(existing, new)
}

// ConvertInterfaceMapToStringMap converts a map[string]interface{} to map[string]string
// by type asserting each value to string. Non-string values are skipped.
func ConvertInterfaceMapToStringMap(interfaceMap map[string]interface{}) map[string]string {
	if interfaceMap == nil {
		return nil
	}

	stringMap := make(map[string]string)
	for k, v := range interfaceMap {
		if str, ok := v.(string); ok {
			stringMap[k] = str
		}
	}
	return stringMap
}

func IsMapSubset(requiredKVPairs map[string]string, mapToCheck map[string]string) bool {

	// If there are no required key-value pairs (nil or empty), always return true
	if len(requiredKVPairs) == 0 {
		return true
	}
	// If the map to look for is nil or empty, return false
	if len(mapToCheck) == 0 {
		return false
	}

	for key, value := range requiredKVPairs {
		if mapToCheck[key] != value {
			return false
		}
	}
	return true
}

// CompareMapsWithExceptions compares two maps but ignoring any differences where the keys are prefixed with any of the 'prefixExceptions'
func CompareMapsWithExceptions(existing, new map[string]string, prefixExceptions ...string) bool {
	// clone the maps because we can make nil maps empty maps to make it easier to compare
	existingCopy := maps.Clone(existing)
	newCopy := maps.Clone(new)
	if existingCopy == nil {
		existingCopy = make(map[string]string)
	}
	if newCopy == nil {
		newCopy = make(map[string]string)
	}

	for existingKey, existingValue := range existingCopy {
		isException := false
		for _, prefixException := range prefixExceptions {
			if strings.HasPrefix(existingKey, prefixException) {
				isException = true
				break
			}
		}
		if !isException {
			// is this key in the other map and does it have the same value?
			newValue := newCopy[existingKey]
			if existingValue != newValue {
				return false
			}
		}

	}

	return true
}
