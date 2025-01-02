package util

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type msa = map[string]any

var simpleMap = msa{
	"map": msa{
		"field": msa{
			"inner":  123,
			"inner2": "inner2val",
		},
		"field2": msa{
			"x": 324,
		},
	},
}

func compactJSON(t *testing.T, jsonObj []byte) *bytes.Buffer {
	compactedObj := new(bytes.Buffer)
	if err := json.Compact(compactedObj, jsonObj); err != nil {
		assert.NoError(t, err)
	}

	return compactedObj
}

func mapToBytesBuffer(t *testing.T, obj any) *bytes.Buffer {
	raw, err := json.Marshal(obj)
	assert.NoError(t, err)

	return compactJSON(t, raw)
}

func Test_ExtractPath(t *testing.T) {
	testCases := []struct {
		name              string
		inputMap          msa
		pathTokens        []string
		expectedOutputMap msa
		expectedIsMap     bool
	}{
		{
			name: "one path token",
			inputMap: msa{
				"lifecycle": msa{
					"desiredPhase": "paused",
					"timeout":      123,
				},
				"vertices": []msa{
					{"name": "v1"},
					{"name": "v2"},
				},
			},
			pathTokens:        []string{"lifecycle"},
			expectedOutputMap: msa{"lifecycle": msa{"desiredPhase": "paused", "timeout": 123}},
			expectedIsMap:     true,
		},
		{
			name:              "simple map - empty paths slice",
			inputMap:          simpleMap,
			pathTokens:        []string{},
			expectedOutputMap: simpleMap,
			expectedIsMap:     true,
		},
		{
			name:              "simple map - nil paths slice",
			inputMap:          simpleMap,
			pathTokens:        nil,
			expectedOutputMap: simpleMap,
			expectedIsMap:     true,
		},
		{
			name:              "simple map - single path - level 3",
			inputMap:          simpleMap,
			pathTokens:        []string{"map", "field", "inner"},
			expectedOutputMap: msa{"map": msa{"field": msa{"inner": 123}}},
			expectedIsMap:     false,
		},
		{
			name:              "simple map - single bad path",
			inputMap:          simpleMap,
			pathTokens:        []string{"invalid"},
			expectedOutputMap: nil,
			expectedIsMap:     false,
		},
		{
			name:              "simple map - single path - level 2",
			inputMap:          simpleMap,
			pathTokens:        []string{"map", "field"},
			expectedOutputMap: msa{"map": msa{"field": msa{"inner": 123, "inner2": "inner2val"}}},
			expectedIsMap:     true,
		},
		{
			name:              "simple map - single path - level 1",
			inputMap:          simpleMap,
			pathTokens:        []string{"map"},
			expectedOutputMap: simpleMap,
			expectedIsMap:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualOutputMapAsMap, actualIsMap, err := ExtractPath(tc.inputMap, tc.pathTokens)
			assert.NoError(t, err)

			actualOutputMap := mapToBytesBuffer(t, actualOutputMapAsMap)
			expectedOutputMap := mapToBytesBuffer(t, tc.expectedOutputMap)

			assert.Equal(t, expectedOutputMap.String(), actualOutputMap.String())
			assert.Equal(t, tc.expectedIsMap, actualIsMap)
		})
	}
}

func Test_CompareStructNumTypeAgnostic(t *testing.T) {
	testCases := []struct {
		name               string
		a                  map[string]interface{}
		b                  map[string]interface{}
		expectedComparison bool
	}{
		{
			name:               "equal structs - same number type",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": 1},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Int8",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": int8(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Int16",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": int16(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Int32",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": int32(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Int64",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": int64(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Float32",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": float32(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as float64",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": float64(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Uint",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": uint(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Uint8",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": uint8(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Uint16",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": uint16(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Uint32",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": uint32(1)},
			expectedComparison: true,
		},
		{
			name:               "equal structs - different number type as Uint64",
			a:                  map[string]interface{}{"number": 1},
			b:                  map[string]interface{}{"number": uint64(1)},
			expectedComparison: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualComparison := CompareStructNumTypeAgnostic(tc.a, tc.b)
			assert.Equal(t, tc.expectedComparison, actualComparison)
		})
	}
}
