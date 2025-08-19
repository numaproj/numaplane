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

func TestOptionalString(t *testing.T) {

	// nil case:
	var intPtr *int
	result := OptionalString(intPtr)
	assert.Equal(t, "nil", result)

	// standard case: pointer to value
	intVal := 10
	intPtr = &intVal
	result = OptionalString(intPtr)
	assert.Equal(t, "10", result)

	// not a pointer:
	result = OptionalString(10)
	assert.Equal(t, "", result)
}

func Test_ToFloat64(t *testing.T) {
	testCases := []struct {
		name              string
		val               any
		expectedResultVal float64
		isNumber          bool
	}{
		{
			name:              "integer",
			val:               int32(5),
			expectedResultVal: 5,
			isNumber:          true,
		},
		{
			name:              "float",
			val:               5.6,
			expectedResultVal: 5.6,
			isNumber:          true,
		},
		{
			name:              "string",
			val:               "5.6",
			expectedResultVal: 0,
			isNumber:          false,
		},
		{
			name:              "nil",
			val:               nil,
			expectedResultVal: 0,
			isNumber:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, isNum := ToFloat64(tc.val)
			assert.Equal(t, tc.expectedResultVal, result)
			assert.Equal(t, tc.isNumber, isNum)
		})
	}
}

func Test_ToInt64(t *testing.T) {
	testCases := []struct {
		name              string
		val               any
		expectedResultVal int64
		isInt             bool
	}{
		{
			name:              "integer",
			val:               int32(5),
			expectedResultVal: 5,
			isInt:             true,
		},
		{
			name:              "float - not castable",
			val:               float64(5.6),
			expectedResultVal: 0,
			isInt:             false,
		},
		{
			name:              "float - castable",
			val:               float64(5.0),
			expectedResultVal: 5,
			isInt:             true,
		},
		{
			name:              "string",
			val:               "5",
			expectedResultVal: 0,
			isInt:             false,
		},
		{
			name:              "nil",
			val:               nil,
			expectedResultVal: 0,
			isInt:             false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, isInt := ToInt64(tc.val)
			assert.Equal(t, tc.expectedResultVal, result)
			assert.Equal(t, tc.isInt, isInt)
		})
	}
}

// This tests the ResolveTemplateSpec function
// This function resolves any templated definition with the provided arguments
func TestResolveTemplateSpec(t *testing.T) {

	tests := []struct {
		name           string
		data           any
		args           map[string]interface{}
		expectedOutput map[string]interface{}
	}{
		{
			name: "standard test",
			data: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-{{.monovertex-name}}",
						"namespace": "{{.monovertex-namespace}}",
					},
				},
			},
			args: map[string]interface{}{
				".monovertex-name":      "my-monovertex-0",
				".monovertex-namespace": "test-namespace",
			},
			expectedOutput: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
		},
		{
			name: "argument is used multiple times",
			data: map[string]interface{}{
				"annotations": map[string]interface{}{
					"link.argocd.argoproj.io/external-link": "https://splunk.intuit.com/en-US/app/search/search?q=search%20(index%3Daccounting-ledger%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2F{{.monovertex-namespace}}%2F*)%20OR%20(index%3Diks%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2F{{.monovertex-namespace}}%2F*)%20kubernetes_pod%3D{{.monovertex-name}}*&display.page.search.mode=fast&dispatch.sample_ratio=1&earliest=-5m&latest=now",
				},
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-{{.monovertex-name}}",
						"namespace": "{{.monovertex-namespace}}",
					},
				},
			},
			args: map[string]interface{}{
				".monovertex-name":      "my-monovertex-0",
				".monovertex-namespace": "test-namespace",
			},
			expectedOutput: map[string]interface{}{
				"annotations": map[string]interface{}{
					"link.argocd.argoproj.io/external-link": "https://splunk.intuit.com/en-US/app/search/search?q=search%20(index%3Daccounting-ledger%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2Ftest-namespace%2F*)%20OR%20(index%3Diks%20source%3Diks2%2Fsbg-qbo-ppd-usw2-k8s%2F*%2Ftest-namespace%2F*)%20kubernetes_pod%3Dmy-monovertex-0*&display.page.search.mode=fast&dispatch.sample_ratio=1&earliest=-5m&latest=now",
				},
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
		},
		{
			name: "input is not templated",
			data: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
			args: map[string]interface{}{
				".monovertex-name":      "my-monovertex-0",
				".monovertex-namespace": "test-namespace",
			},
			expectedOutput: map[string]interface{}{
				"spec": map[string]interface{}{
					"configMap": map[string]interface{}{
						"name":      "test-volume-my-monovertex-0",
						"namespace": "test-namespace",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ResolveTemplatedSpec(tt.data, tt.args)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, result)
		})
	}

}
