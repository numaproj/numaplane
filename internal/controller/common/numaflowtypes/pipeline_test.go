package numaflowtypes

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	withDesiredPhase = `
	{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"desiredPhase": "Paused"
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "out"
		}
	  ]
	}
	`

	withLifecycle = `
	{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {},
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "out"
		}
	  ]
	}
	`

	withoutLifecycle = `
	{
	  "interStepBufferServiceName": "default",
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "out"
		}
	  ]
	}
	`

	withDesiredPhaseAndPauseGracePeriodSeconds = `
	{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"desiredPhase": "Paused",
		"pauseGracePeriodSeconds": "60"
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "out"
		}
	  ]
	}
	`

	withPauseGracePeriodSeconds = `
	{
	  "interStepBufferServiceName": "default",
	  "lifecycle": {
		"pauseGracePeriodSeconds": "60"
	  },
	  "vertices": [
		{
		  "name": "in",
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ],
	  "edges": [
		{
		  "from": "in",
		  "to": "out"
		}
	  ]
	}
	`
)

func Test_WithDesiredPhase(t *testing.T) {

	testCases := []struct {
		name                 string
		originalPipelineYAML string
		expectedPipelineYAML string
	}{
		{
			name:                 "no initial lifecycle or desired phase",
			originalPipelineYAML: withoutLifecycle,
			expectedPipelineYAML: withDesiredPhase,
		},
		{
			name:                 "initial lifecycle but not desired phase",
			originalPipelineYAML: withLifecycle,
			expectedPipelineYAML: withDesiredPhase,
		},
		{
			name:                 "initial lifecycle and desired phase",
			originalPipelineYAML: withDesiredPhase,
			expectedPipelineYAML: withDesiredPhase,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// marshal original yaml into a map and put into an Unstructured type
			pipeline := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var originalYamlSpec map[string]interface{}
			err := json.Unmarshal([]byte(tc.originalPipelineYAML), &originalYamlSpec)
			assert.NoError(t, err)
			pipeline.Object["spec"] = originalYamlSpec

			_ = PipelineWithDesiredPhase(pipeline, "Paused")

			// marshal expected yaml into a map so we can compare them
			var expectedYamlSpec map[string]interface{}
			err = json.Unmarshal([]byte(tc.expectedPipelineYAML), &expectedYamlSpec)
			assert.NoError(t, err)

			assert.Equal(t, expectedYamlSpec, originalYamlSpec)

		})
	}
}

func Test_WithoutDesiredPhase(t *testing.T) {
	testCases := []struct {
		name                 string
		originalPipelineYAML string
		expectedPipelineYAML string
	}{
		{
			name:                 "no initial lifecycle or desired phase",
			originalPipelineYAML: withoutLifecycle,
			expectedPipelineYAML: withoutLifecycle,
		},
		{
			name:                 "initial lifecycle but not desired phase",
			originalPipelineYAML: withLifecycle,
			expectedPipelineYAML: withoutLifecycle,
		},
		{
			name:                 "initial lifecycle and desired phase",
			originalPipelineYAML: withDesiredPhase,
			expectedPipelineYAML: withoutLifecycle,
		},
		{
			name:                 "initial lifecycle and desired phase and pauseGracePeriodSeconds",
			originalPipelineYAML: withDesiredPhaseAndPauseGracePeriodSeconds,
			expectedPipelineYAML: withPauseGracePeriodSeconds,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// marshal original yaml into a map and put into an Unstructured type
			pipeline := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var originalYamlSpec map[string]interface{}
			err := json.Unmarshal([]byte(tc.originalPipelineYAML), &originalYamlSpec)
			assert.NoError(t, err)
			pipeline.Object["spec"] = originalYamlSpec

			PipelineWithoutDesiredPhase(pipeline)

			// marshal expected yaml into a map so we can compare them
			var expectedYamlSpec map[string]interface{}
			err = json.Unmarshal([]byte(tc.expectedPipelineYAML), &expectedYamlSpec)
			assert.NoError(t, err)

			assert.Equal(t, expectedYamlSpec, originalYamlSpec)

		})
	}
}

func Test_GetVertexFromPipelineSpecMap(t *testing.T) {
	testCases := []struct {
		name           string
		pipelineSpec   map[string]interface{}
		vertexName     string
		expectedVertex map[string]interface{}
		expectedFound  bool
		expectError    bool
		errorContains  string
	}{
		{
			name: "vertex found successfully",
			pipelineSpec: map[string]interface{}{
				"vertices": []interface{}{
					map[string]interface{}{
						"name": "in",
						"source": map[string]interface{}{
							"generator": map[string]interface{}{
								"rpu":      int64(5),
								"duration": "1s",
							},
						},
					},
					map[string]interface{}{
						"name": "out",
						"sink": map[string]interface{}{
							"log": map[string]interface{}{},
						},
					},
				},
			},
			vertexName: "in",
			expectedVertex: map[string]interface{}{
				"name": "in",
				"source": map[string]interface{}{
					"generator": map[string]interface{}{
						"rpu":      int64(5),
						"duration": "1s",
					},
				},
			},
			expectedFound: true,
			expectError:   false,
		},
		{
			name: "vertex not found",
			pipelineSpec: map[string]interface{}{
				"vertices": []interface{}{
					map[string]interface{}{
						"name": "in",
						"source": map[string]interface{}{
							"generator": map[string]interface{}{
								"rpu":      int64(5),
								"duration": "1s",
							},
						},
					},
					map[string]interface{}{
						"name": "out",
						"sink": map[string]interface{}{
							"log": map[string]interface{}{},
						},
					},
				},
			},
			vertexName:     "nonexistent",
			expectedVertex: nil,
			expectedFound:  false,
			expectError:    false,
		},
		{
			name: "empty vertices array",
			pipelineSpec: map[string]interface{}{
				"vertices": []interface{}{},
			},
			vertexName:     "in",
			expectedVertex: nil,
			expectedFound:  false,
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vertex, found, err := GetVertexFromPipelineSpecMap(tc.pipelineSpec, tc.vertexName)

			if tc.expectError {
				assert.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
				assert.False(t, found)
				assert.Nil(t, vertex)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedFound, found)
				if tc.expectedFound {
					assert.Equal(t, tc.expectedVertex, vertex)
				} else {
					assert.Nil(t, vertex)
				}
			}
		})
	}
}
