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

			_ = WithDesiredPhase(pipeline, "Paused")

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

			WithoutDesiredPhase(pipeline)

			// marshal expected yaml into a map so we can compare them
			var expectedYamlSpec map[string]interface{}
			err = json.Unmarshal([]byte(tc.expectedPipelineYAML), &expectedYamlSpec)
			assert.NoError(t, err)

			assert.Equal(t, expectedYamlSpec, originalYamlSpec)

		})
	}
}
