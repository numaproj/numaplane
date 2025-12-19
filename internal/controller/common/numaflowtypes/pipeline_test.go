package numaflowtypes

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
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

func Test_ScalePipelineDefSourceVerticesToZero(t *testing.T) {
	tests := []struct {
		name                      string
		pipelineDef               string
		expectError               bool
		expectedResultPipelineDef string
	}{
		{
			name: "Multiple source vertices",
			pipelineDef: `
{
  "vertices": [
	{
	  "name": "source1",
	  "scale": {
		"min": 3,
		"max": 6
	  },
	  "source": {
		"generator": {
		  "rpu": 10,
		  "duration": "2s"
		}
	  }
	},
	{
	  "name": "source2",
	  "source": {
		"http": {}
	  }
	},
	{
	  "name": "processor",
	  "scale": {
		"min": 2,
		"max": 5
	  },
	  "sink": {
		"log": {}
	  }
	}
  ]
}
`,
			expectError: false,
			expectedResultPipelineDef: `
{
  "vertices": [
	{
	  "name": "source1",
	  "scale": {
		"min": 0,
		"max": 0,
		"disabled": false
	  },
	  "source": {
		"generator": {
		  "rpu": 10,
		  "duration": "2s"
		}
	  }
	},
	{
	  "name": "source2",
	  "scale": {
		"min": 0,
		"max": 0,
		"disabled": false
	  },
	  "source": {
		"http": {}
	  }
	},
	{
	  "name": "processor",
	  "scale": {
		"min": 2,
		"max": 5,
		"disabled": false
	  },
	  "sink": {
		"log": {}
	  }
	}
  ]
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()

			// Create unstructured object with the pipeline definition
			obj := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var spec map[string]interface{}
			err := json.Unmarshal([]byte(tt.pipelineDef), &spec)
			assert.NoError(t, err)

			obj.Object["spec"] = spec
			obj.SetName("test-pipeline")
			obj.SetNamespace("test-namespace")

			// Call the function under test
			err = ScalePipelineDefSourceVerticesToZero(ctx, obj)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the result matches expected output
				expectedSpecMap := map[string]interface{}{}
				err = json.Unmarshal([]byte(tt.expectedResultPipelineDef), &expectedSpecMap)
				assert.NoError(t, err)
				assert.True(t, util.CompareStructNumTypeAgnostic(expectedSpecMap, obj.Object["spec"]))
			}
		})
	}
}

func Test_ApplyScaleValuesToPipelineDefinition(t *testing.T) {
	one := int64(1)
	five := int64(5)
	tests := []struct {
		name                      string
		pipelineDef               string
		vertexScaleDefinitions    []apiv1.VertexScaleDefinition
		expectError               bool
		expectedResultPipelineDef string
	}{
		{
			name: "various scale definitions",
			pipelineDef: `
{
	  "vertices": [
		{
		  "name": "in",
		  "scale": {
			"min": 1,
			"max": 5,
			"lookbackSeconds": 1
		  },
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "scale": {
		  },
		  "udf": {
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
			}
		  }
		},
		{
		  "name": "cat-2",
		  "udf": {
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ]
	
}
	  `,
			vertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: nil,
					},
				},
				{
					VertexName: "cat",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: &five,
					},
				},
				{
					VertexName: "cat-2",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: nil,
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Disabled: true,
					},
				},
			},
			expectError: false,
			expectedResultPipelineDef: `
	  
{
	  "vertices": [
		{
			"name": "in",
			"scale": {
				"lookbackSeconds": 1,
				"disabled": false
			},
			"source": {
				"generator": {
					"rpu": 5,
					"duration": "1s"
				}
			}
		},
		{
			"name": "cat",
			"scale": {
				"max": 5,
				"disabled": false
			},
			"udf": {
				"container": {
					"image": "quay.io/numaio/numaflow-go/map-cat:stable",
					"imagePullPolicy": "Always,"
				}
			}
		},
		{
			"name": "cat-2",
			"scale": {
				"min": 1,
				"disabled": false
			},
			"udf": {
				"container": {
					"image": "quay.io/numaio/numaflow-go/map-cat:stable",
					"imagePullPolicy": "Always,"
				}
			}
		},
		{
			"name": "out",
			"scale": {
				"disabled": true
			},
			"sink": {
			"log": {}
			}
		}
	  ]
	
}
	  `,
		},
		{
			name: "invalid vertex names", // this should just issue a warning
			pipelineDef: `
{
	  "vertices": [
		{
		  "name": "in",
		  "scale": {
			"min": 1,
			"max": 5
		  },
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
	  ]
	
}
	  `,
			vertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: nil,
					},
				},
				{
					VertexName: "cat",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: &five,
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: &five,
					},
				},
			},
			expectError: false,
			expectedResultPipelineDef: `
	  
{
	  "vertices": [
		{
			"name": "in",
			"scale": {
				"disabled": false
			},
			"source": {
			"generator": {
				"rpu": 5,
				"duration": "1s"
			}
			}
		},
		{
			"name": "out",
			"scale": {
				"min": 1,
				"max": 5,
				"disabled": false
			},
			"sink": {
			"log": {}
			}
		}
]
	
}
	  `,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()

			obj := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var spec map[string]interface{}
			err := json.Unmarshal([]byte(tt.pipelineDef), &spec)
			assert.NoError(t, err)

			obj.Object["spec"] = spec

			err = ApplyScaleValuesToPipelineDefinition(ctx, obj, tt.vertexScaleDefinitions)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				expectedPipelineSpecMap := make(map[string]interface{})
				err := json.Unmarshal([]byte(tt.expectedResultPipelineDef), &expectedPipelineSpecMap)
				assert.NoError(t, err)
				assert.True(t, util.CompareStructNumTypeAgnostic(expectedPipelineSpecMap, obj.Object["spec"]))
			}
		})
	}

}

func Test_ApplyScaleValuesToLivePipeline(t *testing.T) {

	_, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)

	ctx := context.Background()
	oneInt64 := int64(1)
	fiveInt64 := int64(5)

	testCases := []struct {
		name                   string
		existingPipelineSpec   string
		vertexScaleDefinitions []apiv1.VertexScaleDefinition
		expectError            bool
		expectedPipelineSpec   string
	}{
		{
			name: "variety of vertices",
			existingPipelineSpec: `

			{
				  "vertices": [
					{
						"name": "in",
						"scale": {
							"lookbackSeconds": 1,
							"min": 1,
							"max": 5
						},
						"source": {
						"generator": {
							"rpu": 5,
							"duration": "1s"
							}
						}
					},
					{
						"name": "cat",
						"scale": {
							"lookbackSeconds": 1
						},
						"udf": {
							"container": {
								"image": "quay.io/numaio/numaflow-go/map-cat:stable",
								"imagePullPolicy": "Always,"
							}
						}
					},
					{
						"name": "out",
						"sink": {
							"log": {}
						}
					}
				  ]

			}
				  `,
			vertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min:      nil,
						Max:      nil,
						Disabled: true,
					},
				},
				{
					VertexName: "cat",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: &fiveInt64,
					},
				},
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &oneInt64,
						Max: &fiveInt64,
					},
				},
			},
			expectedPipelineSpec: `

			{
				  "vertices": [
					{
						"name": "in",
						"scale": {
							"lookbackSeconds": 1,
							"min": null,
							"max": null,
							"disabled": true
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					},
					{
						"name": "cat",
						"scale": {
							"lookbackSeconds": 1,
							"min": null,
							"max": 5,
							"disabled": false
						},
						"udf": {
							"container": {
								"image": "quay.io/numaio/numaflow-go/map-cat:stable",
								"imagePullPolicy": "Always,"
							}
						}
					},
					{
						"name": "out",
						"scale": {
							"min": 1,
							"max": 5,
							"disabled": false
						},
						"sink": {
							"log": {}
						}
					}
				  ]

			}
				  `},

		{
			name: "partial set of vertices passed in",
			existingPipelineSpec: `
	
				{
						"vertices": [
						{
							"name": "in",
							"scale": {
								"lookbackSeconds": 1,
								"min": 1,
								"max": 5
							},
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
						]
	
				}
						`,
			vertexScaleDefinitions: []apiv1.VertexScaleDefinition{
				{
					VertexName: "out",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &oneInt64,
						Max: &fiveInt64,
					},
				},
			},
			expectedPipelineSpec: `

				{
					  "vertices": [
						{
							"name": "in",
							"scale": {
								"lookbackSeconds": 1,
								"min": 1,
								"max": 5
							},
							"source": {
								"generator": {
									"rpu": 5,
									"duration": "1s"
								}
							}
						},
						{
							"name": "out",
							"scale": {
								"min": 1,
								"max": 5,
								"disabled": false
							},
							"sink": {
								"log": {}
							}
						}
					  ]
	
				}
					  `},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {
			_ = numaflowClientSet.NumaflowV1alpha1().Pipelines(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			// Create an Unstructured Pipeline based on our spec in K8S
			pipelineDef, err := ctlrcommon.CreateTestPipelineUnstructured(ctlrcommon.DefaultTestPipelineName, tc.existingPipelineSpec)
			assert.NoError(t, err)

			fmt.Printf("pipelineDef=%+v\n", pipelineDef)
			err = kubernetes.CreateResource(ctx, client, pipelineDef)
			assert.NoError(t, err)

			pipeline, err := kubernetes.GetResource(ctx, client, numaflowv1.PipelineGroupVersionKind,
				k8stypes.NamespacedName{Name: ctlrcommon.DefaultTestPipelineName, Namespace: ctlrcommon.DefaultTestNamespace})
			assert.NoError(t, err)

			err = ApplyScaleValuesToLivePipeline(ctx, pipeline, tc.vertexScaleDefinitions, client)
			assert.NoError(t, err)

			// get the result pipeline after the patch
			resultPipeline, err := kubernetes.GetResource(ctx, client, numaflowv1.PipelineGroupVersionKind,
				k8stypes.NamespacedName{Name: ctlrcommon.DefaultTestPipelineName, Namespace: ctlrcommon.DefaultTestNamespace})
			assert.NoError(t, err)

			expectedSpecMap := map[string]interface{}{}
			err = json.Unmarshal([]byte(tc.expectedPipelineSpec), &expectedSpecMap)
			assert.NoError(t, err)
			assert.True(t, util.CompareStructNumTypeAgnostic(expectedSpecMap, resultPipeline.Object["spec"]))

		})
	}
}

func TestGetScaleValuesFromPipelineSpec(t *testing.T) {
	one := int64(1)
	five := int64(5)
	tests := []struct {
		name           string
		pipelineDef    string
		expectedResult []apiv1.VertexScaleDefinition
		expectError    bool
	}{
		{
			name: "Valid pipeline spec with various scale definitions",
			pipelineDef: `
{
	  "vertices": [
		{
		  "name": "in",
		  "scale": {
			"min": 1,
			"max": 5
		  },
		  "source": {
			"generator": {
			  "rpu": 5,
			  "duration": "1s"
			}
		  }
		},
		{
		  "name": "cat",
		  "scale": {
		  },
		  "udf": {
			"container": {
				"image": "quay.io/numaio/numaflow-go/map-cat:stable",
				"imagePullPolicy": "Always,"
			}
		  }
		},
		{
		  "name": "out",
		  "sink": {
			"log": {}
		  }
		}
	  ]
	
}
			`,
			expectedResult: []apiv1.VertexScaleDefinition{
				{
					VertexName: "in",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: &one,
						Max: &five,
					},
				},
				{
					VertexName: "cat",
					ScaleDefinition: &apiv1.ScaleDefinition{
						Min: nil,
						Max: nil,
					},
				},
				{
					VertexName:      "out",
					ScaleDefinition: nil,
				},
			},

			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()

			obj := &unstructured.Unstructured{Object: make(map[string]interface{})}
			var spec map[string]interface{}
			err := json.Unmarshal([]byte(tt.pipelineDef), &spec)
			assert.NoError(t, err)

			obj.Object["spec"] = spec

			result, err := GetScaleValuesFromPipelineDefinition(ctx, obj)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func Test_CheckPipelineScaledToZero(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		pipelineSpec   string
		expectedResult bool
		expectError    bool
	}{
		{
			name: "All vertices scaled to zero",
			pipelineSpec: `{
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 0,
							"max": 0
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					},
					{
						"name": "cat",
						"scale": {
							"min": 0,
							"max": 0
						},
						"udf": {
							"container": {
								"image": "quay.io/numaio/numaflow-go/map-cat:stable",
								"imagePullPolicy": "Always,"
							}
						}
					},
					{
						"name": "out",
						"scale": {
							"min": 0,
							"max": 0
						},
						"sink": {
							"log": {}
						}
					}
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Some vertices not scaled to zero",
			pipelineSpec: `{
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 0,
							"max": 0
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					},
					{
						"name": "cat",
						"scale": {
							"min": 0,
							"max": 3
						},
						"udf": {
							"container": {
								"image": "quay.io/numaio/numaflow-go/map-cat:stable",
								"imagePullPolicy": "Always,"
							}
						}
					},
					{
						"name": "out",
						"scale": {
							"min": 0,
							"max": 0
						},
						"sink": {
							"log": {}
						}
					}
				]
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "Vertex with nil scale definition",
			pipelineSpec: `{
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 0,
							"max": 0
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					},
					{
						"name": "cat",
						"udf": {
							"container": {
								"image": "quay.io/numaio/numaflow-go/map-cat:stable",
								"imagePullPolicy": "Always,"
							}
						}
					}
				]
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "Vertex with unset min and max",
			pipelineSpec: `{
				"vertices": [
					{
						"name": "in",
						"scale": {
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					}
				]
			}`,
			expectedResult: false,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create unstructured pipeline object
			pipeline, err := ctlrcommon.CreateTestPipelineUnstructured("test-pipeline", tt.pipelineSpec)
			assert.NoError(t, err)

			// Call the function under test
			result, err := CheckPipelineScaledToZero(ctx, pipeline)

			// Verify results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result, "Expected scaled to zero result to be %v, got %v", tt.expectedResult, result)
			}
		})
	}
}

func Test_CanPipelineIngestData(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		pipelineSpec   string
		expectedResult bool
		expectError    bool
	}{
		{
			name: "Pipeline set to ingest data - source vertex with max > 0 and desiredPhase=Running",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 1,
							"max": 3
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					},
					{
						"name": "out",
						"scale": {
							"min": 1,
							"max": 2
						},
						"sink": {
							"log": {}
						}
					}
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Pipeline set to ingest data - source vertex with max > 0 and no desiredPhase (defaults to Running)",
			pipelineSpec: `{
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 0,
							"max": 1
						},
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
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Pipeline NOT set to ingest data - source vertex with max = 0",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 0,
							"max": 0
						},
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
				]
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "Pipeline NOT set to ingest data - desiredPhase=Paused",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Paused"
				},
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 1,
							"max": 3
						},
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
				]
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "Pipeline set to ingest data - source vertex with no scale definition (defaults to max=1)",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
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
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Pipeline set to ingest data - multiple source vertices, one with max > 0",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"vertices": [
					{
						"name": "in1",
						"scale": {
							"min": 0,
							"max": 0
						},
						"source": {
							"generator": {
								"rpu": 5,
								"duration": "1s"
							}
						}
					},
					{
						"name": "in2",
						"scale": {
							"min": 1,
							"max": 2
						},
						"source": {
							"generator": {
								"rpu": 3,
								"duration": "2s"
							}
						}
					},
					{
						"name": "out",
						"sink": {
							"log": {}
						}
					}
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Pipeline NOT set to ingest data - source vertex with max = 0 and desiredPhase=Paused",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Paused"
				},
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 0,
							"max": 0
						},
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
				]
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "Pipeline can ingest data - empty lifecycle (defaults to Running)",
			pipelineSpec: `{
				"lifecycle": {},
				"vertices": [
					{
						"name": "in",
						"scale": {
							"min": 1,
							"max": 3
						},
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
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Pipeline can ingest data - empty scale on source vertex (defaults to max = 1)",
			pipelineSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"vertices": [
					{
						"name": "in",
						"scale": {},
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
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "Pipeline can ingest data - scale and lifecycle unset (defaults to Running and max = 1)",
			pipelineSpec: `{
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
				]
			}`,
			expectedResult: true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create unstructured pipeline object
			pipeline, err := ctlrcommon.CreateTestPipelineUnstructured("test-pipeline", tt.pipelineSpec)
			assert.NoError(t, err)

			// Call the function under test
			result, err := CanPipelineIngestData(ctx, pipeline)

			// Verify results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result, "Expected result to be %v, got %v", tt.expectedResult, result)
			}
		})
	}
}

func Test_GenerateAndApplyFullScaleDefinitions(t *testing.T) {
	// Pipeline with a mix of vertices:
	// - "in": scale undefined
	// - "cat": scale defined but empty (no children)
	// - "cat-2": scale defined with some fields (min, max)
	// - "out": scale defined with all fields including disabled
	pipelineSpec := `
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
			"name": "cat",
			"scale": {},
			"udf": {
				"container": {
					"image": "quay.io/numaio/numaflow-go/map-cat:stable"
				}
			}
		},
		{
			"name": "cat-2",
			"scale": {
				"min": 2,
				"max": 10
			},
			"udf": {
				"container": {
					"image": "quay.io/numaio/numaflow-go/map-cat:stable"
				}
			}
		},
		{
			"name": "out",
			"scale": {
				"min": 1,
				"max": 5,
				"disabled": true,
				"lookbackSeconds": 120
			},
			"sink": {
				"log": {}
			}
		}
	],
	"edges": [
		{"from": "in", "to": "cat"},
		{"from": "cat", "to": "cat-2"},
		{"from": "cat-2", "to": "out"}
	]
}`

	// Parse the pipeline spec into a map and wrap it in a "spec" key
	// (to match the structure of pipelineDef.Object)
	var specMap map[string]interface{}
	err := json.Unmarshal([]byte(pipelineSpec), &specMap)
	assert.NoError(t, err)

	originalPipelineMap := map[string]interface{}{
		"spec": specMap,
	}

	// Make a deep copy for modification
	var pipelineMapCopy map[string]interface{}
	err = util.StructToStruct(originalPipelineMap, &pipelineMapCopy)
	assert.NoError(t, err)

	// Step 1: Generate full scale definitions from the pipeline map
	scaleDefinitions, err := GenerateFullScaleDefinitionsFromPipelineMap(originalPipelineMap)
	assert.NoError(t, err)
	assert.Len(t, scaleDefinitions, 4, "Expected 4 scale definitions, one per vertex")

	// Verify the scale definitions
	// Vertex 0 ("in"): no scale defined
	assert.Equal(t, "null", scaleDefinitions[0], "Vertex 'in' should have null scale")

	// Vertex 1 ("cat"): scale defined but empty
	var scale1 map[string]interface{}
	err = json.Unmarshal([]byte(scaleDefinitions[1]), &scale1)
	assert.NoError(t, err)
	assert.Empty(t, scale1, "Vertex 'cat' should have empty scale object")

	// Vertex 2 ("cat-2"): scale with min and max
	var scale2 map[string]interface{}
	err = json.Unmarshal([]byte(scaleDefinitions[2]), &scale2)
	assert.NoError(t, err)
	assert.NotNil(t, scale2["min"], "Vertex 'cat-2' should have min")
	assert.NotNil(t, scale2["max"], "Vertex 'cat-2' should have max")

	// Vertex 3 ("out"): scale with all fields
	var scale3 map[string]interface{}
	err = json.Unmarshal([]byte(scaleDefinitions[3]), &scale3)
	assert.NoError(t, err)
	assert.NotNil(t, scale3["min"], "Vertex 'out' should have min")
	assert.NotNil(t, scale3["max"], "Vertex 'out' should have max")
	assert.NotNil(t, scale3["disabled"], "Vertex 'out' should have disabled")
	assert.NotNil(t, scale3["lookbackSeconds"], "Vertex 'out' should have lookbackSeconds")

	// Step 2: Apply the scale definitions back to the copy
	err = ApplyFullScaleDefinitionsToPipelineMap(pipelineMapCopy, scaleDefinitions)
	assert.NoError(t, err)

	// Step 3: Verify the original and modified pipeline maps are equal
	assert.Equal(t, originalPipelineMap, pipelineMapCopy,
		"Pipeline map should be unchanged after generate and apply round-trip")
}
