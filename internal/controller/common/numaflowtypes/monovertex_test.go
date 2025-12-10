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

package numaflowtypes

import (
	"context"
	"encoding/json"
	"testing"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
)

func createTestMonoVertexUnstructured(name string, spec string) (*unstructured.Unstructured, error) {
	monoVertexDef := &unstructured.Unstructured{Object: make(map[string]interface{})}
	monoVertexDef.SetGroupVersionKind(numaflowv1.MonoVertexGroupVersionKind)
	monoVertexDef.SetName(name)
	monoVertexDef.SetNamespace(ctlrcommon.DefaultTestNamespace)
	var monoVertexSpec map[string]interface{}
	if err := json.Unmarshal([]byte(spec), &monoVertexSpec); err != nil {
		return nil, err
	}
	monoVertexDef.Object["spec"] = monoVertexSpec

	return monoVertexDef, nil
}

func Test_CanMonoVertexIngestData(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		monoVertexSpec string
		expectedResult bool
		expectError    bool
	}{
		{
			name: "MonoVertex can ingest data - scale.max > 0 and desiredPhase=Running",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"scale": {
					"min": 1,
					"max": 3
				},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "MonoVertex can ingest data - scale unset (max defaults to 1) and no desiredPhase (defaults to Running)",
			monoVertexSpec: `{
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "MonoVertex cannot ingest data - scale.max = 0",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"scale": {
					"min": 0,
					"max": 0
				},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "MonoVertex cannot ingest data - desiredPhase=Paused",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Paused"
				},
				"scale": {
					"min": 1,
					"max": 3
				},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "MonoVertex can ingest data - no scale definition (defaults to max = 1)",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "MonoVertex cannot ingest data - scale.max = 0 and desiredPhase=Paused",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Paused"
				},
				"scale": {
					"min": 0,
					"max": 0
				},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: false,
			expectError:    false,
		},
		{
			name: "MonoVertex can ingest data - empty lifecycle (defaults to Running)",
			monoVertexSpec: `{
				"lifecycle": {},
				"scale": {
					"min": 2,
					"max": 4
				},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: true,
			expectError:    false,
		},
		{
			name: "MonoVertex can ingest data - empty scale (defaults to max = 1)",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"scale": {},
				"source": {
					"udsource": {
						"container": {
							"image": "quay.io/numaio/numaflow-rs/simple-source:stable"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedResult: true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create unstructured monovertex object
			monoVertex, err := createTestMonoVertexUnstructured("test-monovertex", tt.monoVertexSpec)
			assert.NoError(t, err)

			// Call the function under test
			result, err := CanMonoVertexIngestData(ctx, monoVertex)

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

func Test_GetMonoVertexDesiredPhase(t *testing.T) {
	tests := []struct {
		name           string
		monoVertexSpec string
		expectedPhase  string
		expectError    bool
	}{
		{
			name: "Explicit Running phase",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Running"
				},
				"source": {
					"udsource": {
						"container": {
							"image": "test"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedPhase: "Running",
			expectError:   false,
		},
		{
			name: "Explicit Paused phase",
			monoVertexSpec: `{
				"lifecycle": {
					"desiredPhase": "Paused"
				},
				"source": {
					"udsource": {
						"container": {
							"image": "test"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedPhase: "Paused",
			expectError:   false,
		},
		{
			name: "No desiredPhase - defaults to Running",
			monoVertexSpec: `{
				"lifecycle": {},
				"source": {
					"udsource": {
						"container": {
							"image": "test"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedPhase: "Running",
			expectError:   false,
		},
		{
			name: "No lifecycle - defaults to Running",
			monoVertexSpec: `{
				"source": {
					"udsource": {
						"container": {
							"image": "test"
						}
					}
				},
				"sink": {
					"blackhole": {}
				}
			}`,
			expectedPhase: "Running",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create unstructured monovertex object
			monoVertex, err := createTestMonoVertexUnstructured("test-monovertex", tt.monoVertexSpec)
			assert.NoError(t, err)

			// Call the function under test
			phase, err := GetMonoVertexDesiredPhase(monoVertex)

			// Verify results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPhase, phase)
			}
		})
	}
}
