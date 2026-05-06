package numaflowtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestValidatePipelineSpec(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid pipeline spec",
			raw: `{
				"interStepBufferServiceName": "default",
				"vertices": [
					{"name": "in", "source": {"generator": {"rpu": 5, "duration": "1s"}}},
					{"name": "out", "sink": {"log": {}}}
				],
				"edges": [{"from": "in", "to": "out"}]
			}`,
			wantErr: false,
		},
		{
			name: "valid pipeline spec with watermark",
			raw: `{
				"interStepBufferServiceName": "default",
				"watermark": {"maxDelay": "1s"},
				"vertices": [
					{"name": "in", "source": {"generator": {"rpu": 5, "duration": "1s"}}},
					{"name": "out", "sink": {"log": {}}}
				],
				"edges": [{"from": "in", "to": "out"}]
			}`,
			wantErr: false,
		},
		{
			name: "invalid duration - missing unit",
			raw: `{
				"interStepBufferServiceName": "default",
				"watermark": {"maxDelay": "1"},
				"vertices": [
					{"name": "in", "source": {"generator": {"rpu": 5, "duration": "1s"}}},
					{"name": "out", "sink": {"log": {}}}
				],
				"edges": [{"from": "in", "to": "out"}]
			}`,
			wantErr: true,
			errMsg:  "invalid Pipeline spec",
		},
		{
			name: "invalid duration in generator source",
			raw: `{
				"interStepBufferServiceName": "default",
				"vertices": [
					{"name": "in", "source": {"generator": {"rpu": 5, "duration": "1"}}},
					{"name": "out", "sink": {"log": {}}}
				],
				"edges": [{"from": "in", "to": "out"}]
			}`,
			wantErr: true,
			errMsg:  "invalid Pipeline spec",
		},
		{
			name:    "malformed JSON",
			raw:     `{invalid json}`,
			wantErr: true,
			errMsg:  "invalid Pipeline spec",
		},
		{
			name: "unknown fields are silently ignored",
			raw: `{
				"interStepBufferServiceName": "default",
				"someUnknownField": "value",
				"vertices": [
					{"name": "in", "source": {"generator": {"rpu": 5, "duration": "1s"}}},
					{"name": "out", "sink": {"log": {}}}
				],
				"edges": [{"from": "in", "to": "out"}]
			}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawSpec := runtime.RawExtension{Raw: []byte(tt.raw)}
			err := ValidatePipelineSpec(rawSpec)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateMonoVertexSpec(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid monovertex spec",
			raw: `{
				"source": {
					"udsource": {
						"container": {"image": "quay.io/numaio/numaflow-go/source-simple-source:stable"}
					}
				},
				"sink": {
					"udsink": {
						"container": {"image": "quay.io/numaio/numaflow-go/sink-log:stable"}
					}
				}
			}`,
			wantErr: false,
		},
		{
			name: "valid monovertex with limits",
			raw: `{
				"limits": {"readTimeout": "2s"},
				"source": {
					"udsource": {
						"container": {"image": "source:latest"}
					}
				},
				"sink": {
					"udsink": {
						"container": {"image": "sink:latest"}
					}
				}
			}`,
			wantErr: false,
		},
		{
			name: "invalid duration in readTimeout",
			raw: `{
				"limits": {"readTimeout": "1"},
				"source": {
					"udsource": {
						"container": {"image": "source:latest"}
					}
				},
				"sink": {
					"udsink": {
						"container": {"image": "sink:latest"}
					}
				}
			}`,
			wantErr: true,
			errMsg:  "invalid MonoVertex spec",
		},
		{
			name:    "malformed JSON",
			raw:     `not valid json`,
			wantErr: true,
			errMsg:  "invalid MonoVertex spec",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawSpec := runtime.RawExtension{Raw: []byte(tt.raw)}
			err := ValidateMonoVertexSpec(rawSpec)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateISBServiceSpec(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid jetstream spec",
			raw: `{
				"jetstream": {
					"version": "2.10.3",
					"persistence": {"volumeSize": "1Gi"}
				}
			}`,
			wantErr: false,
		},
		{
			name: "valid redis spec",
			raw: `{
				"redis": {
					"native": {"version": "7.0.15"}
				}
			}`,
			wantErr: false,
		},
		{
			name:    "malformed JSON",
			raw:     `{bad`,
			wantErr: true,
			errMsg:  "invalid ISBService spec",
		},
		{
			name: "unknown fields are silently ignored",
			raw: `{
				"jetstream": {"version": "2.10.3"},
				"futureField": "value"
			}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawSpec := runtime.RawExtension{Raw: []byte(tt.raw)}
			err := ValidateISBServiceSpec(rawSpec)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
