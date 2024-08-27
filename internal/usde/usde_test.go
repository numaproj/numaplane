package usde

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

var pipelineSpecSourceRPU = int64(5)
var pipelineSpecSourceDuration = metav1.Duration{
	Duration: time.Second,
}

var pipelineSpec = numaflowv1.PipelineSpec{
	InterStepBufferServiceName: "my-isbsvc",
	Vertices: []numaflowv1.AbstractVertex{
		{
			Name: "in",
			Source: &numaflowv1.Source{
				Generator: &numaflowv1.GeneratorSource{
					RPU:      &pipelineSpecSourceRPU,
					Duration: &pipelineSpecSourceDuration,
				},
			},
		},
		{
			Name: "cat",
			UDF: &numaflowv1.UDF{
				Builtin: &numaflowv1.Function{
					Name: "cat",
				},
			},
		},
		{
			Name: "out",
			Sink: &numaflowv1.Sink{
				AbstractSink: numaflowv1.AbstractSink{
					Log: &numaflowv1.Log{},
				},
			},
		},
	},
	Edges: []numaflowv1.Edge{
		{
			From: "in",
			To:   "cat",
		},
		{
			From: "cat",
			To:   "out",
		},
	},
}

var pipelineSpecRaw, _ = json.Marshal(pipelineSpec)

var prs = apiv1.PipelineRolloutSpec{
	Pipeline: apiv1.Pipeline{
		Spec: runtime.RawExtension{
			Raw: pipelineSpecRaw,
		},
	},
}

var defaultNewSpec = kubernetes.GenericObject{
	TypeMeta: metav1.TypeMeta{
		Kind:       "Pipeline",
		APIVersion: "numaflow.numaproj.io/v1alpha1",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test-pipeline",
		Namespace: "default",
	},
	Spec: prs.Pipeline.Spec,
}

// var defaultExistingSpec kubernetes.GenericObject = kubernetes.GenericObject{}

func Test_GetUpgradeStrategy(t *testing.T) {
	configManager := config.GetConfigManagerInstance()

	testCases := []struct {
		name             string
		newSpec          kubernetes.GenericObject
		existingSpec     kubernetes.GenericObject
		usdeConfig       config.USDEConfig
		expectedStrategy UpgradeStrategy
	}{
		{
			name:         "empty pipeline spec excluded paths",
			newSpec:      defaultNewSpec,
			existingSpec: defaultNewSpec,
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{},
			},
			expectedStrategy: UpgradeStrategyNoOp,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configManager.UpdateUSDEConfig(tc.usdeConfig)
			strategy, err := GetUpgradeStrategy(&tc.newSpec, &tc.existingSpec)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedStrategy, strategy)
		})
	}
}
