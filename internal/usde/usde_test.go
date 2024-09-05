package usde

import (
	"context"
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

const defaultNamespace = "default"

var pipelineSpecSourceRPU = int64(5)
var pipelineSpecSourceDuration = metav1.Duration{Duration: 2 * time.Second}
var defaultPipelineSpec = numaflowv1.PipelineSpec{
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

func makePipelineDefinition(pipelineSpec numaflowv1.PipelineSpec) kubernetes.GenericObject {
	pipelineSpecRaw, _ := json.Marshal(pipelineSpec)

	prs := apiv1.PipelineRolloutSpec{
		Pipeline: apiv1.Pipeline{
			Spec: runtime.RawExtension{
				Raw: pipelineSpecRaw,
			},
		},
	}

	return kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pipeline",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline",
			Namespace: defaultNamespace,
		},
		Spec: prs.Pipeline.Spec,
	}
}

func Test_GetUpgradeStrategy(t *testing.T) {
	ctx := context.Background()

	configManager := config.GetConfigManagerInstance()

	pipelineDefn := makePipelineDefinition(defaultPipelineSpec)

	testCases := []struct {
		name             string
		newSpec          kubernetes.GenericObject
		existingSpec     kubernetes.GenericObject
		usdeConfig       config.USDEConfig
		namespaceConfig  config.NamespaceConfig
		expectedStrategy UpgradeStrategy
	}{
		{
			name:         "empty pipeline spec excluded paths",
			newSpec:      pipelineDefn,
			existingSpec: pipelineDefn,
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{},
			},
			namespaceConfig:  config.NamespaceConfig{},
			expectedStrategy: UpgradeStrategyNoOp,
		},
		{
			name:    "empty pipeline spec excluded paths and change interStepBufferServiceName field",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.InterStepBufferServiceName = "changed-isbsvc"
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{},
			},
			namespaceConfig:  config.NamespaceConfig{},
			expectedStrategy: UpgradeStrategyPPND, // TODO-PROGRESSIVE: the strategy should be UpgradeStrategyProgressive instead of UpgradeStrategyPPND
		},
		{
			name:    "only exclude interStepBufferServiceName field (changed)",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.InterStepBufferServiceName = "changed-isbsvc"
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:  config.NamespaceConfig{},
			expectedStrategy: UpgradeStrategyApply,
		},
		{
			name:         "only exclude interStepBufferServiceName field (NOT changed)",
			newSpec:      pipelineDefn,
			existingSpec: pipelineDefn,
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:  config.NamespaceConfig{},
			expectedStrategy: UpgradeStrategyNoOp,
		},
		{
			name:    "only exclude interStepBufferServiceName field and change some other field (no user strategy)",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:  config.NamespaceConfig{},
			expectedStrategy: UpgradeStrategyPPND, // TODO-PROGRESSIVE: the strategy should be UpgradeStrategyProgressive instead of UpgradeStrategyPPND
		},
		{
			name:    "only exclude interStepBufferServiceName field and change some other field (with empty user strategy)",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:  config.NamespaceConfig{UpgradeStrategy: ""},
			expectedStrategy: UpgradeStrategyPPND, // TODO-PROGRESSIVE: the strategy should be UpgradeStrategyProgressive instead of UpgradeStrategyPPND
		},
		{
			name:    "only exclude interStepBufferServiceName field and change some other field (with invalid user strategy)",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:  config.NamespaceConfig{UpgradeStrategy: "invalid"},
			expectedStrategy: UpgradeStrategyPPND, // TODO-PROGRESSIVE: the strategy should be UpgradeStrategyProgressive instead of UpgradeStrategyPPND
		},
		{
			name:    "only exclude interStepBufferServiceName field and change some other field (with valid user strategy)",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:  config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedStrategy: UpgradeStrategyPPND,
		},
		{
			name:    "with changes in array deep map but excluded",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newRPU := int64(10)
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.InterStepBufferServiceName = "changed-isbsvc"
				newPipelineSpec.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator.rpu"},
			},
			namespaceConfig:  config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedStrategy: UpgradeStrategyApply,
		},
		{
			name:    "with changes in array deep map but one is NOT excluded",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newRPU := int64(10)
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[0].Name = "new-vtx-name"
				newPipelineSpec.InterStepBufferServiceName = "changed-isbsvc"
				newPipelineSpec.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator.rpu"},
			},
			namespaceConfig:  config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedStrategy: UpgradeStrategyPPND,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configManager.UpdateUSDEConfig(tc.usdeConfig)
			configManager.UpdateNamespaceConfig(defaultNamespace, tc.namespaceConfig)
			// TODO: write test cases with various values for inProgressUpgradeStrategy and override arguments instead of empty string and nils.
			// Also, include testing the boolean returned value specsDiffer
			strategy, _, err := DeriveUpgradeStrategy(ctx, &tc.newSpec, &tc.existingSpec, "", nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedStrategy, strategy)
		})
	}
}
