package usde

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
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

var volSize, _ = apiresource.ParseQuantity("10Mi")
var memLimit, _ = apiresource.ParseQuantity("10Mi")
var newMemLimit, _ = apiresource.ParseQuantity("20Mi")
var defaultISBServiceSpec = numaflowv1.InterStepBufferServiceSpec{
	Redis: nil,
	JetStream: &numaflowv1.JetStreamBufferService{
		Version: "2.9.6",
		Persistence: &numaflowv1.PersistenceStrategy{
			VolumeSize: &volSize,
		},
		// ContainerTemplate: &numaflowv1.ContainerTemplate{
		// 	Resources: v1.ResourceRequirements{
		// 		Limits: v1.ResourceList{v1.ResourceMemory: memLimit},
		// 	},
		// },
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

func makeISBServiceDefinition(isbServiceSpec numaflowv1.InterStepBufferServiceSpec) kubernetes.GenericObject {
	isbServiceSpecRaw, _ := json.Marshal(isbServiceSpec)

	isbrs := apiv1.ISBServiceRolloutSpec{
		InterStepBufferService: apiv1.InterStepBufferService{
			Spec: runtime.RawExtension{
				Raw: isbServiceSpecRaw,
			},
		},
	}

	return kubernetes.GenericObject{
		TypeMeta: metav1.TypeMeta{
			Kind:       "InterStepBufferService",
			APIVersion: "numaflow.numaproj.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-isbsvc",
			Namespace: defaultNamespace,
		},
		Spec: isbrs.InterStepBufferService.Spec,
	}

}

func Test_ResourceNeedsUpdating(t *testing.T) {
	ctx := context.Background()

	configManager := config.GetConfigManagerInstance()

	pipelineDefn := makePipelineDefinition(defaultPipelineSpec)
	isbServiceDefn := makeISBServiceDefinition(defaultISBServiceSpec)

	testCases := []struct {
		name                  string
		newSpec               kubernetes.GenericObject
		existingSpec          kubernetes.GenericObject
		usdeConfig            config.USDEConfig
		namespaceConfig       *config.NamespaceConfig
		expectedNeedsUpdating bool
		expectedStrategy      apiv1.UpgradeStrategy
	}{
		{
			name:         "empty pipeline spec excluded paths",
			newSpec:      pipelineDefn,
			existingSpec: pipelineDefn,
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: false,
			expectedStrategy:      apiv1.UpgradeStrategyNoOp,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name:         "only exclude interStepBufferServiceName field (NOT changed)",
			newSpec:      pipelineDefn,
			existingSpec: pipelineDefn,
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: false,
			expectedStrategy:      apiv1.UpgradeStrategyNoOp,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "invalid"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator.rpu"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name:    "with changes in array deep map but excluded parent",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newRPU := int64(10)
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.InterStepBufferServiceName = "changed-isbsvc"
				newPipelineSpec.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
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
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator.rpu"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:    "with changes in array deep map - detect pointer fields",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[2].Sink.Log = nil
				newPipelineSpec.Vertices[2].Sink.Blackhole = &numaflowv1.Blackhole{}
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"vertices.sink.log"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:    "with changes in array deep map - detect pointer fields - parent field is excluded",
			newSpec: pipelineDefn,
			existingSpec: func() kubernetes.GenericObject {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.Vertices[2].Sink.Log = nil
				newPipelineSpec.Vertices[2].Sink.Blackhole = &numaflowv1.Blackhole{}
				return makePipelineDefinition(*newPipelineSpec)
			}(),
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"vertices"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name:         "excluded paths not found",
			newSpec:      pipelineDefn,
			existingSpec: pipelineDefn,
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"vertices.source.something"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: false,
			expectedStrategy:      apiv1.UpgradeStrategyNoOp,
		},
		{
			name:    "isb test",
			newSpec: isbServiceDefn,
			existingSpec: func() kubernetes.GenericObject {
				newISBServiceSpec := defaultISBServiceSpec.DeepCopy()
				newISBServiceSpec.JetStream.ContainerTemplate = &numaflowv1.ContainerTemplate{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{v1.ResourceMemory: memLimit},
					},
				}
				return makeISBServiceDefinition(*newISBServiceSpec)
			}(),
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:      config.PPNDStrategyID,
				PipelineSpecExcludedPaths:   []string{"vertices.source.something"},
				ISBServiceSpecExcludedPaths: []string{"jetstream.containerTemplate.resources.limits"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configManager.UpdateUSDEConfig(tc.usdeConfig)
			if tc.namespaceConfig != nil {
				configManager.UpdateNamespaceConfig(defaultNamespace, *tc.namespaceConfig)
			} else {
				configManager.UnsetNamespaceConfig(defaultNamespace)
			}

			needsUpdating, strategy, err := ResourceNeedsUpdating(ctx, &tc.newSpec, &tc.existingSpec)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNeedsUpdating, needsUpdating)
			assert.Equal(t, tc.expectedStrategy, strategy)
		})
	}
}
