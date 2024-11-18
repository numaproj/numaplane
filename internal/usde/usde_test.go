package usde

import (
	"context"
	"encoding/json"
<<<<<<< HEAD
	"os"
	"path/filepath"
=======
	"log"
>>>>>>> chandankumar4/controller_refactor
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
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

var existingPipelineSpec1 = numaflowv1.PipelineSpec{
	InterStepBufferServiceName: "my-isbsvc",
	Vertices: []numaflowv1.AbstractVertex{
		{
			Name:   "v1",
			Source: &numaflowv1.Source{},
		},
		{
			Name:   "v2",
			Source: nil,
		},
	},
}

var existingPipelineSpec2 = numaflowv1.PipelineSpec{
	InterStepBufferServiceName: "my-isbsvc",
	Vertices: []numaflowv1.AbstractVertex{
		{
			Name: "v1",
			Source: &numaflowv1.Source{
				Generator: &numaflowv1.GeneratorSource{},
			},
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
		ContainerTemplate: &numaflowv1.ContainerTemplate{
			Resources: v1.ResourceRequirements{
				Limits: v1.ResourceList{v1.ResourceMemory: memLimit},
			},
		},
	},
}

func makePipelineDefinition(pipelineSpec numaflowv1.PipelineSpec) unstructured.Unstructured {
	pipelineSpecRaw, _ := json.Marshal(pipelineSpec)

	prs := apiv1.PipelineRolloutSpec{
		Pipeline: apiv1.Pipeline{
			Spec: runtime.RawExtension{
				Raw: pipelineSpecRaw,
			},
		},
	}

	pipelineDef := unstructured.Unstructured{Object: make(map[string]interface{})}
	pipelineDef.SetGroupVersionKind(numaflowv1.PipelineGroupVersionKind)
	pipelineDef.SetName("test-pipeline")
	pipelineDef.SetNamespace(defaultNamespace)
	var pipelineSpecMap map[string]interface{}
	if err := util.StructToStruct(prs.Pipeline.Spec, &pipelineSpecMap); err != nil {
		log.Fatal(err)
	}
	pipelineDef.Object["spec"] = pipelineSpecMap

	return pipelineDef
}

func makeISBServiceDefinition(isbServiceSpec numaflowv1.InterStepBufferServiceSpec) unstructured.Unstructured {
	isbServiceSpecRaw, _ := json.Marshal(isbServiceSpec)

	isbrs := apiv1.ISBServiceRolloutSpec{
		InterStepBufferService: apiv1.InterStepBufferService{
			Spec: runtime.RawExtension{
				Raw: isbServiceSpecRaw,
			},
		},
	}

	isbServiceDef := unstructured.Unstructured{Object: make(map[string]interface{})}
	isbServiceDef.SetGroupVersionKind(numaflowv1.ISBGroupVersionKind)
	isbServiceDef.SetName("test-isbsvc")
	isbServiceDef.SetNamespace(defaultNamespace)
	var isbServiceSpecMap map[string]interface{}
	if err := util.StructToStruct(isbrs.InterStepBufferService.Spec, &isbServiceSpecMap); err != nil {
		log.Fatal(err)
	}
	isbServiceDef.Object["spec"] = isbServiceSpecMap

	return isbServiceDef
}

func Test_ResourceNeedsUpdating(t *testing.T) {
	ctx := context.Background()

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	pipelineDefn := makePipelineDefinition(defaultPipelineSpec)
	isbServiceDefn := makeISBServiceDefinition(defaultISBServiceSpec)

	testCases := []struct {
		name                  string
		newDefinition         unstructured.Unstructured
		existingDefinition    unstructured.Unstructured
		usdeConfig            config.USDEConfig
		namespaceConfig       *config.NamespaceConfig
		expectedNeedsUpdating bool
		expectedStrategy      apiv1.UpgradeStrategy
	}{
		{
<<<<<<< HEAD
			name: "NoOp: empty pipeline spec data loss fields, and equivalent metadata",
			newDefinition: func() kubernetes.GenericObject {
				pipelineDef := pipelineDefn
				pipelineDef.Annotations = map[string]string{"something": "a"}
				pipelineDef.Labels = map[string]string{"something": "a"}
=======
			name: "NoOp: empty pipeline spec excluded paths, and equivalent metadata",
			newDefinition: func() unstructured.Unstructured {
				pipelineDef := *pipelineDefn.DeepCopy()
				pipelineDef.SetAnnotations(map[string]string{"something": "a"})
				pipelineDef.SetLabels(map[string]string{"something": "a"})
>>>>>>> chandankumar4/controller_refactor
				return pipelineDef
			}(),
			existingDefinition: func() unstructured.Unstructured {
				pipelineDef := *pipelineDefn.DeepCopy()
				pipelineDef.SetAnnotations(map[string]string{"something": "a"})
				pipelineDef.SetLabels(map[string]string{"something": "a"})
				return pipelineDef
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: false,
			expectedStrategy:      apiv1.UpgradeStrategyNoOp,
		},
		{
<<<<<<< HEAD
			name:          "empty pipeline spec data loss fields and change interStepBufferServiceName field",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
=======
			name:          "empty pipeline spec excluded paths and change interStepBufferServiceName field",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
>>>>>>> chandankumar4/controller_refactor
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.InterStepBufferServiceName = "changed-isbsvc"
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
<<<<<<< HEAD
				PipelineSpecDataLossFields: []config.SpecDataLossField{},
=======
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:          "only exclude interStepBufferServiceName field (changed)",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.InterStepBufferServiceName = "changed-isbsvc"
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName"},
>>>>>>> chandankumar4/controller_refactor
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
<<<<<<< HEAD
			name:          "only include interStepBufferServiceName field (changed)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.InterStepBufferServiceName = "changed-isbsvc"
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.interStepBufferServiceName"}},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:               "only include interStepBufferServiceName field (NOT changed)",
			newDefinition:      pipelineDefn,
			existingDefinition: pipelineDefn,
=======
			name:               "only exclude interStepBufferServiceName field (NOT changed)",
			newDefinition:      *pipelineDefn.DeepCopy(),
			existingDefinition: *pipelineDefn.DeepCopy(),
>>>>>>> chandankumar4/controller_refactor
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.interStepBufferServiceName"}},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: false,
			expectedStrategy:      apiv1.UpgradeStrategyNoOp,
		},
		{
<<<<<<< HEAD
			name:          "only include interStepBufferServiceName field and change some other field (no user strategy)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
=======
			name:          "only exclude interStepBufferServiceName field and change some other field (no user strategy)",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
>>>>>>> chandankumar4/controller_refactor
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.interStepBufferServiceName"}},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
<<<<<<< HEAD
			name:          "only include interStepBufferServiceName field and change some other field (with invalid user strategy)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
=======
			name:          "only exclude interStepBufferServiceName field and change some other field (with invalid user strategy)",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
>>>>>>> chandankumar4/controller_refactor
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.interStepBufferServiceName"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "invalid"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
<<<<<<< HEAD
			name:          "only include interStepBufferServiceName field and change some other field (with valid user strategy)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
=======
			name:          "only exclude interStepBufferServiceName field and change some other field (with valid user strategy)",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
>>>>>>> chandankumar4/controller_refactor
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[0].Name = "new-vtx-name"
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.interStepBufferServiceName"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name:          "with changes in array deep map (map field)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
				newRPU := int64(10)
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.source.generator", IncludeSubfields: true}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
<<<<<<< HEAD
			name:          "with changes in array deep map (primitive field)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
=======
			name:          "with changes in array deep map but excluded",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
>>>>>>> chandankumar4/controller_refactor
				newRPU := int64(10)
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
<<<<<<< HEAD
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.source.generator.rpu"}},
=======
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator.rpu"},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name:          "with changes in array deep map but excluded parent",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
				newRPU := int64(10)
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.InterStepBufferServiceName = "changed-isbsvc"
				newPipelineDef.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineDef)
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
			name:          "with changes in array deep map but one is NOT excluded",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
				newRPU := int64(10)
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[0].Name = "new-vtx-name"
				newPipelineDef.InterStepBufferServiceName = "changed-isbsvc"
				newPipelineDef.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				DefaultUpgradeStrategy:    config.PPNDStrategyID,
				PipelineSpecExcludedPaths: []string{"interStepBufferServiceName", "vertices.source.generator.rpu"},
>>>>>>> chandankumar4/controller_refactor
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:          "with changes in array deep map - detect pointer fields",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[2].Sink.Log = nil
				newPipelineDef.Vertices[2].Sink.Blackhole = &numaflowv1.Blackhole{}
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.sink.log"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
<<<<<<< HEAD
			name:          "with changes in array deep map - detect pointer fields - parent field is included (array)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
=======
			name:          "with changes in array deep map - detect pointer fields - parent field is excluded",
			newDefinition: *pipelineDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
>>>>>>> chandankumar4/controller_refactor
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[2].Sink.Log = nil
				newPipelineDef.Vertices[2].Sink.Blackhole = &numaflowv1.Blackhole{}
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:          "with changes in array deep map - detect pointer fields - parent field is included (no subfields)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[2].Sink.Log = nil
				newPipelineDef.Vertices[2].Sink.Blackhole = &numaflowv1.Blackhole{}
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.sink"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
<<<<<<< HEAD
			name:          "with changes in array deep map - detect pointer fields - parent field is included (with subfields)",
			newDefinition: pipelineDefn,
			existingDefinition: func() kubernetes.GenericObject {
				newPipelineDef := defaultPipelineSpec.DeepCopy()
				newPipelineDef.Vertices[2].Sink.Log = nil
				newPipelineDef.Vertices[2].Sink.Blackhole = &numaflowv1.Blackhole{}
				return makePipelineDefinition(*newPipelineDef)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.sink", IncludeSubfields: true}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name:               "included paths not found",
			newDefinition:      pipelineDefn,
			existingDefinition: pipelineDefn,
=======
			name:               "excluded paths not found",
			newDefinition:      *pipelineDefn.DeepCopy(),
			existingDefinition: *pipelineDefn.DeepCopy(),
>>>>>>> chandankumar4/controller_refactor
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.source.something"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: false,
			expectedStrategy:      apiv1.UpgradeStrategyNoOp,
		},
		{
			name:          "isb test",
			newDefinition: *isbServiceDefn.DeepCopy(),
			existingDefinition: func() unstructured.Unstructured {
				newISBServiceSpec := defaultISBServiceSpec.DeepCopy()
				newISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits = v1.ResourceList{v1.ResourceMemory: newMemLimit}
				return makeISBServiceDefinition(*newISBServiceSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields:   []config.SpecDataLossField{{Path: "spec.vertices.source.something"}},
				ISBServiceSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.jetstream.containerTemplate.resources.limits"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name:          "isb test - include subfields",
			newDefinition: isbServiceDefn,
			existingDefinition: func() kubernetes.GenericObject {
				newISBServiceSpec := defaultISBServiceSpec.DeepCopy()
				newISBServiceSpec.JetStream.ContainerTemplate.Resources.Limits = v1.ResourceList{v1.ResourceMemory: newMemLimit}
				return makeISBServiceDefinition(*newISBServiceSpec)
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields:   []config.SpecDataLossField{{Path: "spec.vertices.source.something"}},
				ISBServiceSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.jetstream.containerTemplate.resources.limits", IncludeSubfields: true}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name: "test Annotation changes resulting in Direct Apply",
			newDefinition: func() unstructured.Unstructured {
				pipelineDef := *pipelineDefn.DeepCopy()
				pipelineDef.SetAnnotations(map[string]string{"something": "a"})
				return pipelineDef
			}(),
			existingDefinition: func() unstructured.Unstructured {
				pipelineDef := *pipelineDefn.DeepCopy()
				pipelineDef.SetAnnotations(map[string]string{"something": "b"})
				return pipelineDef
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyApply,
		},
		{
			name: "test Annotation change which requires Progressive update, overriding spec change resulting in Direct Apply",
			newDefinition: func() unstructured.Unstructured {
				pipelineDef := *pipelineDefn.DeepCopy()
				pipelineDef.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "0"})
				pipelineDef.SetLabels(map[string]string{"something": "a"})
				return pipelineDef
			}(),
			existingDefinition: func() unstructured.Unstructured {
				newPipelineSpec := defaultPipelineSpec.DeepCopy()
				newPipelineSpec.InterStepBufferServiceName = "changed-isbsvc"
				pipelineDef := makePipelineDefinition(*newPipelineSpec)
				pipelineDef.SetAnnotations(map[string]string{common.AnnotationKeyNumaflowInstanceID: "1"})
				pipelineDef.SetLabels(map[string]string{"something": "b"})
				return pipelineDef
			}(),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.interStepBufferServiceName"}},
			},
			namespaceConfig:       nil,
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name: "existing pipeline with empty or nil map and new pipeline adding subfield map",
			newDefinition: func() kubernetes.GenericObject {
				newPipelineDef := existingPipelineSpec1.DeepCopy()
				newPipelineDef.Vertices[0].Source.Generator = &numaflowv1.GeneratorSource{}
				newPipelineDef.Vertices[1].Source = &numaflowv1.Source{Generator: &numaflowv1.GeneratorSource{}}
				return makePipelineDefinition(*newPipelineDef)
			}(),
			existingDefinition: makePipelineDefinition(existingPipelineSpec1),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.source.generator"}},
			},
			namespaceConfig:       &config.NamespaceConfig{UpgradeStrategy: "pause-and-drain"},
			expectedNeedsUpdating: true,
			expectedStrategy:      apiv1.UpgradeStrategyPPND,
		},
		{
			name: "existing pipeline with empty map and new pipeline adding subfield primitive",
			newDefinition: func() kubernetes.GenericObject {
				newRPU := int64(10)
				newPipelineDef := existingPipelineSpec2.DeepCopy()
				newPipelineDef.Vertices[0].Source.Generator.RPU = &newRPU
				return makePipelineDefinition(*newPipelineDef)
			}(),
			existingDefinition: makePipelineDefinition(existingPipelineSpec2),
			usdeConfig: config.USDEConfig{
				PipelineSpecDataLossFields: []config.SpecDataLossField{{Path: "spec.vertices.source.generator"}},
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

			needsUpdating, strategy, err := ResourceNeedsUpdating(ctx, &tc.newDefinition, &tc.existingDefinition)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNeedsUpdating, needsUpdating)
			assert.Equal(t, tc.expectedStrategy, strategy)
		})
	}
}

func TestGetMostConservativeStrategy(t *testing.T) {
	tests := []struct {
		name                   string
		strategies             []apiv1.UpgradeStrategy
		expectedStrategyRating int
	}{
		{
			name: "Multiple Strategies",
			strategies: []apiv1.UpgradeStrategy{
				apiv1.UpgradeStrategyNoOp,
				apiv1.UpgradeStrategyApply,
				apiv1.UpgradeStrategyPPND,
			},
			expectedStrategyRating: 2,
		},
		{
			name:                   "Empty List",
			strategies:             []apiv1.UpgradeStrategy{},
			expectedStrategyRating: 0,
		},
		{
			name: "Same Rating",
			strategies: []apiv1.UpgradeStrategy{
				apiv1.UpgradeStrategyPPND,
				apiv1.UpgradeStrategyProgressive,
			},
			expectedStrategyRating: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getMostConservativeStrategy(tt.strategies)
			assert.Equal(t, tt.expectedStrategyRating, strategyRating[result])
		})
	}
}
