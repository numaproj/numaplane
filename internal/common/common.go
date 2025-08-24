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

package common

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// UpgradeState is the enum to track the possible state of
// a resource upgrade: it can be `promoted`, `in-progress`, or `recyclable`.
type UpgradeState string

// UpgradeStateReason is the enum to track reasons for UpgradeState, to provide additional information when useful
type UpgradeStateReason string

const (
	// SSAManager is the default numaplane manager name used by server-side apply syncs
	SSAManager = "numaplane-controller"
	// EnvLogLevel log level that is defined by `--loglevel` option
	EnvLogLevel = "NUMAPLANE_LOG_LEVEL"

	// NumaflowAPIGroup is the group of the Numaflow API
	NumaflowAPIGroup = "numaflow.numaproj.io"

	// NumaflowAPIVersion is the version of the Numaflow API
	NumaflowAPIVersion = "v1alpha1"

	// NumaflowPipelineKind is the kind of the Numaflow Pipeline
	NumaflowPipelineKind = "Pipeline"

	// NumaflowMonoVertexKind is the kind of the Numaflow MonoVertex
	NumaflowMonoVertexKind = "MonoVertex"

	// NumaflowISBServiceKind is the kind of the Numaflow ISB Service
	NumaflowISBServiceKind = "InterStepBufferService"

	FinalizerName = KeyNumaplanePrefix + "numaplane-controller"

	// LABELS:

	KeyNumaplanePrefix = "numaplane.numaproj.io/"
	KeyNumaflowPrefix  = "numaflow.numaproj.io/"

	// LabelKeyNumaplaneInstance Resource metadata labels (keys and values) used for tracking
	LabelKeyNumaplaneInstance = KeyNumaplanePrefix + "tracking-id"

	// LabelKeyNumaplaneControllerConfig is the label key used to identify additional Numaplane ConfigMaps (ex: Numaflow Controller definitions, USDE, etc.)
	LabelKeyNumaplaneControllerConfig = KeyNumaplanePrefix + "config"

	// LabelValueNumaflowControllerDefinitions is the label value used to identify the Numaplane ConfigMap for the Numaflow Controller definitions
	LabelValueNumaflowControllerDefinitions = "numaflow-controller-definitions"

	// LabelValueUSDEConfig is the label value used to identify the USDE ConfigMap
	LabelValueUSDEConfig = "usde-config"

	// LabelValueNamespaceConfig is the label value used to identify the user's namespace-level ConfigMap
	LabelValueNamespaceConfig = "namespace-level-config"

	// LabelKeyISBServiceRONameForPipeline is the label key used to identify the ISBServiceRollout that a Pipeline is associated with
	LabelKeyISBServiceRONameForPipeline = KeyNumaplanePrefix + "isbsvc-name" // TODO: this is still named "isbsvc-name" instead of "isbsvc-rollout-name" - consider deprecating this and creating a separate label for isbsvc-rollout-name?

	// LabelKeyISBServiceChildNameForPipeline is the label key used to identify the InterstepBufferService that a Pipeline is associated with
	LabelKeyISBServiceChildNameForPipeline = KeyNumaplanePrefix + "isbsvc-child-name"

	// LabelKeyParentRollout is the label key used to identify the Rollout that a child Resource is managed by
	// This is useful as a Label to quickly locate all children of a given Rollout
	LabelKeyParentRollout = KeyNumaplanePrefix + "parent-rollout-name"

	// LabelKeyAllowDataLoss is the label key on a Pipeline to indicate that PPND strategy can skip the usual pausing required
	// this includes both the case of pausing for Pipeline updating as well as for NumaflowController and isbsvc updating
	LabelKeyAllowDataLoss = KeyNumaplanePrefix + "allow-data-loss"

	// LabelKeyUpgradeState is the label key used to identify the upgrade state of a resource that is managed by
	// a NumaRollout.
	LabelKeyUpgradeState = KeyNumaplanePrefix + "upgrade-state"

	// LabelKeyUpgradeStateReason is an optional label to provide more information on top of the LabelKeyUpgradeState
	LabelKeyUpgradeStateReason = KeyNumaplanePrefix + "upgrade-state-reason"

	// LabelValueUpgradePromoted is the label value indicating that the resource managed by a NumaRollout is promoted
	// after an upgrade.
	LabelValueUpgradePromoted UpgradeState = "promoted"

	// LabelValueUpgradeInProgress is the label value indicating that the resource managed by a NumaRollout is in progress
	// of upgrade.
	LabelValueUpgradeInProgress UpgradeState = "in-progress"

	// LabelValueUpgradeRecyclable is the label value indicating that the resource managed by a NumaRollout is recyclable
	// after an upgrade.
	LabelValueUpgradeRecyclable UpgradeState = "recyclable"

	// LabelValueProgressiveSuccess is the value used for the Label `LabelKeyUpgradeStateReason` when `LabelKeyUpgradeState`="recyclable" due to Progressive child succeeding
	LabelValueProgressiveSuccess UpgradeStateReason = "progressive-success"

	// LabelValueProgressiveReplaced is the value used for the Label `LabelKeyUpgradeStateReason` when `LabelKeyUpgradeState`="recyclable" due to Progressive child having been replaced
	LabelValueProgressiveReplaced UpgradeStateReason = "progressive-replaced"

	// LabelValueProgressiveReplacedFailed is the value used for the Label `LabelKeyUpgradeStateReason` when `LabelKeyUpgradeState`="recyclable" due to Progressive child having been
	// replaced after having failed
	LabelValueProgressiveReplacedFailed UpgradeStateReason = "progressive-replaced-failed"

	// LabelValueDeleteRecreateChild is the value used for the Label `LabelKeyUpgradeStateReason` when `LabelKeyUpgradeState`="recyclable" due to a child being deleted and recreated
	LabelValueDeleteRecreateChild UpgradeStateReason = "delete-recreate"

	// LabelValueDiscontinueProgressive is the value used for the Label `LabelKeyUpgradeStateReason` when `LabelKeyUpgradeState`="recyclable" due to discontinuing Progressive upgrade process
	LabelValueDiscontinueProgressive UpgradeStateReason = "discontinue-progressive"

	// LabelValuePurgeOld is the value used for the Label `LabelKeyUpgradeStateReason` when `LabelKeyUpgradeState`="recyclable" if there's a strange case in which there are multiple of a given
	// child and there should only be one
	// TODO: reevaluate if we really need that
	LabelValuePurgeOld UpgradeStateReason = "purge"
	// LabelKeyNumaflowPodPipelineName is the label key used to identify the pod associated to a specific pipeline
	LabelKeyNumaflowPodPipelineName = KeyNumaflowPrefix + "pipeline-name"

	// LabelKeyNumaflowPodMonoVertexName is the label key used to identify the pod associated to a specific monovertex
	LabelKeyNumaflowPodMonoVertexName = "app.kubernetes.io/name"

	// LabelKeyNumaflowPodPipelineVertexName is the label key used to identify the pod associated to a specific pipeline vertex
	LabelKeyNumaflowPodPipelineVertexName = KeyNumaflowPrefix + "vertex-name"

	// LabelKeyNumaflowPodMonoVertexVertexName is the label key used to identify the pod associated to a specific monovertex vertex
	LabelKeyNumaflowPodMonoVertexVertexName = KeyNumaflowPrefix + "mono-vertex-name"

	// LabelKeyForcePromote is the label key used to force promote the upgrading child during a progressive upgrade
	LabelKeyForcePromote = KeyNumaplanePrefix + "force-promote"

	// AnnotationKeyNumaflowInstanceID is the annotation passed to Numaflow Controller so it knows whether it should reconcile the resource
	AnnotationKeyNumaflowInstanceID = KeyNumaflowPrefix + "instance"

	// AnnotationKeyHash is used to maintain a hash of a Rider to know whether it's changed
	AnnotationKeyHash = KeyNumaplanePrefix + "hash"

	// AnnotationKeyOverriddenSpec is used in the process of deletion for describing if a pipeline has had its spec overridden with that of a
	// "promoted" pipeline for the purpose of "force draining" it
	AnnotationKeyOverriddenSpec = KeyNumaplanePrefix + "overridden-spec"

	// NumaplaneSystemNamespace is the namespace where the Numaplane Controller is deployed
	NumaplaneSystemNamespace = "numaplane-system"

	// Values used for templating specs in this format: `{{.value}}`

	// TemplatePipelineName can be used as a templated argument in a PipelineRollout's pipeline spec
	TemplatePipelineName = ".pipeline-name"

	// TemplateVertexName can be used as a templated argument in a PipelineRollout's pipeline spec
	TemplateVertexName = ".vertex-name"

	// TemplatePipelineNamespace can be used as a templated argument in a PipelineRollout's pipeline spec
	TemplatePipelineNamespace = ".pipeline-namespace"

	// TemplateMonoVertexName can be used as a templated argument in a MonoVertexRollout's mvtx spec
	TemplateMonoVertexName = ".monovertex-name"

	// TemplateMonoVertexNamespace can be used as a templated argument in a MonoVertexRollout's mvtx spec
	TemplateMonoVertexNamespace = ".monovertex-namespace"
)

var (
	NumaflowGroupVersion = fmt.Sprintf("%s/%s", NumaflowAPIGroup, NumaflowAPIVersion)

	PipelineGVR = schema.GroupVersionResource{
		Group:    NumaflowAPIGroup,
		Version:  NumaflowAPIVersion,
		Resource: "pipelines",
	}

	MonoVertexGVR = schema.GroupVersionResource{
		Group:    NumaflowAPIGroup,
		Version:  NumaflowAPIVersion,
		Resource: "monovertices",
	}

	ISBServiceGVR = schema.GroupVersionResource{
		Group:    NumaflowAPIGroup,
		Version:  NumaflowAPIVersion,
		Resource: "interstepbufferservices",
	}
)

const (
	// DefaultRequeueDelay indicates the default requeue time (in seconds) used by Reconcilers
	DefaultRequeueDelay = 20 * time.Second

	// MaxNameCount represents the maximum index value used as a suffix for a given child Numaflow resource
	// (after reaching this value, we roll over back to 0)
	MaxNameCount int32 = 999
)
