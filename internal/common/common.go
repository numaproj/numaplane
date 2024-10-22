package common

import (
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// UpgradeState is the enum to track the possible state of
// a resource upgrade, it can only be `promoted` or `in-progress`.
type UpgradeState string

const (
	// SSAManager is the default numaplane manager name used by server-side apply syncs
	SSAManager = "numaplane-controller"
	// EnvLogLevel log level that is defined by `--loglevel` option
	EnvLogLevel = "NUMAPLANE_LOG_LEVEL"

	NumaflowAPIGroup = "numaflow.numaproj.io"

	NumaflowAPIVersion = "v1alpha1"

	// LABELS:

	// LabelKeyNumaplaneInstance Resource metadata labels (keys and values) used for tracking
	LabelKeyNumaplaneInstance = "numaplane.numaproj.io/tracking-id"

	// LabelKeyNumaplaneControllerConfig is the label key used to identify additional Numaplane ConfigMaps (ex: Numaflow Controller definitions, USDE, etc.)
	LabelKeyNumaplaneControllerConfig = "numaplane.numaproj.io/config"

	// LabelValueNumaflowControllerDefinitions is the label value used to identify the Numaplane ConfigMap for the Numaflow Controller definitions
	LabelValueNumaflowControllerDefinitions = "numaflow-controller-definitions"

	// LabelValueUSDEConfig is the label value used to identify the USDE ConfigMap
	LabelValueUSDEConfig = "usde-config"

	// LabelValueNamespaceConfig is the label value used to identify the user's namespace-level ConfigMap
	LabelValueNamespaceConfig = "namespace-level-config"

	// LabelKeyISBServiceNameForPipeline is the label key used to identify the ISBService being used by a Pipeline
	// This is useful as a Label to quickly locate all Pipelines of a given ISBService
	LabelKeyISBServiceNameForPipeline = "numaplane.numaproj.io/isbsvc-name"

	// LabelKeyParentRollout is the label key used to identify the Rollout that a child Resource is managed by
	// This is useful as a Label to quickly locate all children of a given Rollout
	LabelKeyParentRollout = "numaplane.numaproj.io/parent-rollout-name"

	// LabelKeyAllowDataLoss is the label key on a Pipeline to indicate that PPND strategy can skip the usual pausing required
	// this includes both the case of pausing for Pipeline updating as well as for NumaflowController and isbsvc updating
	LabelKeyAllowDataLoss = "numaplane.numaproj.io/allow-data-loss"

	// LabelKeyUpgradeState is the label key used to identify the upgrade state of a resource that is managed by
	// a NumaRollout.
	LabelKeyUpgradeState = "numaplane.numaproj.io/upgrade-state"

	// LabelValueUpgradePromoted is the label value indicating that the resource managed by a NumaRollout is promoted
	// after an upgrade.
	LabelValueUpgradePromoted UpgradeState = "promoted"

	// LabelValueUpgradeInProgress is the label value indicating that the resource managed by a NumaRollout is in progress
	// of upgrade.
	LabelValueUpgradeInProgress UpgradeState = "in-progress"

	// LabelValueUpgradeRecyclable is the label value indicating that the resource managed by a NumaRollout is recyclable
	// after an upgrade.
	LabelValueUpgradeRecyclable UpgradeState = "recyclable"

	// AnnotationKeyNumaflowInstanceID is the annotation passed to Numaflow Controller so it knows whether it should reconcile the resource
	AnnotationKeyNumaflowInstanceID = "numaflow.numaproj.io/instance"
)

var (
	NumaflowGroupVersion = fmt.Sprintf("%s/%s", NumaflowAPIGroup, NumaflowAPIVersion)

	PipelineGVR = schema.GroupVersionResource{
		Group:    NumaflowAPIGroup,
		Version:  NumaflowAPIVersion,
		Resource: "pipelines",
	}

	// default requeue time used by Reconcilers
	DefaultDelayedRequeue = ctrl.Result{RequeueAfter: 20 * time.Second}
)
