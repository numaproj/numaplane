package common

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

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

	// LabelKeyNumaplaneControllerConfig is the label key used to identify the configmap for the controller definitions
	LabelKeyNumaplaneControllerConfig = "numaplane.numaproj.io/config"
	// LabelValueNumaplaneControllerConfig is the label value used to identify the configmap for the controller definitions
	LabelValueNumaplaneControllerConfig = "numaflow-controller-definitions"

	LabelKeyISBServiceNameForStatefulSet = "numaflow.numaproj.io/isbsvc-name"

	LabelKeyISBServiceNameForPipeline = "numaplane.numaproj.io/isbsvc-name"
)

var (
	NumaflowGroupVersion = fmt.Sprintf("%s/%s", NumaflowAPIGroup, NumaflowAPIVersion)

	PipelineGVR = schema.GroupVersionResource{
		Group:    NumaflowAPIGroup,
		Version:  NumaflowAPIVersion,
		Resource: "pipelines",
	}
)
