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

	// LabelKeyISBServiceNameForPipeline is the label key used to identify the ISBService being used by a Pipeline
	// This is useful as a Label to quickly locate all Pipelines of a given ISBService
	LabelKeyISBServiceNameForPipeline = "numaplane.numaproj.io/isbsvc-name"
)

var (
	NumaflowGroupVersion = fmt.Sprintf("%s/%s", NumaflowAPIGroup, NumaflowAPIVersion)

	PipelineGVR = schema.GroupVersionResource{
		Group:    NumaflowAPIGroup,
		Version:  NumaflowAPIVersion,
		Resource: "pipelines",
	}

	DataLossPrevention bool
)
