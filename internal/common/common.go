package common

const (
	// LabelKeyNumaplaneInstance Resource metadata labels (keys and values) used for tracking
	LabelKeyNumaplaneInstance = "numaplane.numaproj.io/tracking-id"
	// SSAManager is the default numaplane manager name used by server-side apply syncs
	SSAManager = "numaplane-controller"
	// EnvLogLevel log level that is defined by `--loglevel` option
	EnvLogLevel = "NUMAPLANE_LOG_LEVEL"

	// LabelKeyNumaplaneControllerConfig is the label key used to identify the configmap for the controller definitions
	LabelKeyNumaplaneControllerConfig = "numaplane.numaproj.io/config"
	// LabelValueNumaplaneControllerConfig is the label value used to identify the configmap for the controller definitions
	LabelValueNumaplaneControllerConfig = "numaflow-controller-definitions"
)
