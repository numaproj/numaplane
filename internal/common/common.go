package common

const (
	// LabelKeyGitSyncInstance Resource metadata labels (keys and values) used for tracking
	LabelKeyGitSyncInstance = "numaplane.numaproj.io/tracking-id"
	// SSAManager is the default numaplane manager name used by server-side apply syncs
	SSAManager = "numaplane-controller"
	// EnvLogLevel log level that is defined by `--loglevel` option
	EnvLogLevel = "NUMAPLANE_LOG_LEVEL"
)
