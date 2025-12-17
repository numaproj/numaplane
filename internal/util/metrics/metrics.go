package metrics

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type LabelValueDrainResult string

type CustomMetrics struct {
	// NumaLogger is used to log messages related to metrics.
	NumaLogger *logger.NumaLogger
	// PipelinesRolloutHealth is the gauge for the health of pipelines.
	PipelinesRolloutHealth *prometheus.GaugeVec
	// PipelineRolloutsRunning is the gauge for the number of running PipelineRollouts.
	PipelineRolloutsRunning *prometheus.GaugeVec
	// PipelineROCounterMap contains the information of all running PipelineRollouts.
	PipelineROCounterMap map[string]map[string]struct{}
	// PipelineROSyncErrors is the counter for the total number of sync errors.
	PipelineROSyncErrors *prometheus.CounterVec
	// PipelineRolloutQueueLength is the gauge for the length of pipeline rollout queue.
	PipelineRolloutQueueLength *prometheus.GaugeVec
	// PipelineROSyncs is the counter for the total number of PipelineRollout reconciliations
	PipelineROSyncs *prometheus.CounterVec
	// ProgressivePipelineDrains is the counter for the total number of drains that have occurred prior to recycling a Pipeline as part of progressive upgrade
	// Labels indicate whether they completed or not
	ProgressivePipelineDrains *prometheus.CounterVec

	// ISBServicesRolloutHealth is the gauge for the health of ISBServiceRollouts.
	ISBServicesRolloutHealth *prometheus.GaugeVec
	// ISBServiceRolloutsRunning is the gauge for the number of running ISBServiceRollouts.
	ISBServiceRolloutsRunning *prometheus.GaugeVec
	// ISBServiceROCounterMap contains the information of all running ISBServiceRollouts.
	ISBServiceROCounterMap map[string]map[string]struct{}
	// ISBServicesROSyncErrors is the counter for the total number of ISBServiceRollout reconciliation errors
	ISBServicesROSyncErrors *prometheus.CounterVec
	// ISBServiceROSyncs is the counter for the total number of ISBServiceRollout reconciliations
	ISBServiceROSyncs *prometheus.CounterVec

	// MonoVerticesRolloutHealth is the gauge for the health of MonoVertexRollout.
	MonoVerticesRolloutHealth *prometheus.GaugeVec
	// MonoVertexRolloutsRunning is the gauge for the number of running MonoVertexRollouts.
	MonoVertexRolloutsRunning *prometheus.GaugeVec
	// MonoVerticesCounterMap contains the information of all running MonoVertexRollouts.
	MonoVerticesCounterMap map[string]map[string]struct{}
	// MonoVertexROSyncErrors is the counter for the total number of MonoVertexRollout reconciliation errors
	MonoVertexROSyncErrors *prometheus.CounterVec
	// MonoVertexROSyncs is the counter for the total number of MonoVertexRollout reconciliations
	MonoVertexROSyncs *prometheus.CounterVec

	// NumaflowControllerRolloutsHealth is the gauge for the health of NumaflowControllerRollouts
	NumaflowControllerRolloutsHealth *prometheus.GaugeVec
	// NumaflowControllerRolloutsRunning is the gauge for the number of running NumaflowControllerRollouts
	NumaflowControllerRolloutsRunning *prometheus.GaugeVec
	// NumaflowControllerRolloutSyncErrors is the counter for the total number of NumaflowControllerRollout reconciliation errors
	NumaflowControllerRolloutSyncErrors *prometheus.CounterVec
	// NumaflowControllerRolloutSyncs is the counter for the total number of NumaflowControllerRollout reconciliations
	NumaflowControllerRolloutSyncs *prometheus.CounterVec
	// NumaflowControllerRolloutPausedSeconds counts the total time a NumaflowControllerRollout requested resources be paused
	NumaflowControllerRolloutPausedSeconds *prometheus.GaugeVec

	// NumaflowControllersHealth is the gauge for the health of NumaflowControllers
	NumaflowControllersHealth *prometheus.GaugeVec
	// NumaflowControllerSyncErrors is the counter for the total number of NumaflowController reconciliation errors
	NumaflowControllerSyncErrors *prometheus.CounterVec
	// NumaflowControllerSyncs is the counter for the total number of NumaflowController reconciliations
	NumaflowControllerSyncs *prometheus.CounterVec
	// NumaflowControllerKubectlExecutionCounter counts the number of kubectl executions during a NumaflowController reconciliation
	NumaflowControllerKubectlExecutionCounter *prometheus.CounterVec

	// ReconciliationDuration is the histogram for the duration of pipeline, isb service, monovertex and numaflow controller reconciliation.
	ReconciliationDuration *prometheus.HistogramVec
	// KubeRequestCounter Count the number of kubernetes requests during reconciliation
	KubeRequestCounter *prometheus.CounterVec
	// KubeResourceMonitored count the number of monitored kubernetes resource objects in cache
	KubeResourceMonitored *prometheus.GaugeVec
	// KubeResourceCache count the number of kubernetes resource objects in cache
	KubeResourceCache *prometheus.GaugeVec
	// ClusterCacheError count the total number of cluster cache errors
	ClusterCacheError *prometheus.CounterVec
	// PipelinePausedSeconds counts the total time a Pipeline was paused.
	PipelinePausedSeconds *prometheus.GaugeVec
	// PipelinePausingSeconds counts the total time a Pipeline was pausing.
	PipelinePausingSeconds *prometheus.GaugeVec
	// ISBServicePausedSeconds counts the total time an ISBService requested resources be paused.
	ISBServicePausedSeconds *prometheus.GaugeVec

	// Progressive Rollout Metrics
	PipelineProgressiveStarted     *prometheus.CounterVec
	PipelineProgressiveCompleted   *prometheus.CounterVec
	ISBServiceProgressiveStarted   *prometheus.CounterVec
	ISBServiceProgressiveCompleted *prometheus.CounterVec
	MonovertexProgressiveStarted   *prometheus.CounterVec
	MonovertexProgressiveCompleted *prometheus.CounterVec
}

const (
	LabelIntuit                    = "intuit_alert"
	LabelVersion                   = "version"
	LabelType                      = "type"
	LabelPhase                     = "phase"
	LabelK8SVersion                = "K8SVersion"
	LabelName                      = "name"
	LabelNamespace                 = "namespace"
	LabelPipeline                  = "pipeline"
	LabelISBService                = "isbservice"
	LabelNumaflowControllerRollout = "numaflowcontrollerrollout"
	LabelNumaflowController        = "numaflowcontroller"
	LabelMonoVertex                = "monovertex"
	LabelPauseType                 = "pause_type"
	LabelPipelineRollout           = "pipelineRollout"
	LabelDrainComplete             = "drainComplete"
	LabelDrainResult               = "drainResult"
	LabelRolloutName               = "rolloutName"
	LabelSuccess                   = "success"
	LabelForcedSuccess             = "forcedSuccess"
	LabelResourceHealthSuccess     = "resourceHealthSuccess"
	LabelCompleted                 = "completed"
)

var (
	phases         = []string{apiv1.PhasePending.String(), apiv1.PhaseDeployed.String(), apiv1.PhaseFailed.String()}
	defaultLabels  = prometheus.Labels{LabelIntuit: "true"}
	pipelineLock   sync.Mutex
	isbServiceLock sync.Mutex
	monoVertexLock sync.Mutex

	LabelValueDrainResult_PipelineFailed   LabelValueDrainResult = "PipelineFailed"
	LabelValueDrainResult_NeverDrained     LabelValueDrainResult = "DrainIncomplete"
	LabelValueDrainResult_StandardDrain    LabelValueDrainResult = "StandardDrain"
	LabelValueDrainResult_ForceDrain       LabelValueDrainResult = "ForceDrain"
	LabelValueDrainResult_DrainNotRequired LabelValueDrainResult = "DrainNotRequired"

	// pipelinesRolloutHealth indicates whether the pipeline rollouts are healthy (from k8s resource perspective).
	pipelinesRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_pipeline_rollout_health",
		Help:        "A metric to indicate whether the pipeline rollout is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelPipeline, LabelPhase})

	// isbServicesRolloutHealth indicates whether the ISB service rollouts are healthy (from k8s resource perspective).
	isbServicesRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_isb_services_rollout_health",
		Help:        "A metric to indicate whether the isb services rollout is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelISBService, LabelPhase})

	// monoVerticesRolloutHealth indicates whether the mono vertices are healthy (from k8s resource perspective).
	monoVerticesRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_monovertex_rollout_health",
		Help:        "A metric to indicate whether the MonoVertex is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelMonoVertex, LabelPhase})

	// numaflowControllersHealth indicates whether the NumaflowControllers are healthy (from k8s resource perspective)
	numaflowControllersHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_health",
		Help:        "A metric to indicate whether the NumaflowController is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelNumaflowController, LabelPhase})

	// numaflowControllerRolloutsHealth indicates whether the numaflow controller rollouts are healthy (from k8s resource perspective)
	numaflowControllerRolloutsHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_rollout_health",
		Help:        "A metric to indicate whether the numaflow controller rollout is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelNumaflowController, LabelPhase})

	// pipelineRolloutsRunning indicates the number of PipelineRollouts
	pipelineRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "pipeline_rollouts_running",
		Help:        "Number of pipeline rollouts running",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace})

	// pipelinePausedSeconds Check the total time a pipeline was paused
	pipelinePausedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_pipeline_system_paused_seconds",
		Help:        "Duration a pipeline is paused for",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelName})

	// pipelinePausingSeconds Check the total time a pipeline was pausing
	pipelinePausingSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_pipeline_system_pausing_seconds",
		Help:        "Duration a pipeline is pausing for",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelName})

	// pipelineROSyncs Check the total number of pipeline rollout reconciliations
	pipelineROSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "pipeline_rollout_syncs_total",
		Help:        "The total number of pipeline synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelineROSyncErrors Check the total number of pipeline rollout reconciliation errors
	pipelineROSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "pipeline_rollout_sync_errors_total",
		Help:        "The total number of pipeline sync failed",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelineRolloutQueueLength check the length of queue
	pipelineRolloutQueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "pipeline_rollout_queue_length",
		Help:        "The length of pipeline rollout queue",
		ConstLabels: defaultLabels,
	}, []string{})

	progressivePipelineDrains = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "progressive_pipeline_drains",
		Help:        "The total number of pipelines drained as part of Progressive Rollout recycling",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelPipelineRollout, LabelPipeline, LabelDrainComplete, LabelDrainResult})

	// isbServiceRolloutsRunning is the gauge for the number of running ISBServiceRollouts.
	isbServiceRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "isb_service_rollouts_running",
		Help:        "Number of ISB Service Rollouts running",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace})

	// isbServiceROSyncs Check the total number of ISBServiceRollout syncs
	isbServiceROSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "isb_service_rollout_syncs_total",
		Help:        "The total number of ISB service rollouts synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// isbServiceROSyncErrors Check the total number of ISBServiceRollout sync errors
	isbServiceROSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "isb_service_rollout_sync_errors_total",
		Help:        "The total number of ISB service sync failed",
		ConstLabels: defaultLabels,
	}, []string{})

	// isbServicePausedSeconds Check the total time an ISBService requested resource to pause
	isbServicePausedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_isbservice_paused_seconds",
		Help:        "Duration an ISBService paused resources for",
		ConstLabels: defaultLabels,
	}, []string{LabelName, LabelNamespace})

	// monoVertexRolloutsRunning is the gauge for the number of MonoVertexRollouts.
	monoVertexRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "monovertex_rollouts_running",
		Help:        "Number of MonoVertexRollouts running",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace})

	// monoVertexROSyncs Check the total number of MonoVertexRollout reconciliations
	monoVertexROSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "monovertex_rollout_syncs_total",
		Help:        "The total number of monovertices synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// monoVertexROSyncErrors Check the total number of MonoVertexRollout sync errors
	monoVertexROSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "monovertex_rollout_sync_errors_total",
		Help:        "The total number of monovertices sync failed",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerRolloutsRunning is the gauge for the number of running numaflow controller rollouts
	numaflowControllerRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_rollouts_running",
		Help:        "Number of NumaflowControllerRollouts",
		ConstLabels: defaultLabels,
	}, []string{LabelName, LabelNamespace, LabelVersion})

	// numaflowControllerRolloutSyncErrors is the total number of NumaflowControllerRollout reconciliation errors
	numaflowControllerRolloutSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_rollout_sync_errors_total",
		Help:        "The total number of NumaflowControllerRollout sync errors",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerRolloutSyncs is the total number of NumaflowControllerRollout reconciliations
	numaflowControllerRolloutSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_rollout_syncs_total",
		Help:        "The total number of NumaflowControllerRollout syncs",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerRolloutPausedSeconds is the total time a NumaflowControllerRollout requested resources be paused
	numaflowControllerRolloutPausedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_rollout_paused_seconds",
		Help:        "Duration a NumaflowControllerRollout paused pipelines for",
		ConstLabels: defaultLabels,
	}, []string{LabelName, LabelNamespace})

	// numaflowControllerSyncErrors is the total number of NumaflowController reconciliation errors
	numaflowControllerSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_sync_errors_total",
		Help:        "The total number of NumaflowController sync errors",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerSyncs is the total number of NumaflowController reconciliations
	numaflowControllerSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_syncs_total",
		Help:        "The total number of NumaflowController syncs",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerKubectlExecutionCounter is the total number of kubectl executions for NumaflowController
	numaflowControllerKubectlExecutionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_kubectl_execution_total",
		Help:        "The total number of kubectl execution for NumaflowController",
		ConstLabels: defaultLabels,
	}, []string{})

	// reconciliationDuration is the histogram for the duration of pipeline, isb service and numaflow controller reconciliation.
	reconciliationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "numaplane_reconciliation_duration_seconds",
		Help:        "Duration of pipeline reconciliation",
		ConstLabels: defaultLabels,
	}, []string{LabelType, LabelPhase})

	// kubeRequestCounter Check the total number of kubernetes requests for numaflow controller
	kubeRequestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_kube_request_total",
		Help:        "The total number of kubernetes request for numaflow controller",
		ConstLabels: defaultLabels,
	}, []string{})

	// kubeResourceCacheMonitored count the number of monitored kubernetes resource objects in cache
	kubeResourceCacheMonitored = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_kube_resource_monitored",
		Help:        "Number of monitored kubernetes resource object in cache",
		ConstLabels: defaultLabels,
	}, []string{})

	// kubeResourceCache count the number of kubernetes resource objects in cache
	kubeResourceCache = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_kube_resource_cache",
		Help:        "Number of kubernetes resource object in cache",
		ConstLabels: defaultLabels,
	}, []string{LabelK8SVersion})

	// clusterCacheError count the total number of cluster cache errors
	clusterCacheError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_cluster_cache_error_total",
		Help:        "The total number of cluster cache error",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelineProgressiveStarted count the total number of pipeline progressive rollouts started
	pipelineProgressiveStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_pipeline_progressive_started",
		Help:        "The total number of pipeline progressive rollout started",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelRolloutName, LabelName})

	// pipelineProgressiveResults count the total number of pipeline progressive rollouts completed
	pipelineProgressiveCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_pipeline_progressive_completed",
		Help:        "The total number of pipeline progressive rollout completed",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelRolloutName, LabelName, LabelSuccess, LabelForcedSuccess, LabelResourceHealthSuccess})

	// isbServiceProgressiveStarted count the total number of ISB Service progressive rollouts started
	isbServiceProgressiveStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_isbsvc_progressive_started",
		Help:        "The total number of pipeline isbsvc rollout started",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelRolloutName, LabelName})

	//isbServiceProgressiveCompleted count the total number of isbsvc progressive rollouts completed
	isbServiceProgressiveCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_isbsvc_progressive_completed",
		Help:        "The total number of isbsvc progressive rollout completed",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelRolloutName, LabelName, LabelSuccess, LabelForcedSuccess, LabelResourceHealthSuccess})

	// monovertexProgressiveStarted count the total number of monovertex progressive rollouts started
	monovertexProgressiveStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_monovertex_progressive_started",
		Help:        "The total number of monovertex rollout started",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelRolloutName, LabelName})

	// monovertexProgressiveCompleted count the total number of monovertex progressive rollout completed
	monovertexProgressiveCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaplane_monovertex_progressive_completed",
		Help:        "The total number of monovertex progressive rollout completed",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelRolloutName, LabelName, LabelSuccess, LabelForcedSuccess, LabelResourceHealthSuccess})
)

// RegisterCustomMetrics registers the custom metrics to the existing global prometheus registry for pipelines, ISB service and numaflow controller
func RegisterCustomMetrics(numaLogger *logger.NumaLogger) *CustomMetrics {
	metrics.Registry.MustRegister(
		pipelinesRolloutHealth, pipelineRolloutsRunning, pipelineROSyncs, pipelineROSyncErrors, pipelineRolloutQueueLength, progressivePipelineDrains,
		isbServicesRolloutHealth, isbServiceRolloutsRunning, isbServiceROSyncs, isbServiceROSyncErrors,
		monoVerticesRolloutHealth, monoVertexRolloutsRunning, monoVertexROSyncs, monoVertexROSyncErrors,
		numaflowControllerRolloutsHealth, numaflowControllerRolloutsRunning, numaflowControllerRolloutSyncs, numaflowControllerRolloutSyncErrors, numaflowControllerRolloutPausedSeconds,
		numaflowControllersHealth, numaflowControllerSyncs, numaflowControllerSyncErrors, numaflowControllerKubectlExecutionCounter,
		reconciliationDuration, kubeRequestCounter, kubeResourceCacheMonitored,
		kubeResourceCache, clusterCacheError, pipelinePausedSeconds, pipelinePausingSeconds, isbServicePausedSeconds,
		pipelineProgressiveStarted, pipelineProgressiveCompleted, isbServiceProgressiveStarted, isbServiceProgressiveCompleted, monovertexProgressiveStarted, monovertexProgressiveCompleted)

	return &CustomMetrics{
		NumaLogger:                                numaLogger,
		PipelinesRolloutHealth:                    pipelinesRolloutHealth,
		PipelineRolloutsRunning:                   pipelineRolloutsRunning,
		PipelineROCounterMap:                      make(map[string]map[string]struct{}),
		PipelineROSyncs:                           pipelineROSyncs,
		PipelineROSyncErrors:                      pipelineROSyncErrors,
		PipelineRolloutQueueLength:                pipelineRolloutQueueLength,
		ProgressivePipelineDrains:                 progressivePipelineDrains,
		ISBServicesRolloutHealth:                  isbServicesRolloutHealth,
		ISBServiceRolloutsRunning:                 isbServiceRolloutsRunning,
		ISBServiceROCounterMap:                    make(map[string]map[string]struct{}),
		ISBServiceROSyncs:                         isbServiceROSyncs,
		ISBServicesROSyncErrors:                   isbServiceROSyncErrors,
		MonoVerticesRolloutHealth:                 monoVerticesRolloutHealth,
		MonoVertexRolloutsRunning:                 monoVertexRolloutsRunning,
		MonoVerticesCounterMap:                    make(map[string]map[string]struct{}),
		MonoVertexROSyncs:                         monoVertexROSyncs,
		MonoVertexROSyncErrors:                    monoVertexROSyncErrors,
		NumaflowControllerRolloutsHealth:          numaflowControllerRolloutsHealth,
		NumaflowControllerRolloutsRunning:         numaflowControllerRolloutsRunning,
		NumaflowControllerRolloutSyncs:            numaflowControllerRolloutSyncs,
		NumaflowControllerRolloutSyncErrors:       numaflowControllerRolloutSyncErrors,
		NumaflowControllerRolloutPausedSeconds:    numaflowControllerRolloutPausedSeconds,
		NumaflowControllersHealth:                 numaflowControllersHealth,
		NumaflowControllerSyncs:                   numaflowControllerSyncs,
		NumaflowControllerSyncErrors:              numaflowControllerSyncErrors,
		NumaflowControllerKubectlExecutionCounter: numaflowControllerKubectlExecutionCounter,
		KubeRequestCounter:                        kubeRequestCounter,
		ReconciliationDuration:                    reconciliationDuration,
		KubeResourceMonitored:                     kubeResourceCacheMonitored,
		KubeResourceCache:                         kubeResourceCache,
		ClusterCacheError:                         clusterCacheError,
		PipelinePausedSeconds:                     pipelinePausedSeconds,
		PipelinePausingSeconds:                    pipelinePausingSeconds,
		ISBServicePausedSeconds:                   isbServicePausedSeconds,
		PipelineProgressiveStarted:                pipelineProgressiveStarted,
		PipelineProgressiveCompleted:              pipelineProgressiveCompleted,
		ISBServiceProgressiveStarted:              isbServiceProgressiveStarted,
		ISBServiceProgressiveCompleted:            isbServiceProgressiveCompleted,
		MonovertexProgressiveStarted:              monovertexProgressiveStarted,
		MonovertexProgressiveCompleted:            monovertexProgressiveCompleted,
	}
}

// IncPipelineROsRunning increments the PipelineRollout counter if it doesn't already know about it
func (m *CustomMetrics) IncPipelineROsRunning(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	if _, ok := m.PipelineROCounterMap[namespace]; !ok {
		m.PipelineROCounterMap[namespace] = make(map[string]struct{})
	}
	m.PipelineROCounterMap[namespace][name] = struct{}{}
	for ns, pipelines := range m.PipelineROCounterMap {
		m.PipelineRolloutsRunning.WithLabelValues(ns).Set(float64(len(pipelines)))
	}
}

// DecPipelineROsRunning decrements the PipelineRollout counter
func (m *CustomMetrics) DecPipelineROsRunning(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	delete(m.PipelineROCounterMap[namespace], name)
	for ns, pipelines := range m.PipelineROCounterMap {
		m.PipelineRolloutsRunning.WithLabelValues(ns).Set(float64(len(pipelines)))
	}
}

// IncISBServiceRollouts increments the ISBServiceRollout counter if it doesn't already know about it
func (m *CustomMetrics) IncISBServiceRollouts(name, namespace string) {
	isbServiceLock.Lock()
	defer isbServiceLock.Unlock()
	if _, ok := m.ISBServiceROCounterMap[namespace]; !ok {
		m.ISBServiceROCounterMap[namespace] = make(map[string]struct{})
	}
	m.ISBServiceROCounterMap[namespace][name] = struct{}{}
	for ns, isbServices := range m.ISBServiceROCounterMap {
		m.ISBServiceRolloutsRunning.WithLabelValues(ns).Set(float64(len(isbServices)))
	}
}

// DecISBServiceRollouts decrements the ISBServiceRollout counter
func (m *CustomMetrics) DecISBServiceRollouts(name, namespace string) {
	isbServiceLock.Lock()
	defer isbServiceLock.Unlock()
	delete(m.ISBServiceROCounterMap[namespace], name)
	for ns, isbServices := range m.ISBServiceROCounterMap {
		m.ISBServiceRolloutsRunning.WithLabelValues(ns).Set(float64(len(isbServices)))
	}
}

// IncMonoVertexRollouts increments the MonoVertexRollout counter if it doesn't already know about it
func (m *CustomMetrics) IncMonoVertexRollouts(name, namespace string) {
	monoVertexLock.Lock()
	defer monoVertexLock.Unlock()
	if _, ok := m.MonoVerticesCounterMap[namespace]; !ok {
		m.MonoVerticesCounterMap[namespace] = make(map[string]struct{})
	}
	m.MonoVerticesCounterMap[namespace][name] = struct{}{}
	for ns, monoVertices := range m.MonoVerticesCounterMap {
		m.MonoVertexRolloutsRunning.WithLabelValues(ns).Set(float64(len(monoVertices)))
	}
}

// DecMonoVertexRollouts decrements the MonoVertexRollout counter
func (m *CustomMetrics) DecMonoVertexRollouts(name, namespace string) {
	monoVertexLock.Lock()
	defer monoVertexLock.Unlock()
	delete(m.MonoVerticesCounterMap[namespace], name)
	for ns, monoVertices := range m.MonoVerticesCounterMap {
		m.MonoVertexRolloutsRunning.WithLabelValues(ns).Set(float64(len(monoVertices)))
	}
}

// SetPipelineRolloutHealth sets the health of the pipeline rollout
func (m *CustomMetrics) SetPipelineRolloutHealth(namespace, name, currentPhase string) {
	for _, phase := range phases {
		if phase == currentPhase {
			m.PipelinesRolloutHealth.WithLabelValues(namespace, name, phase).Set(1)
		} else {
			m.PipelinesRolloutHealth.WithLabelValues(namespace, name, phase).Set(0)
		}
	}
}

// DeletePipelineRolloutHealth deletes the pipeline rollout health metric
func (m *CustomMetrics) DeletePipelineRolloutHealth(namespace, name string) {
	m.NumaLogger.Infof("Deleting pipeline rollout health metrics for %s/%s", namespace, name)
	for _, phase := range phases {
		deleted := m.PipelinesRolloutHealth.DeleteLabelValues(namespace, name, phase)
		m.NumaLogger.WithValues("phase", phase, "deleted", deleted).Debugf("Result of deletion of pipeline rollout health metrics for %s/%s", namespace, name)
	}
}

// SetISBServicesRolloutHealth sets the health of the ISB service rollout
func (m *CustomMetrics) SetISBServicesRolloutHealth(namespace, name, currentPhase string) {
	for _, phase := range phases {
		if phase == currentPhase {
			m.ISBServicesRolloutHealth.WithLabelValues(namespace, name, phase).Set(1)
		} else {
			m.ISBServicesRolloutHealth.WithLabelValues(namespace, name, phase).Set(0)
		}
	}
}

// DeleteISBServicesRolloutHealth deletes the ISB service rollout health metric
func (m *CustomMetrics) DeleteISBServicesRolloutHealth(namespace, name string) {
	m.NumaLogger.Infof("Deleting isbsvc rollout health metrics for %s/%s", namespace, name)
	for _, phase := range phases {
		deleted := m.ISBServicesRolloutHealth.DeleteLabelValues(namespace, name, phase)
		m.NumaLogger.WithValues("phase", phase, "deleted", deleted).Debugf("Result of deletion of isbsvc rollout health metrics for %s/%s", namespace, name)
	}
}

// SetMonoVerticesRolloutHealth sets the health of the monovertex rollout
func (m *CustomMetrics) SetMonoVerticesRolloutHealth(namespace, name, currentPhase string) {
	for _, phase := range phases {
		if phase == currentPhase {
			m.MonoVerticesRolloutHealth.WithLabelValues(namespace, name, phase).Set(1)
		} else {
			m.MonoVerticesRolloutHealth.WithLabelValues(namespace, name, phase).Set(0)
		}
	}
}

// DeleteMonoVerticesRolloutHealth deletes the monovertex rollout health metric
func (m *CustomMetrics) DeleteMonoVerticesRolloutHealth(namespace, name string) {
	m.NumaLogger.Infof("Deleting monovertex rollout health metrics for %s/%s", namespace, name)
	for _, phase := range phases {
		deleted := m.MonoVerticesRolloutHealth.DeleteLabelValues(namespace, name, phase)
		m.NumaLogger.WithValues("phase", phase, "deleted", deleted).Debugf("Result of deletion of monovertex rollout health metrics for %s/%s", namespace, name)
	}
}

// SetNumaflowControllerRolloutsHealth sets the health of the numaflow controller rollout
func (m *CustomMetrics) SetNumaflowControllerRolloutsHealth(namespace, name, currentPhase string) {
	for _, phase := range phases {
		if phase == currentPhase {
			m.NumaflowControllerRolloutsHealth.WithLabelValues(namespace, name, phase).Set(1)
		} else {
			m.NumaflowControllerRolloutsHealth.WithLabelValues(namespace, name, phase).Set(0)
		}
	}
}

// DeleteNumaflowControllerRolloutsHealth deletes the numaflow controller rollout health metric
func (m *CustomMetrics) DeleteNumaflowControllerRolloutsHealth(namespace, name string) {
	m.NumaLogger.Infof("Deleting numaflow controller rollout health metrics for %s/%s", namespace, name)
	for _, phase := range phases {
		deleted := m.NumaflowControllerRolloutsHealth.DeleteLabelValues(namespace, name, phase)
		m.NumaLogger.WithValues("phase", phase, "deleted", deleted).Debugf("Result of deletion of numaflow controller rollout health metrics for %s/%s", namespace, name)
	}
}

// SetNumaflowControllersHealth sets the health of the numaflow controller
func (m *CustomMetrics) SetNumaflowControllersHealth(namespace, name, currentPhase string) {
	for _, phase := range phases {
		if phase == currentPhase {
			m.NumaflowControllersHealth.WithLabelValues(namespace, name, phase).Set(1)
		} else {
			m.NumaflowControllersHealth.WithLabelValues(namespace, name, phase).Set(0)
		}
	}
}

// DeleteNumaflowControllersHealth deletes the numaflow controller health metric
func (m *CustomMetrics) DeleteNumaflowControllersHealth(namespace, name string) {
	m.NumaLogger.Infof("Deleting numaflow controller health metrics for %s/%s", namespace, name)
	for _, phase := range phases {
		deleted := m.NumaflowControllersHealth.DeleteLabelValues(namespace, name, phase)
		m.NumaLogger.WithValues("phase", phase, "deleted", deleted).Debugf("Result of deletion of numaflow controller health metrics for %s/%s", namespace, name)
	}
}

func (m *CustomMetrics) IncProgressivePipelineDrains(namespace, pipelineRolloutName, pipelineName string, drainComplete bool, drainResult LabelValueDrainResult) {
	m.ProgressivePipelineDrains.WithLabelValues(namespace, pipelineRolloutName, pipelineName, strconv.FormatBool(drainComplete), string(drainResult)).Inc()
}

func (m *CustomMetrics) IncPipelineProgressiveStarted(namespace, rolloutName, pipelineName string) {
	m.PipelineProgressiveStarted.WithLabelValues(namespace, rolloutName, pipelineName).Inc()
}

func (m *CustomMetrics) IncPipelineProgressiveCompleted(namespace, rolloutName, pipelineName string, basicAssessmentResult, successStatus util.OptionalBoolStr, forcedSuccess bool) {
	m.PipelineProgressiveCompleted.WithLabelValues(namespace, rolloutName, pipelineName, successStatus.ToString(), strconv.FormatBool(forcedSuccess), basicAssessmentResult.ToString()).Inc()
}

func (m *CustomMetrics) IncIBSServiceProgressiveStarted(namespace, rolloutName, isbServiceName string) {
	m.ISBServiceProgressiveStarted.WithLabelValues(namespace, rolloutName, isbServiceName).Inc()
}

func (m *CustomMetrics) IncISBServiceProgressiveCompleted(namespace, rolloutName, isbServiceName string, basicAssessmentResult, successStatus util.OptionalBoolStr, forcedSuccess bool) {
	m.ISBServiceProgressiveCompleted.WithLabelValues(namespace, rolloutName, isbServiceName, successStatus.ToString(), strconv.FormatBool(forcedSuccess), basicAssessmentResult.ToString()).Inc()
}

func (m *CustomMetrics) IncMonovertexProgressiveStarted(namespace, rolloutName, monovertexName string) {
	m.MonovertexProgressiveStarted.WithLabelValues(namespace, rolloutName, monovertexName).Inc()
}

func (m *CustomMetrics) IncMonovertexProgressiveCompleted(namespace, rolloutName, monovertexName string, basicAssessmentResult, successStatus util.OptionalBoolStr, forcedSuccess bool) {
	m.MonovertexProgressiveCompleted.WithLabelValues(namespace, rolloutName, monovertexName, successStatus.ToString(), strconv.FormatBool(forcedSuccess), basicAssessmentResult.ToString()).Inc()
}

func EvaluateSuccessStatusForMetrics(assessmentResult apiv1.AssessmentResult) util.OptionalBoolStr {
	if assessmentResult == apiv1.AssessmentResultSuccess {
		return "true"
	} else if assessmentResult == apiv1.AssessmentResultFailure {
		return "false"
	} else {
		return ""
	}
}
