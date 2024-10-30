package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

type CustomMetrics struct {
	// PipelinesRolloutHealth is the gauge for the health of pipelines.
	PipelinesRolloutHealth *prometheus.GaugeVec
	// PipelineRolloutsRunning is the gauge for the number of running pipelines.
	PipelineRolloutsRunning *prometheus.GaugeVec
	// PipelineROCounterMap contains the information of all running pipelines.
	PipelineROCounterMap map[string]map[string]struct{}
	// PipelineROSyncErrors is the counter for the total number of failed synced.
	PipelineROSyncErrors *prometheus.CounterVec
	// PipelineRolloutQueueLength is the gauge for the length of pipeline rollout queue.
	PipelineRolloutQueueLength *prometheus.GaugeVec
	// PipelineROSyncs is the counter for the total number of pipelines synced.
	PipelineROSyncs *prometheus.CounterVec
	// ISBServicesRolloutHealth is the gauge for the health of ISB services.
	ISBServicesRolloutHealth *prometheus.GaugeVec
	// ISBServiceRolloutsRunning is the gauge for the number of running ISB services.
	ISBServiceRolloutsRunning *prometheus.GaugeVec
	// ISBServiceROCounterMap contains the information of all running ISB services.
	ISBServiceROCounterMap map[string]map[string]struct{}
	// ISBServicesROSyncErrors is the counter for the total number of ISB service syncing failed.
	ISBServicesROSyncErrors *prometheus.CounterVec
	// ISBServiceROSyncs is the counter for the total number of ISB service synced.
	ISBServiceROSyncs *prometheus.CounterVec
	// MonoVerticesRolloutHealth is the gauge for the health of monovertices.
	MonoVerticesRolloutHealth *prometheus.GaugeVec
	// MonoVertexRolloutsRunning is the gauge for the number of running monovertices.
	MonoVertexRolloutsRunning *prometheus.GaugeVec
	// MonoVerticesCounterMap contains the information of all running monovertices.
	MonoVerticesCounterMap map[string]map[string]struct{}
	// MonoVertexROSyncErrors is the counter for the total number of monovertices syncing failed.
	MonoVertexROSyncErrors *prometheus.CounterVec
	// MonoVertexROSyncs is the counter for the total number of monovertices synced.
	MonoVertexROSyncs *prometheus.CounterVec
	// NumaflowControllersRolloutHealth is the gauge for the health of Numaflow controller.
	NumaflowControllersRolloutHealth *prometheus.GaugeVec
	// NumaflowControlleRORunning is the gauge for the number of running numaflow controllers with a specific version.
	NumaflowControlleRORunning *prometheus.GaugeVec
	// NumaflowControllerROSyncErrors is the counter for the total number of Numaflow controller syncing failed.
	NumaflowControllerROSyncErrors *prometheus.CounterVec
	// NumaflowControllersROSyncs in the counter for the total number of Numaflow controllers synced.
	NumaflowControllersROSyncs *prometheus.CounterVec
	// ReconciliationDuration is the histogram for the duration of pipeline, isb service, monovertex and numaflow controller reconciliation.
	ReconciliationDuration *prometheus.HistogramVec
	// NumaflowControllerKubectlExecutionCounter Count the number of kubectl executions during numaflow controller reconciliation
	NumaflowControllerKubectlExecutionCounter *prometheus.CounterVec
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
	// ISBServicePausedSeconds counts the total time an ISBService requested resources be paused.
	ISBServicePausedSeconds *prometheus.GaugeVec
	// NumaflowControllerPausedSeconds counts the total time a Numaflow controller requested resources be paused.
	NumaflowControllerPausedSeconds *prometheus.GaugeVec
}

const (
	LabelIntuit             = "intuit_alert"
	LabelVersion            = "version"
	LabelType               = "type"
	LabelPhase              = "phase"
	LabelK8SVersion         = "K8SVersion"
	LabelName               = "name"
	LabelNamespace          = "namespace"
	LabelPipeline           = "pipeline"
	LabelISBService         = "isbservice"
	LabelNumaflowController = "numaflowcontroller"
	LabelMonoVertex         = "monovertex"
)

var (
	defaultLabels  = prometheus.Labels{LabelIntuit: "true"}
	pipelineLock   sync.Mutex
	isbServiceLock sync.Mutex
	monoVertexLock sync.Mutex

	// pipelinesRolloutHealth indicates whether the pipeline rollouts are healthy (from k8s resource perspective).
	pipelinesRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_pipeline_rollout_health",
		Help:        "A metric to indicate whether the pipeline rollout is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelPipeline})

	// isbServicesRolloutHealth indicates whether the ISB service rollouts are healthy (from k8s resource perspective).
	isbServicesRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_isb_services_rollout_health",
		Help:        "A metric to indicate whether the isb services rollout is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelISBService})

	// numaflowControllersRolloutHealth indicates whether the numaflow controller rollouts are healthy (from k8s resource perspective).
	numaflowControllersRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_rollout_health",
		Help:        "A metric to indicate whether the numaflow controller rollout is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelNumaflowController})

	// monoVerticesRolloutHealth indicates whether the mono vertices are healthy (from k8s resource perspective).
	monoVerticesRolloutHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaplane_monovertex_rollout_health",
		Help:        "A metric to indicate whether the MonoVertex is healthy. '1' means healthy, '0' means unhealthy",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelMonoVertex})

	// pipelineRolloutsRunning indicates the number of PipelineRollouts
	pipelineRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_pipeline_rollouts_running",
		Help:        "Number of Numaflow pipeline rollouts running",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace})

	// pipelinePausedSeconds Check the total time a pipeline was paused
	pipelinePausedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_pipeline_paused_seconds",
		Help:        "Duration a pipeline was paused for",
		ConstLabels: defaultLabels,
	}, []string{LabelNamespace, LabelName})

	// pipelineROSyncs Check the total number of pipeline rollout reconciliations
	pipelineROSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "pipeline_synced_total",
		Help:        "The total number of pipeline synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelineROSyncErrors Check the total number of pipeline rollout reconciliation errors
	pipelineROSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "pipeline_sync_failed_total",
		Help:        "The total number of pipeline sync failed",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelineRolloutQueueLength check the length of queue
	pipelineRolloutQueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "pipeline_rollout_queue_length",
		Help:        "The length of pipeline rollout queue",
		ConstLabels: defaultLabels,
	}, []string{})

	// isbServiceRolloutsRunning is the gauge for the number of running ISBServiceRollouts.
	isbServiceRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_isb_service_rollouts_running",
		Help:        "Number of Numaflow ISB Service Rollouts running",
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
	}, []string{LabelName})

	// monoVertexRolloutsRunning is the gauge for the number of MonoVertexRollouts.
	monoVertexRolloutsRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_monovertex_rollouts_running",
		Help:        "Number of Numaflow MonoVertexRollouts running",
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

	// numaflowControllerRORunning is the gauge for the number of running numaflow controllers.
	numaflowControllerRORunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_rollout_running",
		Help:        "Number of NumaflowControllerRollouts",
		ConstLabels: defaultLabels,
	}, []string{LabelName, LabelNamespace, LabelVersion})

	// numaflowControllerROSyncs Check the total number of NumaflowControllerRollout reconciliations
	numaflowControllerROSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_rollout_syncs_total",
		Help:        "The total number of NumaflowControllerRollout syncs",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerROSyncErrors Check the total number of NumaflowControllerRollout reconciliation errors
	numaflowControllerROSyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_rollout_sync_errors_total",
		Help:        "The total number of Numaflow controller sync errors",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerKubectlExecutionCounter Check the total number of kubectl executions for numaflow controller
	numaflowControllerKubectlExecutionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_kubectl_execution_total",
		Help:        "The total number of kubectl execution for numaflow controller",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerPausedSeconds Check the total time a Numaflow controller requested resources be paused
	numaflowControllerPausedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_paused_seconds",
		Help:        "Duration a Numaflow controller paused pipelines for",
		ConstLabels: defaultLabels,
	}, []string{LabelName})

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
)

// RegisterCustomMetrics registers the custom metrics to the existing global prometheus registry for pipelines, ISB service and numaflow controller
func RegisterCustomMetrics() *CustomMetrics {
	metrics.Registry.MustRegister(pipelinesRolloutHealth, pipelineRolloutsRunning, pipelineROSyncs, pipelineROSyncErrors, pipelineRolloutQueueLength,
		isbServicesRolloutHealth, isbServiceRolloutsRunning, isbServiceROSyncs, isbServiceROSyncErrors,
		monoVerticesRolloutHealth, monoVertexRolloutsRunning, monoVertexROSyncs, monoVertexROSyncErrors,
		numaflowControllersRolloutHealth, numaflowControllerRORunning, numaflowControllerROSyncs, numaflowControllerROSyncErrors, reconciliationDuration, kubeRequestCounter,
		numaflowControllerKubectlExecutionCounter, kubeResourceCacheMonitored, kubeResourceCache, clusterCacheError,
		pipelinePausedSeconds, isbServicePausedSeconds, numaflowControllerPausedSeconds)

	return &CustomMetrics{
		PipelinesRolloutHealth:                    pipelinesRolloutHealth,
		PipelineRolloutsRunning:                   pipelineRolloutsRunning,
		PipelineROCounterMap:                      make(map[string]map[string]struct{}),
		PipelineROSyncs:                           pipelineROSyncs,
		PipelineROSyncErrors:                      pipelineROSyncErrors,
		PipelineRolloutQueueLength:                pipelineRolloutQueueLength,
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
		NumaflowControllersRolloutHealth:          numaflowControllersRolloutHealth,
		NumaflowControlleRORunning:                numaflowControllerRORunning,
		NumaflowControllersROSyncs:                numaflowControllerROSyncs,
		NumaflowControllerROSyncErrors:            numaflowControllerROSyncErrors,
		KubeRequestCounter:                        kubeRequestCounter,
		NumaflowControllerKubectlExecutionCounter: numaflowControllerKubectlExecutionCounter,
		ReconciliationDuration:                    reconciliationDuration,
		KubeResourceMonitored:                     kubeResourceCacheMonitored,
		KubeResourceCache:                         kubeResourceCache,
		ClusterCacheError:                         clusterCacheError,
		PipelinePausedSeconds:                     pipelinePausedSeconds,
		ISBServicePausedSeconds:                   isbServicePausedSeconds,
		NumaflowControllerPausedSeconds:           numaflowControllerPausedSeconds,
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
