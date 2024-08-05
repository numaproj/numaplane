package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

type CustomMetrics struct {
	// PipelinesRunning is the gauge for the number of running pipelines.
	PipelinesRunning *prometheus.GaugeVec
	// PipelineCounterMap contains the information of all running pipelines with "name_namespace" as a key.
	PipelineCounterMap map[string]struct{}
	// PipelinesSyncFailed is the counter for the total number of failed synced.
	PipelinesSyncFailed *prometheus.CounterVec
	// PipelineRolloutQueueLength is the gauge for the length of pipeline rollout queue.
	PipelineRolloutQueueLength *prometheus.GaugeVec
	// PipelinesSynced is the counter for the total number of pipelines synced.
	PipelinesSynced *prometheus.CounterVec
	// ISBServicesRunning is the gauge for the number of running ISB services.
	ISBServicesRunning *prometheus.GaugeVec
	// ISBServiceCounterMap contains the information of all running isb services with "name_namespace" as a key.
	ISBServiceCounterMap map[string]struct{}
	// ISBServicesSyncFailed is the counter for the total number of ISB service syncing failed.
	ISBServicesSyncFailed *prometheus.CounterVec
	// ISBServicesSynced is the counter for the total number of ISB service synced.
	ISBServicesSynced *prometheus.CounterVec
	// NumaflowControllerVersionCounter contains the information of all running numaflow controllers with "version" as a key and "name_namespace" as a value.
	NumaflowControllerVersionCounter map[string]map[string]struct{}
	// NumaflowControllerRunning is the gauge for the number of running numaflow controllers with a specific version.
	NumaflowControllerRunning *prometheus.GaugeVec
	// NumaflowControllersSyncFailed is the counter for the total number of Numaflow controller syncing failed.
	NumaflowControllersSyncFailed *prometheus.CounterVec
	// NumaflowControllersSynced in the counter for the total number of Numaflow controllers synced.
	NumaflowControllersSynced *prometheus.CounterVec
	// ReconciliationDuration is the histogram for the duration of pipeline, isb service and numaflow controller reconciliation.
	ReconciliationDuration *prometheus.HistogramVec
	// NumaflowControllerKubeRequestCounter Count the number of kubernetes requests during numaflow controller reconciliation
	NumaflowControllerKubeRequestCounter *prometheus.CounterVec
	// NumaflowControllerKubectlExecutionCounter Count the number of kubectl executions during numaflow controller reconciliation
	NumaflowControllerKubectlExecutionCounter *prometheus.CounterVec
	// KubeResourceMonitored count the number of monitored kubernetes resource objects in cache
	KubeResourceMonitored *prometheus.GaugeVec
	// KubeResourceCache count the number of kubernetes resource objects in cache
	KubeResourceCache *prometheus.GaugeVec
	// ClusterCacheError count the total number of cluster cache errors
	ClusterCacheError *prometheus.CounterVec
}

const (
	LabelIntuit     = "intuit_alert"
	LabelVersion    = "version"
	LabelType       = "type"
	LabelPhase      = "phase"
	LabelK8SVersion = "K8SVersion"
)

var (
	defaultLabels          = prometheus.Labels{LabelIntuit: "true"}
	pipelineLock           sync.Mutex
	isbServiceLock         sync.Mutex
	numaflowControllerLock sync.Mutex

	pipelinesRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_pipelines_running",
		Help:        "Number of Numaflow pipelines running",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelinesSynced Check the total number of pipeline synced
	pipelinesSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "pipeline_synced_total",
		Help:        "The total number of pipeline synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// pipelinesSyncFailed Check the total number of pipeline syncs failed
	pipelinesSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
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

	// isbServicesRunning is the gauge for the number of running ISB services.
	isbServicesRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_isb_services_running",
		Help:        "Number of Numaflow ISB Service running",
		ConstLabels: defaultLabels,
	}, []string{})

	// isbServicesSynced Check the total number of ISB services synced
	isbServicesSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "isb_services_synced_total",
		Help:        "The total number of ISB service synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// isbServicesSyncFailed Check the total number of ISB service syncs failed
	isbServicesSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "isb_service_sync_failed_total",
		Help:        "The total number of ISB service sync failed",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerRunning is the gauge for the number of running numaflow controllers.
	numaflowControllerRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_controller_running",
		Help:        "Number of Numaflow controller running",
		ConstLabels: defaultLabels,
	}, []string{LabelVersion})

	// numaflowControllersSynced Check the total number of Numaflow controllers synced
	numaflowControllersSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_synced_total",
		Help:        "The total number of Numaflow controller synced",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllersSyncFailed Check the total number of Numaflow controller syncs failed
	numaflowControllersSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_sync_failed_total",
		Help:        "The total number of Numaflow controller sync failed",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerKubeRequestCounter Check the total number of kubernetes requests for numaflow controller
	numaflowControllerKubeRequestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_kube_request_total",
		Help:        "The total number of kubernetes request for numaflow controller",
		ConstLabels: defaultLabels,
	}, []string{})

	// numaflowControllerKubectlExecutionCounter Check the total number of kubectl executions for numaflow controller
	numaflowControllerKubectlExecutionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_controller_kubectl_execution_total",
		Help:        "The total number of kubectl execution for numaflow controller",
		ConstLabels: defaultLabels,
	}, []string{})

	// reconciliationDuration is the histogram for the duration of pipeline, isb service and numaflow controller reconciliation.
	reconciliationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "numaplane_reconciliation_duration_seconds",
		Help:        "Duration of pipeline reconciliation",
		ConstLabels: defaultLabels,
	}, []string{LabelType, LabelPhase})

	// kubeResourceCacheMonitored count the number of monitored kubernetes resource objects in cache
	kubeResourceCacheMonitored = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_kube_resource_monitored",
		Help:        "Number of monitored kubernetes resource object in cache",
		ConstLabels: defaultLabels,
	}, []string{})

	// kubeResourceCache count the number of kubernetes resource objects in cache
	kubeResourceCache = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "numaflow_kube_resource_cache",
		Help:        "Number of kubernetes resource object in cache",
		ConstLabels: defaultLabels,
	}, []string{LabelK8SVersion})

	// clusterCacheError count the total number of cluster cache errors
	clusterCacheError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        "numaflow_cluster_cache_error_total",
		Help:        "The total number of cluster cache error",
		ConstLabels: defaultLabels,
	}, []string{})
)

// RegisterCustomMetrics registers the custom metrics to the existing global prometheus registry for pipelines, ISB service and numaflow controller
func RegisterCustomMetrics() *CustomMetrics {
	metrics.Registry.MustRegister(pipelinesRunning, pipelinesSynced, pipelinesSyncFailed, pipelineRolloutQueueLength, isbServicesRunning, isbServicesSynced, isbServicesSyncFailed,
		numaflowControllerRunning, numaflowControllersSynced, numaflowControllersSyncFailed, reconciliationDuration, numaflowControllerKubeRequestCounter,
		numaflowControllerKubectlExecutionCounter, kubeResourceCacheMonitored, kubeResourceCache, clusterCacheError)

	return &CustomMetrics{
		PipelinesRunning:                          pipelinesRunning,
		PipelineCounterMap:                        make(map[string]struct{}),
		PipelinesSynced:                           pipelinesSynced,
		PipelinesSyncFailed:                       pipelinesSyncFailed,
		PipelineRolloutQueueLength:                pipelineRolloutQueueLength,
		ISBServicesRunning:                        isbServicesRunning,
		ISBServiceCounterMap:                      make(map[string]struct{}),
		ISBServicesSynced:                         isbServicesSynced,
		ISBServicesSyncFailed:                     isbServicesSyncFailed,
		NumaflowControllerRunning:                 numaflowControllerRunning,
		NumaflowControllerVersionCounter:          make(map[string]map[string]struct{}),
		NumaflowControllersSynced:                 numaflowControllersSynced,
		NumaflowControllersSyncFailed:             numaflowControllersSyncFailed,
		NumaflowControllerKubeRequestCounter:      numaflowControllerKubeRequestCounter,
		NumaflowControllerKubectlExecutionCounter: numaflowControllerKubectlExecutionCounter,
		ReconciliationDuration:                    reconciliationDuration,
		KubeResourceMonitored:                     kubeResourceCacheMonitored,
		KubeResourceCache:                         kubeResourceCache,
		ClusterCacheError:                         clusterCacheError,
	}
}

// IncPipelinesRunningMetrics increments the pipeline counter if it doesn't already know about the pipeline
func (m *CustomMetrics) IncPipelinesRunningMetrics(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	m.PipelineCounterMap[name+"_"+namespace] = struct{}{}
	m.PipelinesRunning.WithLabelValues().Set(float64(len(m.PipelineCounterMap)))
}

// DecPipelineMetrics decrements the pipeline counter
func (m *CustomMetrics) DecPipelineMetrics(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	delete(m.PipelineCounterMap, name+"_"+namespace)
	m.PipelinesRunning.WithLabelValues().Set(float64(len(m.PipelineCounterMap)))
}

// IncISBServiceMetrics increments the ISBService counter if it doesn't already know about the ISBService
func (m *CustomMetrics) IncISBServiceMetrics(name, namespace string) {
	isbServiceLock.Lock()
	defer isbServiceLock.Unlock()
	m.ISBServiceCounterMap[name+"_"+namespace] = struct{}{}
	m.ISBServicesRunning.WithLabelValues().Set(float64(len(m.ISBServiceCounterMap)))
}

// DecISBServiceMetrics decrements the ISBService counter
func (m *CustomMetrics) DecISBServiceMetrics(name, namespace string) {
	isbServiceLock.Lock()
	defer isbServiceLock.Unlock()
	delete(m.ISBServiceCounterMap, name+"_"+namespace)
	m.ISBServicesRunning.WithLabelValues().Set(float64(len(m.ISBServiceCounterMap)))
}

// IncNumaflowControllerMetrics increments the Numaflow Controller counter
// if it doesn't already know about the Numaflow Controller
func (m *CustomMetrics) IncNumaflowControllerMetrics(name, namespace, version string) {
	numaflowControllerLock.Lock()
	defer numaflowControllerLock.Unlock()
	// If the version is not in the map, create a new map for the version
	if _, ok := m.NumaflowControllerVersionCounter[version]; !ok {
		m.NumaflowControllerVersionCounter[version] = make(map[string]struct{})
	}
	m.NumaflowControllerVersionCounter[version][name+"_"+namespace] = struct{}{}
	for key, value := range m.NumaflowControllerVersionCounter {
		m.NumaflowControllerRunning.WithLabelValues(key).Set(float64(len(value)))
	}
}

// DecNumaflowControllerMetrics decrements the Numaflow Controller counter
func (m *CustomMetrics) DecNumaflowControllerMetrics(name, namespace, version string) {
	numaflowControllerLock.Lock()
	defer numaflowControllerLock.Unlock()
	delete(m.NumaflowControllerVersionCounter[version], name+"_"+namespace)
	for key, value := range m.NumaflowControllerVersionCounter {
		m.NumaflowControllerRunning.WithLabelValues(key).Set(float64(len(value)))
	}
}
