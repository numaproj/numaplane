package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

type CustomMetrics struct {
	// PipelinesGauge is the gauge for the number of running pipelines.
	PipelinesGauge *prometheus.GaugeVec
	// PipelineCounterMap contains the information of all running pipelines with "name_namespace" as a key.
	PipelineCounterMap map[string]struct{}
	// PipelineSyncFailed is the counter for the total number of pipeline syncing failed.
	PipelineSyncFailed *prometheus.CounterVec
	// PipelineRolloutQueueLength is the gauge for the length of pipeline rollout queue.
	PipelineRolloutQueueLength *prometheus.GaugeVec
	// PipelineSynced is the counter for the total number of pipeline synced.
	PipelineSynced *prometheus.CounterVec
	// ISBServiceGauge is the gauge for the number of running ISB services.
	ISBServiceGauge *prometheus.GaugeVec
	// ISBServiceCounterMap contains the information of all running isb services with "name_namespace" as a key.
	ISBServiceCounterMap map[string]struct{}
	// ISBServiceSyncFailed is the counter for the total number of ISB service syncing failed.
	ISBServiceSyncFailed *prometheus.CounterVec
	// ISBServicesSynced is the counter for the total number of ISB service synced.
	ISBServicesSynced *prometheus.CounterVec
	// NumaflowControllerVersionCounter contains the information of all running numaflow controllers with "version" as a key and "name_namespace" as a value.
	NumaflowControllerVersionCounter map[string]map[string]struct{}
	// NumaflowControllerGauge is the gauge for the number of running numaflow controllers with a specific version.
	NumaflowControllerGauge *prometheus.GaugeVec
	// NumaflowControllerSyncFailed is the counter for the total number of Numaflow controller syncing failed.
	NumaflowControllerSyncFailed *prometheus.CounterVec
	// NumaflowControllerSynced in the gauge for the total number of Numaflow controllers synced.
	NumaflowControllerSynced *prometheus.GaugeVec
	// ReconciliationDuration is the histogram for the duration of pipeline, isb service and numaflow controller reconciliation.
	ReconciliationDuration *prometheus.HistogramVec
	// NumaflowKubeRequestCounter Count the number of kubernetes required during numaflow controller reconciliation
	NumaflowKubeRequestCounter *prometheus.CounterVec
	// NumaflowKubectlExecutionCounter Count the number of kubectl executions during numaflow controller reconciliation
	NumaflowKubectlExecutionCounter *prometheus.CounterVec
	// KubeResourceMonitored count the number of monitored kubernetes resource objects in cache
	KubeResourceMonitored *prometheus.GaugeVec
	// KubeResourceCache count the number of kubernetes resource objects in cache
	KubeResourceCache *prometheus.GaugeVec
	// ClusterCacheError count the total number of cluster cache errors
	ClusterCacheError *prometheus.CounterVec
}

const (
	LabelVersion    = "version"
	LabelType       = "type"
	LabelPhase      = "phase"
	LabelK8SVersion = "K8SVersion"
)

var (
	pipelineLock                sync.Mutex
	isbServiceLock              sync.Mutex
	numaflowControllerLock      sync.Mutex
	numaflowControllerMaxSynced int
	pipelinesGauge              = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "numaflow_pipelines_running",
		Help: "Number of Numaflow pipelines running",
	}, []string{})

	// pipelineSynced Check the total number of pipeline synced
	pipelineSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_synced_total",
		Help: "The total number of pipeline synced",
	}, []string{})

	// pipelineSyncFailed Check the total number of pipeline syncs failed
	pipelineSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipeline_sync_failed_total",
		Help: "The total number of pipeline sync failed",
	}, []string{})

	// pipelineRolloutQueueLength check the length of queue
	pipelineRolloutQueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pipeline_rollout_queue_length",
		Help: "The length of pipeline rollout queue",
	}, []string{})

	// isbServiceGauge is the gauge for the number of running ISB services.
	isbServiceGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "numaflow_isb_services_running",
		Help: "Number of Numaflow ISB Service running",
	}, []string{})

	// isbServicesSynced Check the total number of ISB services synced
	isbServicesSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "isb_services_synced_total",
		Help: "The total number of ISB service synced",
	}, []string{})

	// isbServiceSyncFailed Check the total number of ISB service syncs failed
	isbServiceSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "isb_service_sync_failed_total",
		Help: "The total number of ISB service sync failed",
	}, []string{})

	// numaflowControllerGauge is the gauge for the number of running numaflow controllers.
	numaflowControllerGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "numaflow_controller_running",
		Help: "Number of Numaflow controller running",
	}, []string{LabelVersion})

	// numaflowControllerSyncFailed Check the total number of Numaflow controller syncs failed
	numaflowControllerSyncFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "numaflow_controller_sync_failed_total",
		Help: "The total number of Numaflow controller sync failed",
	}, []string{})

	// numaflowKubeRequestCounter Check the total number of kubernetes request for numaflow controller
	numaflowKubeRequestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "numaflow_kube_request_total",
		Help: "The total number of kubernetes request for numaflow controller",
	}, []string{})

	// numaflowKubectlExecutionCounter Check the total number of kubectl execution for numaflow controller
	numaflowKubectlExecutionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "numaflow_kubectl_execution_total",
		Help: "The total number of kubectl execution for numaflow controller",
	}, []string{})

	// reconciliationDuration is the histogram for the duration of pipeline, isb service and numaflow controller reconciliation.
	reconciliationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "numaplane_reconciliation_duration_seconds",
		Help: "Duration of pipeline reconciliation",
	}, []string{LabelType, LabelPhase})

	// numaflowControllerSynced Check the total number of Numaflow controllers synced
	numaflowControllerSynced = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "numaflow_controller_synced_total", //
		Help: "The total number of Numaflow controller synced",
	}, []string{})

	// kubeResourceMonitored count the number of monitored kubernetes resource objects in cache
	kubeResourceMonitored = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "numaflow_kube_resource_monitored",
		Help: "Number of monitored kubernetes resource object in cache",
	}, []string{})

	// kubeResourceCache count the number of kubernetes resource objects in cache
	kubeResourceCache = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "numaflow_kube_resource_cache",
		Help: "Number of kubernetes resource object in cache",
	}, []string{LabelK8SVersion})

	// clusterCacheError count the total number of cluster cache errors
	clusterCacheError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "numaflow_cluster_cache_error_total",
		Help: "The total number of cluster cache error",
	}, []string{})
)

// RegisterCustomMetrics registers the custom metrics to the existing global prometheus registry for pipelines, ISB service and numaflow controller
func RegisterCustomMetrics() *CustomMetrics {
	metrics.Registry.MustRegister(pipelinesGauge, pipelineSynced, isbServiceGauge, numaflowControllerGauge, reconciliationDuration,
		pipelineSyncFailed, pipelineRolloutQueueLength, isbServiceSyncFailed, numaflowControllerSyncFailed, numaflowKubeRequestCounter,
		numaflowKubectlExecutionCounter, kubeResourceMonitored, kubeResourceCache, clusterCacheError)

	return &CustomMetrics{
		PipelinesGauge:                   pipelinesGauge,
		PipelineCounterMap:               make(map[string]struct{}),
		PipelineSynced:                   pipelineSynced,
		PipelineSyncFailed:               pipelineSyncFailed,
		PipelineRolloutQueueLength:       pipelineRolloutQueueLength,
		ISBServiceGauge:                  isbServiceGauge,
		ISBServiceCounterMap:             make(map[string]struct{}),
		ISBServicesSynced:                isbServicesSynced,
		ISBServiceSyncFailed:             isbServiceSyncFailed,
		NumaflowControllerGauge:          numaflowControllerGauge,
		NumaflowControllerVersionCounter: make(map[string]map[string]struct{}),
		NumaflowControllerSynced:         numaflowControllerSynced,
		NumaflowControllerSyncFailed:     numaflowControllerSyncFailed,
		NumaflowKubeRequestCounter:       numaflowKubeRequestCounter,
		NumaflowKubectlExecutionCounter:  numaflowKubectlExecutionCounter,
		ReconciliationDuration:           reconciliationDuration,
		KubeResourceMonitored:            kubeResourceMonitored,
		KubeResourceCache:                kubeResourceCache,
		ClusterCacheError:                clusterCacheError,
	}
}

// IncPipelineMetrics increments the pipeline counter if it doesn't already know about the pipeline
func (m *CustomMetrics) IncPipelineMetrics(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	if _, ok := m.PipelineCounterMap[name+"_"+namespace]; !ok {
		m.PipelineCounterMap[name+"_"+namespace] = struct{}{}
		m.PipelineSynced.WithLabelValues().Inc()
	}
	m.PipelinesGauge.WithLabelValues().Set(float64(len(m.PipelineCounterMap)))
}

// DecPipelineMetrics decrements the pipeline counter
func (m *CustomMetrics) DecPipelineMetrics(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	delete(m.PipelineCounterMap, name+"_"+namespace)
	m.PipelinesGauge.WithLabelValues().Set(float64(len(m.PipelineCounterMap)))
}

// IncISBServiceMetrics increments the ISBService counter if it doesn't already know about the ISBService
func (m *CustomMetrics) IncISBServiceMetrics(name, namespace string) {
	isbServiceLock.Lock()
	defer isbServiceLock.Unlock()
	if _, ok := m.ISBServiceCounterMap[name+"_"+namespace]; !ok {
		m.ISBServiceCounterMap[name+"_"+namespace] = struct{}{}
		m.ISBServicesSynced.WithLabelValues().Inc()
	}
	m.ISBServiceGauge.WithLabelValues().Set(float64(len(m.ISBServiceCounterMap)))
}

// DecISBServiceMetrics decrements the ISBService counter
func (m *CustomMetrics) DecISBServiceMetrics(name, namespace string) {
	isbServiceLock.Lock()
	defer isbServiceLock.Unlock()
	delete(m.ISBServiceCounterMap, name+"_"+namespace)
	m.ISBServiceGauge.WithLabelValues().Set(float64(len(m.ISBServiceCounterMap)))
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
	var totalNumaflowRunning int
	for key, value := range m.NumaflowControllerVersionCounter {
		m.NumaflowControllerGauge.WithLabelValues(key).Set(float64(len(value)))
		totalNumaflowRunning += len(value)
	}
	if totalNumaflowRunning > numaflowControllerMaxSynced {
		numaflowControllerMaxSynced = totalNumaflowRunning
		m.NumaflowControllerSynced.WithLabelValues().Set(float64(numaflowControllerMaxSynced))
	}
}

// DecNumaflowControllerMetrics decrements the Numaflow Controller counter
func (m *CustomMetrics) DecNumaflowControllerMetrics(name, namespace, version string) {
	numaflowControllerLock.Lock()
	defer numaflowControllerLock.Unlock()
	delete(m.NumaflowControllerVersionCounter[version], name+"_"+namespace)
	for key, value := range m.NumaflowControllerVersionCounter {
		m.NumaflowControllerGauge.WithLabelValues(key).Set(float64(len(value)))
	}
}
