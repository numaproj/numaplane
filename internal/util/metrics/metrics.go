package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

type CustomMetrics struct {
	// pipelinesGauge is the gauge for the number of running pipelines.
	pipelinesGauge *prometheus.GaugeVec
	// the pipelineCounterMap contains the information of all running pipelines with "name_namespace" as a key.
	pipelineCounterMap map[string]struct{}
	// isbsvcGauge is the gauge for the number of running ISB services.
	isbsvcGauge *prometheus.GaugeVec
	// the isbsvcCounterMap contains the information of all running isb services with "name_namespace" as a key.
	isbsvcCounterMap map[string]struct{}
	// numaflowControllerGauge is the gauge for the number of running numaflow controllers.
	numaflowControllerGauge *prometheus.GaugeVec
}

var (
	pipelineLock   sync.Mutex
	isbsvcLock     sync.Mutex
	pipelinesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "numaflow_pipelines_running",
			Help: "Number of Numaflow pipelines running",
		},
		[]string{},
	)
	isbsvcGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "numaflow_isb_service_running",
			Help: "Number of Numaflow ISB Service running",
		},
		[]string{},
	)
	numaflowControllerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "numaflow_controller_running",
			Help: "Number of Numaflow controller running",
		},
		[]string{"name", "namespace", "version"},
	)
)

// RegisterCustomMetrics registers the custom metrics to the existing global prometheus registry for pipelines, ISB service and numaflow controller
func RegisterCustomMetrics() *CustomMetrics {
	metrics.Registry.MustRegister(pipelinesGauge, isbsvcGauge, numaflowControllerGauge)

	return &CustomMetrics{
		pipelinesGauge:          pipelinesGauge,
		pipelineCounterMap:      make(map[string]struct{}),
		isbsvcGauge:             isbsvcGauge,
		isbsvcCounterMap:        make(map[string]struct{}),
		numaflowControllerGauge: numaflowControllerGauge,
	}
}

// IncPipelineMetrics increments the pipeline counter if it doesn't already know about the pipeline
func (m *CustomMetrics) IncPipelineMetrics(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	m.pipelineCounterMap[name+"_"+namespace] = struct{}{}
	m.pipelinesGauge.WithLabelValues().Set(float64(len(m.pipelineCounterMap)))
}

// DecPipelineMetrics decrements the pipeline counter
func (m *CustomMetrics) DecPipelineMetrics(name, namespace string) {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	delete(m.pipelineCounterMap, name+"_"+namespace)
	m.pipelinesGauge.WithLabelValues().Set(float64(len(m.pipelineCounterMap)))
}

// GetPipelineCounterMap returns the pipeline counter
func (m *CustomMetrics) GetPipelineCounterMap() map[string]struct{} {
	pipelineLock.Lock()
	defer pipelineLock.Unlock()
	return m.pipelineCounterMap
}

// IncISBServiceMetrics increments the ISBService counter if it doesn't already know about the ISBService
func (m *CustomMetrics) IncISBServiceMetrics(name, namespace string) {
	isbsvcLock.Lock()
	defer isbsvcLock.Unlock()
	m.isbsvcCounterMap[name+"_"+namespace] = struct{}{}
	m.isbsvcGauge.WithLabelValues().Set(float64(len(m.isbsvcCounterMap)))
}

// DecISBServiceMetrics decrements the ISBService counter
func (m *CustomMetrics) DecISBServiceMetrics(name, namespace string) {
	isbsvcLock.Lock()
	defer isbsvcLock.Unlock()
	delete(m.isbsvcCounterMap, name+"_"+namespace)
	m.isbsvcGauge.WithLabelValues().Set(float64(len(m.isbsvcCounterMap)))
}

// GetISBServiceCounterMap returns the ISBService counter
func (m *CustomMetrics) GetISBServiceCounterMap() map[string]struct{} {
	isbsvcLock.Lock()
	defer isbsvcLock.Unlock()
	return m.isbsvcCounterMap
}

// IncNumaflowControllerMetrics increments the Numaflow Controller counter
// if it doesn't already know about the Numaflow Controller
func (m *CustomMetrics) IncNumaflowControllerMetrics(name, namespace, version string) {
	m.numaflowControllerGauge.WithLabelValues(name, namespace, version).Set(1)
}

// DecNumaflowControllerMetrics decrements the Numaflow Controller counter
func (m *CustomMetrics) DecNumaflowControllerMetrics(name, namespace, version string) {
	m.numaflowControllerGauge.WithLabelValues(name, namespace, version).Set(0)
}
