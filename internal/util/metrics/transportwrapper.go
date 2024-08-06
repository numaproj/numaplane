package metrics

import (
	"github.com/argoproj/pkg/kubeclientmetrics"
	"k8s.io/client-go/rest"
)

// AddMetricsTransportWrapper adds a transport wrapper which increments 'numaplane_app_k8s_request_total' counter on each kubernetes request
func AddMetricsTransportWrapper(metrics *CustomMetrics, config *rest.Config) *rest.Config {
	fn := func(resourceInfo kubeclientmetrics.ResourceInfo) error {
		metrics.KubeRequestCounter.WithLabelValues().Inc()
		return nil
	}

	newConfig := kubeclientmetrics.AddMetricsTransportWrapper(config, fn)
	return newConfig
}
