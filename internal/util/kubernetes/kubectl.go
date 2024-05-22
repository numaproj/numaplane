package kubernetes

import (
	"os"

	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	"github.com/argoproj/gitops-engine/pkg/utils/tracing"

	"github.com/numaproj/numaplane/internal/util/logger"
)

var tracer tracing.Tracer = &tracing.NopTracer{}

func init() {
	if os.Getenv("NUMAPLANE_TRACING_ENABLED") == "1" {
		tracer = tracing.NewLoggingTracer(*logger.New().LogrLogger)
	}
}

func NewKubectl() kube.Kubectl {
	return &kube.KubectlCmd{Tracer: tracer, Log: *logger.New().LogrLogger}
}
