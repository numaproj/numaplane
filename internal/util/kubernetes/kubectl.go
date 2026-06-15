package kubernetes

import (
	"os"

	"github.com/argoproj/argo-cd/gitops-engine/pkg/utils/kube"
	"github.com/argoproj/argo-cd/gitops-engine/pkg/utils/tracing"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/openapi"

	"github.com/numaproj/numaplane/internal/util/logger"
)

var tracer tracing.Tracer = &tracing.NopTracer{}

func init() {
	if os.Getenv("NUMAPLANE_TRACING_ENABLED") == "1" {
		tracer = tracing.NewLoggingTracer(*logger.New().LogrLogger)
	}
}

type kubectlCmd struct {
	*kube.KubectlCmd
}

func (k *kubectlCmd) ManageResources(config *rest.Config, openAPISchema openapi.Resources) (kube.ResourceOperations, func(), error) {
	return k.KubectlCmd.ManageResources(RestConfigForTempKubeconfig(config), openAPISchema)
}

func NewKubectl() kube.Kubectl {
	return &kubectlCmd{
		KubectlCmd: &kube.KubectlCmd{Tracer: tracer, Log: *logger.New().LogrLogger},
	}
}
