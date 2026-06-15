package kubernetes

import (
	"k8s.io/client-go/rest"
)

const inClusterServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// RestConfigForTempKubeconfig returns a copy of config for gitops-engine's temp kubeconfig.
//
// gitops-engine WriteKubeConfig copies rest.Config.BearerToken into the kubeconfig as a static
// token. client-go refreshes credentials from BearerTokenFile on each request, but kubectl
// subprocesses use only what is written to the kubeconfig. After a projected service account
// token rotates, the static token goes stale while the main controller client keeps working.
//
// When BearerTokenFile is set, drop the startup BearerToken snapshot so WriteKubeConfig falls
// back to tokenFile in the generated kubeconfig (the standard in-cluster path for pods).
func RestConfigForTempKubeconfig(config *rest.Config) *rest.Config {
	if config == nil || config.BearerTokenFile == "" {
		return config
	}

	copied := rest.CopyConfig(config)
	copied.BearerToken = ""
	return copied
}
