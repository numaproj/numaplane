package kubernetes

import (
	"testing"

	gitopskube "github.com/argoproj/argo-cd/gitops-engine/pkg/utils/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestRestConfigForTempKubeconfig(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		assert.Nil(t, RestConfigForTempKubeconfig(nil))
	})

	t.Run("no token file", func(t *testing.T) {
		cfg := &rest.Config{BearerToken: "static-token"}
		assert.Equal(t, cfg, RestConfigForTempKubeconfig(cfg))
	})

	t.Run("token file clears stale bearer token", func(t *testing.T) {
		cfg := &rest.Config{
			Host:            "https://kubernetes.default.svc",
			BearerToken:     "stale-token",
			BearerTokenFile: inClusterServiceAccountTokenPath,
		}

		sanitized := RestConfigForTempKubeconfig(cfg)
		require.NotSame(t, cfg, sanitized)
		assert.Empty(t, sanitized.BearerToken)
		assert.Equal(t, inClusterServiceAccountTokenPath, sanitized.BearerTokenFile)

		kubeConfig := gitopskube.NewKubeConfig(sanitized, "")
		authInfo := kubeConfig.AuthInfos[kubeConfig.CurrentContext]
		assert.Empty(t, authInfo.Token)
		assert.Equal(t, inClusterServiceAccountTokenPath, authInfo.TokenFile)
	})

	t.Run("original config unchanged", func(t *testing.T) {
		cfg := &rest.Config{
			BearerToken:     "stale-token",
			BearerTokenFile: inClusterServiceAccountTokenPath,
		}

		_ = RestConfigForTempKubeconfig(cfg)
		assert.Equal(t, "stale-token", cfg.BearerToken)
	})

	t.Run("static bearer token without file is written to kubeconfig", func(t *testing.T) {
		cfg := &rest.Config{
			Host:        "https://kubernetes.default.svc",
			BearerToken: "static-token",
		}

		kubeConfig := gitopskube.NewKubeConfig(cfg, "")
		authInfo := kubeConfig.AuthInfos[kubeConfig.CurrentContext]
		assert.Equal(t, "static-token", authInfo.Token)
		assert.Empty(t, authInfo.TokenFile)
	})
}
