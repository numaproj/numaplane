package e2e

import (
	"testing"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// var k8sClient client.Client // global variable declaration

var (
	// kubeClient client.Client
	scheme = runtime.NewScheme()
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	// Add CRDs to the scheme
	_ = apiv1.AddToScheme(scheme)
}

type Given struct {
	t         *testing.T
	k8sClient client.Client
}

func NewGiven(t *testing.T, k8sClient client.Client) *Given {
	// Setup kubeClient if not set
	if k8sClient == nil {
		t.Fatal("Unable to create k8sClient")
	}

	return &Given{t: t, k8sClient: k8sClient}
}
