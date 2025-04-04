package kubernetes

import (
	"fmt"

	"github.com/numaproj/numaplane/pkg/client/clientset/versioned"
	"k8s.io/client-go/dynamic"
	clientkube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var DynamicClient *dynamic.DynamicClient
var KubernetesClient *clientkube.Clientset
var NumaplaneClient *versioned.Clientset

func SetClientSets(restConfig *rest.Config) error {
	var err error
	DynamicClient, err = dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	KubernetesClient, err = clientkube.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %v", err)
	}

	NumaplaneClient, err = versioned.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create numaplane clientset: %v", err)
	}

	return nil
}
