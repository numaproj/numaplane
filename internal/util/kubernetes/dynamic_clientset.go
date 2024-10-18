package kubernetes

import (
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var dynamicClient *dynamic.DynamicClient

func SetDynamicClient(restConfig *rest.Config) error {
	var err error
	dynamicClient, err = dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %v", err)
	}

	return nil
}
