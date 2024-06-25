package kubernetes

import (
	"context"
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/logger"
)

// StartConfigMapWatcher will start a watcher for configmaps with the given label key and value
func StartConfigMapWatcher(ctx context.Context, config *rest.Config) error {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	namespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return fmt.Errorf("failed to read namespace: %w", err)
	}

	go watchConfigMaps(ctx, client, string(namespace))

	return nil
}

// watchConfigMaps watches for configmaps continuously and updates the controller definition config based on the configmap data
func watchConfigMaps(ctx context.Context, client kubernetes.Interface, namespace string) {
	numaLogger := logger.FromContext(ctx)
	configMapLabel := fmt.Sprintf("%s=%s", common.LabelKeyNumaplaneControllerConfig, common.LabelValueNumaplaneControllerConfig)
	watcher, err := client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: configMapLabel,
	})
	if err != nil {
		numaLogger.Fatal(err, "failed to initialize watcher for configmaps")
		return
	}
	for {
		event, ok := <-watcher.ResultChan()
		if !ok {
			watcher, err = client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{
				LabelSelector: configMapLabel,
			})
			numaLogger.Error(err, "watcher channel closed, restarting watcher")
			continue
		}
		configMap, ok := event.Object.(*corev1.ConfigMap)
		if !ok {
			numaLogger.Error(fmt.Errorf("failed to convert object to configmap"), "")
		}

		// Add or update the controller definition config based on a version if the configmap has the correct label
		for _, v := range configMap.Data {
			var controllerConfig config.NumaflowControllerDefinitionConfig
			if err := yaml.Unmarshal([]byte(v), &controllerConfig); err != nil {
				numaLogger.Error(err, "Failed to unmarshal controller config")
				continue
			}
			// controller config definition is immutable, so no need to update the existing config
			if event.Type == watch.Added {
				config.GetConfigManagerInstance().UpdateControllerDefinitionConfig(controllerConfig)
			} else if event.Type == watch.Deleted {
				config.GetConfigManagerInstance().RemoveControllerDefinitionConfig(controllerConfig)
			}
		}
	}
}
