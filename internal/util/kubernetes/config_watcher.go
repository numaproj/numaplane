package kubernetes

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/numaproj/numaplane/internal/util/logger"
)

const (
	labelKey   = "config"
	labelValue = "numaflow-controller-rollout"
)

// StartConfigMapWatcher will start a watcher for configmaps with the given label key and value
func StartConfigMapWatcher(ctx context.Context, config *rest.Config, dir string) error {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	namespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return fmt.Errorf("failed to read namespace: %w", err)
	}

	if err = os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err = os.WriteFile(fmt.Sprintf("%s/%s", dir, "controller_definitions.yaml"), []byte(""), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	go watchConfigMaps(ctx, client, string(namespace), dir)

	return nil
}

// watchConfigMaps watches for configmaps continuously and writes the data to the given directory based on an event type.
func watchConfigMaps(ctx context.Context, client *kubernetes.Clientset, namespace, dir string) {
	numaLogger := logger.FromContext(ctx)
	watcher, err := client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{})
	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				watcher, err = client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{})
				if err != nil {
					numaLogger.Error(err, "Failed to watch configmaps")
				}
				break
			}
			configMap, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				numaLogger.Error(fmt.Errorf("failed to convert object to configmap"), "")
			}

			if value, ok := configMap.Labels[labelKey]; ok && value == labelValue {
				for k, v := range configMap.Data {
					if event.Type == watch.Added || event.Type == watch.Modified {
						if err := os.WriteFile(fmt.Sprintf("%s/%s", dir, k), []byte(v), 0644); err != nil {
							numaLogger.Error(err, "Failed to write file")
						}
					} else if event.Type == watch.Deleted {
						if err := os.Remove(fmt.Sprintf("%s/%s", dir, k)); err != nil {
							numaLogger.Error(err, "Failed to remove file")
						}
					} else {
						numaLogger.Info("Unknown event type")
					}
				}
			}
		}
	}
}
