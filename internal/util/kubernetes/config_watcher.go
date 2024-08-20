package kubernetes

import (
	"context"
	"errors"
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
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/logger"
)

// StartConfigMapWatcher will start a watcher for ConfigMaps with the given label key and value
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

	namespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get namespaces list: %v", err)
	}

	for _, ns := range namespaces.Items {
		if ns.Name != string(namespace) {
			go watchConfigMaps(ctx, client, ns.Name)
		}
	}

	return nil
}

// watchConfigMaps watches for ConfigMaps continuously and updates the in-memory config objects based on the ConfigMaps data
func watchConfigMaps(ctx context.Context, client kubernetes.Interface, namespace string) {
	numaLogger := logger.FromContext(ctx)

	watcher, err := client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: common.LabelKeyNumaplaneControllerConfig,
	})
	if err != nil {
		numaLogger.Fatal(err, "failed to initialize watcher for configmaps")
		return
	}

	for {
		event, ok := <-watcher.ResultChan()
		if !ok {
			watcher, err = client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{
				LabelSelector: common.LabelKeyNumaplaneControllerConfig,
			})
			numaLogger.Error(err, "watcher channel closed, restarting watcher")
			continue
		}

		configMap, ok := event.Object.(*corev1.ConfigMap)
		if !ok {
			numaLogger.Error(fmt.Errorf("failed to convert object to configmap"), "")
		}

		labelVal := configMap.Labels[common.LabelKeyNumaplaneControllerConfig]
		switch labelVal {

		case common.LabelValueNumaflowControllerDefinitions:
			// TODO: only do this if the ConfigMap is in the numaplane-system namespace
			handleNumaflowControllerDefinitionsConfigMapEvent(ctx, configMap, event)

		case common.LabelValueUSDEConfig:
			// TODO: only do this if the ConfigMap is in the numaplane-system namespace
			if err := handleUSDEConfigMapEvent(configMap, event); err != nil {
				numaLogger.Error(err, "error while handling event on USDE ConfigMap")
			}

		case common.LabelValueNamespaceConfig:
			// TODO: only do this if the ConfigMap is NOT in the numaplane-system namespace
			if err := handleNamespaceConfigMapEvent(namespace, configMap, event); err != nil {
				numaLogger.WithValues("configMap", configMap).Error(err, "error while handling event on namespace-level ConfigMap")
			}

		default:
			numaLogger.Errorf(err, "the ConfigMap named '%s' is not supported", configMap.Name)

		}
	}
}

func handleNumaflowControllerDefinitionsConfigMapEvent(ctx context.Context, configMap *corev1.ConfigMap, event watch.Event) {
	numaLogger := logger.FromContext(ctx)

	// Add or update the controller definition config based on a version if the configmap has the correct label
	for _, v := range configMap.Data {
		var controllerConfig config.NumaflowControllerDefinitionConfig
		if err := yaml.Unmarshal([]byte(v), &controllerConfig); err != nil {
			numaLogger.Error(err, "failed to unmarshal Numaflow Controller Definitions config")
			continue
		}

		// controller config definition is immutable, so no need to update the existing config
		if event.Type == watch.Added {
			config.GetConfigManagerInstance().GetControllerDefinitionsMgr().UpdateNumaflowControllerDefinitionConfig(controllerConfig)
		} else if event.Type == watch.Deleted {
			config.GetConfigManagerInstance().GetControllerDefinitionsMgr().RemoveNumaflowControllerDefinitionConfig(controllerConfig)
		}
	}
}

func handleUSDEConfigMapEvent(configMap *corev1.ConfigMap, event watch.Event) error {
	if event.Type == watch.Added || event.Type == watch.Modified {
		if configMap == nil || configMap.Data == nil {
			return errors.New("no ConfigMap or data field available for USDE Config")
		}

		usdeConfig := config.USDEConfig{}

		err := yaml.Unmarshal([]byte(configMap.Data["pipelineSpecExcludedPaths"]), &usdeConfig.PipelineSpecExcludedPaths)
		if err != nil {
			return fmt.Errorf("error unmarshalling USDE PipelineSpecExcludedPaths: %v", err)
		}

		err = yaml.Unmarshal([]byte(configMap.Data["isbServiceSpecExcludedPaths"]), &usdeConfig.ISBServiceSpecExcludedPaths)
		if err != nil {
			return fmt.Errorf("error unmarshalling USDE ISBServiceSpecExcludedPaths: %v", err)
		}

		config.GetConfigManagerInstance().UpdateUSDEConfig(usdeConfig)
	} else if event.Type == watch.Deleted {
		config.GetConfigManagerInstance().UnsetUSDEConfig()
	}

	return nil
}

func handleNamespaceConfigMapEvent(namespace string, configMap *corev1.ConfigMap, event watch.Event) error {
	if event.Type == watch.Added || event.Type == watch.Modified {
		if configMap == nil || configMap.Data == nil {
			return fmt.Errorf("no ConfigMap or data field available for Namespace-level Config (namespace: %s)", namespace)
		}

		namespaceConfig := config.NamespaceConfig{}
		err := util.StructToStruct(configMap.Data, &namespaceConfig)
		if err != nil {
			return fmt.Errorf("error converting Namespace-level ConfigMap: %v", err)
		}

		config.GetConfigManagerInstance().UpdateNamespaceConfig(namespace, namespaceConfig)
	} else if event.Type == watch.Deleted {
		config.GetConfigManagerInstance().UnsetNamespaceConfig(namespace)
	}

	return nil
}
