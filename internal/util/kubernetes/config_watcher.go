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

	numaplaneNamespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return fmt.Errorf("failed to read namespace: %w", err)
	}

	go watchConfigMaps(ctx, client, string(numaplaneNamespace))

	return nil
}

// watchConfigMaps watches for ConfigMaps continuously and updates the in-memory config objects based on the ConfigMaps data
func watchConfigMaps(ctx context.Context, client kubernetes.Interface, numaplaneNamespace string) {
	numaLogger := logger.FromContext(ctx)

	watcher, err := client.CoreV1().ConfigMaps("").Watch(ctx, metav1.ListOptions{
		LabelSelector: common.LabelKeyNumaplaneControllerConfig,
	})
	if err != nil {
		numaLogger.Fatal(err, "failed to initialize watcher for configmaps")
		return
	}

	for {
		event, ok := <-watcher.ResultChan()
		if !ok {
			watcher, err = client.CoreV1().ConfigMaps("").Watch(ctx, metav1.ListOptions{
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

			// Only handle this kind of ConfigMap if it is in the Numaplane namespace
			if configMap.Namespace != numaplaneNamespace {
				break
			}

			handleNumaflowControllerDefinitionsConfigMapEvent(ctx, configMap, event)

		case common.LabelValueUSDEConfig:
			// Only handle this kind of ConfigMap if it is in the Numaplane namespace
			if configMap.Namespace != numaplaneNamespace {
				break
			}

			if err := handleUSDEConfigMapEvent(configMap, event); err != nil {
				numaLogger.Error(err, "error while handling event on USDE ConfigMap")
			}

		case common.LabelValueNamespaceConfig:
			// Only handle this kind of ConfigMap if it is NOT in the Numaplane namespace
			if configMap.Namespace == numaplaneNamespace {
				break
			}

			if err := handleNamespaceConfigMapEvent(configMap, event); err != nil {
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

		err := yaml.Unmarshal([]byte(configMap.Data["pipelineSpecDataLossFields"]), &usdeConfig.PipelineSpecDataLossFields)
		if err != nil {
			return fmt.Errorf("error unmarshalling USDE PipelineSpecDataLossFields: %v", err)
		}

		err = yaml.Unmarshal([]byte(configMap.Data["isbServiceSpecDataLossFields"]), &usdeConfig.ISBServiceSpecDataLossFields)
		if err != nil {
			return fmt.Errorf("error unmarshalling USDE ISBServiceSpecDataLossFields: %v", err)
		}

		config.GetConfigManagerInstance().UpdateUSDEConfig(usdeConfig)
	} else if event.Type == watch.Deleted {
		config.GetConfigManagerInstance().UnsetUSDEConfig()
	}

	return nil
}

func handleNamespaceConfigMapEvent(configMap *corev1.ConfigMap, event watch.Event) error {
	if event.Type == watch.Added || event.Type == watch.Modified {
		if configMap == nil || configMap.Data == nil {
			return fmt.Errorf("no ConfigMap or data field available for Namespace-level ConfigMap")
		}

		namespaceConfig := config.NamespaceConfig{}
		err := util.StructToStruct(configMap.Data, &namespaceConfig)
		if err != nil {
			return fmt.Errorf("error converting Namespace-level ConfigMap: %v", err)
		}

		config.GetConfigManagerInstance().UpdateNamespaceConfig(configMap.Namespace, namespaceConfig)
	} else if event.Type == watch.Deleted {
		config.GetConfigManagerInstance().UnsetNamespaceConfig(configMap.Namespace)
	}

	return nil
}
