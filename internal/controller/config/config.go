/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type ConfigManager struct {
	config *GlobalConfig
	lock   *sync.RWMutex
	// if the configmap changes, these callbacks will be called
	callbacks []func(config GlobalConfig)

	numaflowControllerDefMgr NumaflowControllerDefinitionsManager

	// USDE Config
	usdeConfig     USDEConfig
	usdeConfigLock *sync.RWMutex

	// User Namespace-level Config
	namespaceConfigMap     map[string]NamespaceConfig
	namespaceConfigMapLock *sync.RWMutex
}

type NumaflowControllerDefinitionsManager struct {
	rolloutConfig map[string]string
	lock          *sync.RWMutex
}

type NamespaceConfig struct {
	UpgradeStrategy USDEUserStrategy `json:"upgradeStrategy,omitempty" yaml:"upgradeStrategy,omitempty"`
}

var instance *ConfigManager
var once sync.Once

// GetConfigManagerInstance returns a singleton config manager throughout the application
func GetConfigManagerInstance() *ConfigManager {
	once.Do(func() {
		instance = &ConfigManager{
			config: &GlobalConfig{},
			lock:   new(sync.RWMutex),
			numaflowControllerDefMgr: NumaflowControllerDefinitionsManager{
				rolloutConfig: map[string]string{},
				lock:          new(sync.RWMutex),
			},
			usdeConfig:             USDEConfig{},
			usdeConfigLock:         new(sync.RWMutex),
			namespaceConfigMap:     make(map[string]NamespaceConfig),
			namespaceConfigMapLock: new(sync.RWMutex),
		}
	})
	return instance
}

func (*ConfigManager) GetControllerDefinitionsMgr() *NumaflowControllerDefinitionsManager {
	return &instance.numaflowControllerDefMgr
}

// GlobalConfig is the configuration for the controllers, it is
// supposed to be populated from the configmap attached to the
// controller manager.
type GlobalConfig struct {
	LogLevel          int    `json:"logLevel" mapstructure:"logLevel"`
	IncludedResources string `json:"includedResources" mapstructure:"includedResources"`
	// List of Numaflow Controller image names to look for
	NumaflowControllerImageNames []string `json:"numaflowControllerImageNames" mapstructure:"numaflowControllerImageNames"`
	// If user's config doesn't exist or doesn't specify strategy, this is the default
	DefaultUpgradeStrategy USDEUserStrategy `json:"defaultUpgradeStrategy" mapstructure:"defaultUpgradeStrategy"`
}

type NumaflowControllerDefinitionConfig struct {
	ControllerDefinitions []apiv1.ControllerDefinitions `json:"controllerDefinitions" yaml:"controllerDefinitions"`
}

func (cm *ConfigManager) GetConfig() (GlobalConfig, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	config, err := CloneWithSerialization(cm.config)
	if err != nil {
		return GlobalConfig{}, err
	}
	return *config, nil
}

func (cm *NumaflowControllerDefinitionsManager) UpdateNumaflowControllerDefinitionConfig(config NumaflowControllerDefinitionConfig) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	// Add or update the controller definition config based on a version
	for _, controller := range config.ControllerDefinitions {
		cm.rolloutConfig[controller.Version] = controller.FullSpec

		log.Debug().Msg(fmt.Sprintf("Added/Updated Controller definition Config, version: %s", config)) // due to cyclical dependency, we can't call logger
	}
}

func (cm *NumaflowControllerDefinitionsManager) RemoveNumaflowControllerDefinitionConfig(config NumaflowControllerDefinitionConfig) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	for _, controller := range config.ControllerDefinitions {
		delete(cm.rolloutConfig, controller.Version)

		log.Debug().Msg(fmt.Sprintf("Removed Controller definition Config, version: %s", controller.Version)) // due to cyclical dependency, we can't call logger
	}
}

func (cm *NumaflowControllerDefinitionsManager) GetNumaflowControllerDefinitionsConfig() map[string]string {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	return cm.rolloutConfig
}

func (cm *ConfigManager) LoadAllConfigs(
	onErrorReloading func(error),
	options ...Option,
) error {
	opts := defaultOptions()
	for _, o := range options {
		if o != nil {
			o(opts)
		}
	}
	if opts.configFileName != "" {
		err := cm.loadGlobalConfig(onErrorReloading, opts.configsPath, opts.configFileName, opts.fileType)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConfigManager) loadGlobalConfig(
	onErrorReloading func(error),
	configsPath, configFileName, configFileType string,
) error {
	v := viper.New()
	v.SetConfigName(configFileName)
	v.SetConfigType(configFileType)
	v.AddConfigPath(configsPath)
	err := v.ReadInConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration file. %w", err)
	}
	{
		cm.lock.Lock()
		defer cm.lock.Unlock()
		err = v.Unmarshal(cm.config)
		if err != nil {
			return fmt.Errorf("failed unmarshal configuration file. %w", err)
		}
	}

	v.OnConfigChange(func(e fsnotify.Event) {
		cm.lock.Lock()
		defer cm.lock.Unlock()
		newConfig := GlobalConfig{}
		err = v.Unmarshal(&newConfig)
		if err != nil {
			onErrorReloading(err)
		}
		cm.config = &newConfig
		log.Debug().Msg(fmt.Sprintf("Global Config update: %+v", newConfig)) // due to cyclical dependency, we can't call logger

		// call any registered callbacks
		for _, f := range cm.callbacks {
			f(*cm.config)
		}
	})
	v.WatchConfig()

	return nil
}

func CloneWithSerialization[T NumaflowControllerDefinitionConfig | GlobalConfig](orig *T) (*T, error) {
	origJSON, err := json.Marshal(orig)
	if err != nil {
		return nil, err
	}
	var clone T
	if err = json.Unmarshal(origJSON, &clone); err != nil {
		return nil, err
	}
	return &clone, nil
}

// RegisterCallback adds a callback to be called when the config changes
func (cm *ConfigManager) RegisterCallback(f func(config GlobalConfig)) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.callbacks = append(cm.callbacks, f)
}

func (cm *ConfigManager) UpdateNamespaceConfig(namespace string, config NamespaceConfig) {
	cm.namespaceConfigMapLock.Lock()
	defer cm.namespaceConfigMapLock.Unlock()

	cm.namespaceConfigMap[namespace] = config
	log.Debug().Msg(fmt.Sprintf("Added Namespace ConfigMap for namespace %s", namespace)) // due to cyclical dependency, we can't call logger
}

func (cm *ConfigManager) UnsetNamespaceConfig(namespace string) {
	cm.namespaceConfigMapLock.Lock()
	defer cm.namespaceConfigMapLock.Unlock()

	delete(cm.namespaceConfigMap, namespace)
	log.Debug().Msg(fmt.Sprintf("Deleted Namespace ConfigMap for namespace %s", namespace)) // due to cyclical dependency, we can't call logger
}

func (cm *ConfigManager) GetNamespaceConfig(namespace string) *NamespaceConfig {
	cm.namespaceConfigMapLock.Lock()
	defer cm.namespaceConfigMapLock.Unlock()

	nsConfigMap, exists := cm.namespaceConfigMap[namespace]
	if !exists {
		return nil
	}

	return &nsConfigMap
}
