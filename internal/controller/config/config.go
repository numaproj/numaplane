package config

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

type ConfigManager struct {
	config        *GlobalConfig
	rolloutConfig *NumaflowControllerDefinitionConfig
	lock          *sync.RWMutex
}

var instance *ConfigManager
var once sync.Once

// GetConfigManagerInstance  returns a singleton config manager throughout the application
func GetConfigManagerInstance() *ConfigManager {
	once.Do(func() {
		instance = &ConfigManager{
			config:        &GlobalConfig{},
			rolloutConfig: &NumaflowControllerDefinitionConfig{},
			lock:          new(sync.RWMutex),
		}
	})
	return instance
}

// GlobalConfig is the configuration for the controllers, it is
// supposed to be populated from the configmap attached to the
// controller manager.
type GlobalConfig struct {
	LogLevel          int    `json:"logLevel" mapstructure:"logLevel"`
	IncludedResources string `json:"includedResources" mapstructure:"includedResources"`
}

type NumaflowControllerDefinitionConfig struct {
	ControllerDefinitions []apiv1.ControllerDefinitions `json:"controllerDefinitions" mapstructure:"controllerDefinitions"`
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

func (cm *ConfigManager) GetControllerDefinitionsConfig() (NumaflowControllerDefinitionConfig, error) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	config, err := CloneWithSerialization(cm.rolloutConfig)
	if err != nil {
		return NumaflowControllerDefinitionConfig{}, err
	}
	return *config, nil
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
		err := cm.loadConfig(onErrorReloading, opts.configsPath, opts.configFileName, opts.fileType, false)
		if err != nil {
			return err
		}
	}
	if opts.defConfigFileName != "" {
		err := cm.loadConfig(onErrorReloading, opts.defConfigPath, opts.defConfigFileName, opts.fileType, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConfigManager) loadConfig(
	onErrorReloading func(error),
	configsPath, configFileName, configFileType string,
	rolloutConfig bool,
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
		if !rolloutConfig {
			err = v.Unmarshal(cm.config)
		} else {
			err = v.Unmarshal(cm.rolloutConfig)
		}

		if err != nil {
			return fmt.Errorf("failed unmarshal configuration file. %w", err)
		}
	}

	// Rollout Config is immutable
	if !rolloutConfig {
		v.OnConfigChange(func(e fsnotify.Event) {
			cm.lock.Lock()
			defer cm.lock.Unlock()
			newConfig := GlobalConfig{}
			err = v.Unmarshal(&newConfig)
			if err != nil {
				onErrorReloading(err)
			}
			cm.config = &newConfig
		})
		v.WatchConfig()
	}

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
