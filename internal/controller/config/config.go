package config

import (
	"encoding/json"
	"fmt"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
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
	ClusterName             string `json:"clusterName" mapstructure:"clusterName"`
	SyncTimeIntervalMs      int    `json:"syncTimeIntervalMs" mapstructure:"syncTimeIntervalMs"`
	AutomatedSyncDisabled   bool   `json:"automatedSyncDisabled" mapstructure:"automatedSyncDisabled"`
	CascadeDeletion         bool   `json:"cascadeDeletion" mapstructure:"cascadeDeletion"`
	AutoHealDisabled        bool   `json:"autoHealDisabled" mapstructure:"autoHealDisabled"`
	IncludedResources       string `json:"includedResources" mapstructure:"includedResources"`
	LogLevel                int    `json:"logLevel" mapstructure:"logLevel"`
	PersistentRepoClonePath string `json:"persistentRepoClonePath" mapstructure:"persistentRepoClonePath"`
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

func (cm *ConfigManager) GetNumaRolloutConfig() (NumaflowControllerDefinitionConfig, error) {
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
		o(opts)
	}
	if opts.configFileName != "" {
		cm.loadConfig(onErrorReloading, opts.configsPath, opts.configFileName, opts.configFileType, false)
	}
	if opts.rolloutConfigFileName != "" {
		cm.loadConfig(onErrorReloading, opts.configsPath, opts.rolloutConfigFileName, opts.configFileType, true)
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
