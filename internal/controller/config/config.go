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
	config *GlobalConfig
	lock   *sync.RWMutex
	// if the configmap changes, these callbacks will be called
	callbacks []func(config GlobalConfig)

	controllerDefMgr ControllerDefinitionsManager
}

type ControllerDefinitionsManager struct {
	rolloutConfig map[string]string
	lock          *sync.RWMutex
}

var instance *ConfigManager
var once sync.Once

// GetConfigManagerInstance returns a singleton config manager throughout the application
func GetConfigManagerInstance() *ConfigManager {
	once.Do(func() {
		instance = &ConfigManager{
			config: &GlobalConfig{},
			lock:   new(sync.RWMutex),
			controllerDefMgr: ControllerDefinitionsManager{
				rolloutConfig: map[string]string{},
				lock:          new(sync.RWMutex),
			},
		}
	})
	return instance
}

func (*ConfigManager) GetControllerDefinitionsMgr() *ControllerDefinitionsManager {
	return &instance.controllerDefMgr
}

// GlobalConfig is the configuration for the controllers, it is
// supposed to be populated from the configmap attached to the
// controller manager.
type GlobalConfig struct {
	LogLevel          int    `json:"logLevel" mapstructure:"logLevel"`
	IncludedResources string `json:"includedResources" mapstructure:"includedResources"`
	// Feature flag - if enabled causes pipeline(s) to be paused when pipeline, numaflow controller, or ISB Service gets updated
	DataLossPrevention bool `json:"dataLossPrevention" mapstructure:"dataLossPrevention"`
	// List of Numaflow Controller image names to look for
	NumaflowControllerImageNames []string `json:"numaflowControllerImageNames" mapstructure:"numaflowControllerImageNames"`
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

func (cm *ControllerDefinitionsManager) UpdateControllerDefinitionConfig(config NumaflowControllerDefinitionConfig) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	// Add or update the controller definition config based on a version
	for _, controller := range config.ControllerDefinitions {
		cm.rolloutConfig[controller.Version] = controller.FullSpec
	}
}

func (cm *ControllerDefinitionsManager) RemoveControllerDefinitionConfig(config NumaflowControllerDefinitionConfig) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	for _, controller := range config.ControllerDefinitions {
		delete(cm.rolloutConfig, controller.Version)
	}
}

func (cm *ControllerDefinitionsManager) GetControllerDefinitionsConfig() map[string]string {
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
