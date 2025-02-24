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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/numaproj/numaplane/internal/common"
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
	// rolloutConfig is a map of controller version to its full spec where key is namespace/version
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
	LogLevel          string `json:"logLevel" mapstructure:"logLevel"`
	IncludedResources string `json:"includedResources" mapstructure:"includedResources"`
	// List of Numaflow Controller image names to look for
	NumaflowControllerImageNames []string `json:"numaflowControllerImageNames" mapstructure:"numaflowControllerImageNames"`
	// If user's config doesn't exist or doesn't specify strategy, this is the default
	DefaultUpgradeStrategy USDEUserStrategy `json:"defaultUpgradeStrategy" mapstructure:"defaultUpgradeStrategy"`

	// Configuration related to the Progressive strategy
	Progressive ProgressiveConfig `json:"progressive" mapstructure:"progressiveConfig"`
}

type ProgressiveConfig struct {
	// schedules for assessing upgrading child (one schedule per Kind)
	DefaultAssessmentSchedule []DefaultAssessmentSchedule `json:"defaultAssessmentSchedule" mapstructure:"defaultAssessmentSchedule"`
}

// DefaultAssessmentSchedule defines a default schedule for each Kind
type DefaultAssessmentSchedule struct {
	Kind string `json:"kind" mapstructure:"kind"`

	// The Schedule variables defines time information used when assessing a child health status.
	// It is a string with 3 comma-separated integer values representing the following:
	// 1. AssessmentDelay: indicates the amount of seconds to delay before assessing the status of the child resource to determine healthiness
	// 2. AssessmentPeriod: indicates the amount of seconds to perform assessments for after the first assessment has been performed
	//    (if failure is indicated before that, it will not continue however)
	// 3. AssessmentInterval: indicates how often to assess the child status once the first assessment has been performed and before the end
	// of the assessments window defined by adding the AssessmentDelay seconds to the AssessmentPeriod seconds
	// NOTE: the order of the values is important since each value represents a specific amount of time
	// Example: "120,60,10" => delay assessment by 120s, assess for 60s, assess every 10s
	// 0 can be used for AssessmentDelay to mean no delay
	// 0 can be used for AssessmentPeriod to not require a period of time to wait before assessment
	// Don't use 0 for AssessmentInterval: if the result is initially "unknown", this value is still needed as a requeing time interval for checking
	Schedule string `json:"schedule" mapstructure:"schedule"`
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

// GetNumaflowControllerDefinitionsConfig looks up the controller definition from user namespace, if not found then use from global namespace.
func (cm *NumaflowControllerDefinitionsManager) GetNumaflowControllerDefinitionsConfig(namespace, version string) (string, error) {
	definition := cm.GetRolloutConfig()
	key := fmt.Sprintf("%s/%s", namespace, version)

	manifest, manifestExists := definition[key]
	if !manifestExists {
		key = fmt.Sprintf("%s/%s", common.NumaplaneSystemNamespace, version)
		manifest, manifestExists = definition[key]
		if !manifestExists {
			return "", fmt.Errorf("no controller definition found for namespace/version %s/%s", namespace, version)
		}
	}

	return manifest, nil
}

func (cm *NumaflowControllerDefinitionsManager) UpdateNumaflowControllerDefinitionConfig(config NumaflowControllerDefinitionConfig, namespace string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	// Add or update the controller definition config based on a version and namespace as key
	for _, controller := range config.ControllerDefinitions {
		key := fmt.Sprintf("%s/%s", namespace, controller.Version)
		cm.rolloutConfig[key] = controller.FullSpec

		log.Debug().Msg(fmt.Sprintf("Added/Updated Controller definition Config, version: %s", config)) // due to cyclical dependency, we can't call logger
	}
}

func (cm *NumaflowControllerDefinitionsManager) RemoveNumaflowControllerDefinitionConfig(config NumaflowControllerDefinitionConfig, namespace string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	// Remove the controller definition config based on a version and namespace as key
	for _, controller := range config.ControllerDefinitions {
		key := fmt.Sprintf("%s/%s", namespace, controller.Version)
		delete(cm.rolloutConfig, key)

		log.Debug().Msg(fmt.Sprintf("Removed Controller definition Config, version: %s", controller.Version)) // due to cyclical dependency, we can't call logger
	}
}

func (cm *NumaflowControllerDefinitionsManager) GetRolloutConfig() map[string]string {
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

type AssessmentSchedule struct {
	Delay    time.Duration
	Period   time.Duration
	Interval time.Duration
}

func ParseAssessmentSchedule(str string) (AssessmentSchedule, error) {
	// Split the schedule string by commas
	parts := strings.Split(str, ",")
	if len(parts) != 3 {
		return AssessmentSchedule{}, fmt.Errorf("invalid schedule format: expected 3 comma-separated values")
	}

	// Convert each part to an integer and then to a time.Duration
	assessmentDelaySeconds, err := strconv.Atoi(parts[0])
	if err != nil {
		return AssessmentSchedule{}, fmt.Errorf("invalid assessmentDelay value: %w", err)
	}
	assessmentPeriodSeconds, err := strconv.Atoi(parts[1])
	if err != nil {
		return AssessmentSchedule{}, fmt.Errorf("invalid assessmentPeriod value: %w", err)
	}
	assessmentIntervalSeconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return AssessmentSchedule{}, fmt.Errorf("invalid assessmentInterval value: %w", err)
	}

	// Convert seconds to time.Duration
	assessmentDelay := time.Duration(assessmentDelaySeconds) * time.Second
	assessmentPeriod := time.Duration(assessmentPeriodSeconds) * time.Second
	assessmentInterval := time.Duration(assessmentIntervalSeconds) * time.Second

	return AssessmentSchedule{
		Delay:    assessmentDelay,
		Period:   assessmentPeriod,
		Interval: assessmentInterval,
	}, nil
}

// GetChildStatusAssessmentSchedule parses the GlobalConfig Default Assessment Schedule string for this kind,
// and returns the assessmentDelay, assessmentPeriod, and assessmentInterval durations.
func (config *ProgressiveConfig) GetChildStatusAssessmentSchedule(kind string) (AssessmentSchedule, error) {

	for _, schedule := range config.DefaultAssessmentSchedule {
		if strings.EqualFold(schedule.Kind, kind) { // case insensitive equals check
			return ParseAssessmentSchedule(schedule.Schedule)
		}
	}
	return AssessmentSchedule{}, fmt.Errorf("No Assessment Schedule found for kind %q", kind)
}
