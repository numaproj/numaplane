package config

import (
	"encoding/json"
	"fmt"
)

type USDEUserStrategy string

const (
	ProgressiveStrategyID USDEUserStrategy = "progressive"
	PPNDStrategyID        USDEUserStrategy = "pause-and-drain"
	NoStrategyID          USDEUserStrategy = ""
)

type USDEConfig struct {
	PipelineSpecExcludedPaths   []string `json:"pipelineSpecExcludedPaths,omitempty" yaml:"pipelineSpecExcludedPaths,omitempty"`
	ISBServiceSpecExcludedPaths []string `json:"isbServiceSpecExcludedPaths,omitempty" yaml:"isbServiceSpecExcludedPaths,omitempty"`
}

func (cm *ConfigManager) UpdateUSDEConfig(config USDEConfig) {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	cm.usdeConfig = config
}

func (cm *ConfigManager) UnsetUSDEConfig() {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	cm.usdeConfig = USDEConfig{}
}

func (cm *ConfigManager) GetUSDEConfig() USDEConfig {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	return cm.usdeConfig
}

func (s *USDEUserStrategy) UnmarshalJSON(data []byte) (err error) {
	var usdeUserStrategyStr string
	if err := json.Unmarshal(data, &usdeUserStrategyStr); err != nil {
		return err
	}

	// Make sure the string is one of the possible strategy values
	if usdeUserStrategyStr != string(PPNDStrategyID) {
		return fmt.Errorf("invalid strategy '%s' (allowed value is: %s)", usdeUserStrategyStr, PPNDStrategyID)
	}
	// TODO-PROGRESSIVE: replace if-statement above for if-statement below
	// if usdeUserStrategyStr != string(ProgressiveStrategyID) && usdeUserStrategyStr != string(PPNDStrategyID) {
	// 	return fmt.Errorf("invalid strategy '%s' (allowed values are: %s or %s)", usdeUserStrategyStr, ProgressiveStrategyID, PPNDStrategyID)
	// }

	*s = USDEUserStrategy(usdeUserStrategyStr)

	return nil
}
