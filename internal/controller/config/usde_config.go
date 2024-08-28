package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// ProgressiveStrategyID = "progressive" // TODO-PROGRESSIVE: enable this
	PPNDStrategyID = "pause-and-drain"
)

type USDEUserStrategy string

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
	// Trim spaces and check length
	dataStrNoSpaces := strings.TrimSpace(string(data[:]))
	if len(dataStrNoSpaces) == 0 {
		return fmt.Errorf("empty strategy (allowed value is: %s)", PPNDStrategyID)
		// TODO-PROGRESSIVE: replace line above for line below
		// return fmt.Errorf("empty strategy (allowed values are: %s or %s)", ProgressiveStrategyID, PPNDStrategyID)
	}

	// Remove the double quotes around the string
	strategyStr := string(data[1 : len(data)-1])

	// Make sure the string is one of the possible strategy values
	if strategyStr != PPNDStrategyID {
		return fmt.Errorf("invalid strategy %s (allowed value is: %s)", string(data[:]), PPNDStrategyID)
	}
	// TODO-PROGRESSIVE: replace if-statement above for if-statement below
	// if strategyStr != ProgressiveStrategyID && strategyStr != PPNDStrategyID {
	// 	return fmt.Errorf("invalid strategy %s (allowed values are: %s or %s)", string(data[:]), ProgressiveStrategyID, PPNDStrategyID)
	// }

	var usdeUserStrategyStr string
	if err := json.Unmarshal(data, &usdeUserStrategyStr); err != nil {
		return err
	}

	*s = USDEUserStrategy(usdeUserStrategyStr)

	return nil
}
