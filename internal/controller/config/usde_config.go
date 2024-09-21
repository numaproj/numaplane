package config

type USDEConfig struct {
	// If user's config doesn't exist or doesn't specify strategy, this is the default
	DefaultUpgradeStrategy      USDEUserStrategy `json:"defaultUpgradeStrategy" mapstructure:"defaultUpgradeStrategy"`
	PipelineSpecExcludedPaths   []string         `json:"pipelineSpecExcludedPaths,omitempty" yaml:"pipelineSpecExcludedPaths,omitempty"`
	ISBServiceSpecExcludedPaths []string         `json:"isbServiceSpecExcludedPaths,omitempty" yaml:"isbServiceSpecExcludedPaths,omitempty"`
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
