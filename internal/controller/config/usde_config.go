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

import "fmt"

type SpecDataLossField struct {
	Path             string `json:"path" yaml:"path"`
	IncludeSubfields bool   `json:"includeSubfields,omitempty" yaml:"includeSubfields,omitempty"`
}

type USDEConfig struct {
	PipelineSpecDataLossFields   []SpecDataLossField `json:"pipelineSpecDataLossFields,omitempty" yaml:"pipelineSpecDataLossFields,omitempty"`
	ISBServiceSpecDataLossFields []SpecDataLossField `json:"isbServiceSpecDataLossFields,omitempty" yaml:"isbServiceSpecDataLossFields,omitempty"`
}

func (cm *ConfigManager) UpdateUSDEConfig(config USDEConfig) {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	cm.usdeConfig = config

	fmt.Printf("USDE Config update: %+v\n", config) // due to cyclical dependency, we can't call logger
}

func (cm *ConfigManager) UnsetUSDEConfig() {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	cm.usdeConfig = USDEConfig{}

	fmt.Println("USDE Config unset") // due to cyclical dependency, we can't call logger
}

func (cm *ConfigManager) GetUSDEConfig() USDEConfig {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	return cm.usdeConfig
}
