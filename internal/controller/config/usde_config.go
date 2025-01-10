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
	"fmt"

	"github.com/rs/zerolog/log"
)

type SpecField struct {
	Path             string `json:"path" yaml:"path"`
	IncludeSubfields bool   `json:"includeSubfields,omitempty" yaml:"includeSubfields,omitempty"`
}

type USDEResourceConfig struct {
	// Recreate indicates fields that require the resource to be recreated upon modification.
	// For PPND strategy, this list is checked before the other two lists.
	Recreate []SpecField `json:"recreate,omitempty" yaml:"recreate,omitempty"`
	// DataLoss represents fields that, when changed, may result in data loss.
	// For PPND strategy, this list is checked after the 'recreate' list.
	DataLoss []SpecField `json:"dataLoss,omitempty" yaml:"dataLoss,omitempty"`
	// Progressive contains fields that can be updated without requiring a full resource recreation and by performing an in-place update.
	// For PPND strategy, this list is checked after the other two lists.
	Progressive []SpecField `json:"progressive,omitempty" yaml:"progressive,omitempty"`
}

type USDEConfig map[string]USDEResourceConfig

func (cm *ConfigManager) UpdateUSDEConfig(config USDEConfig) {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	cm.usdeConfig = config

	log.Debug().Msg(fmt.Sprintf("USDE Config update: %+v", config)) // due to cyclical dependency, we can't call logger
}

func (cm *ConfigManager) UnsetUSDEConfig() {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	cm.usdeConfig = USDEConfig{}

	log.Debug().Msg("USDE Config unset") // due to cyclical dependency, we can't call logger
}

func (cm *ConfigManager) GetUSDEConfig() USDEConfig {
	cm.usdeConfigLock.Lock()
	defer cm.usdeConfigLock.Unlock()

	return cm.usdeConfig
}
