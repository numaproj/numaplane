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
)

type USDEUserStrategy string

const (
	ProgressiveStrategyID USDEUserStrategy = "progressive"
	PPNDStrategyID        USDEUserStrategy = "pause-and-drain"
	NoStrategyID          USDEUserStrategy = "no-strategy"
)

func (s *USDEUserStrategy) UnmarshalJSON(data []byte) (err error) {
	var usdeUserStrategyStr string
	if err := json.Unmarshal(data, &usdeUserStrategyStr); err != nil {
		return err
	}

	allowedValues := map[USDEUserStrategy]struct{}{
		ProgressiveStrategyID: {},
		PPNDStrategyID:        {},
		NoStrategyID:          {}}

	// Make sure the string is one of the possible strategy values
	_, found := allowedValues[USDEUserStrategy(usdeUserStrategyStr)]
	if !found {
		return fmt.Errorf("invalid strategy '%s' (allowed values: %+v)", usdeUserStrategyStr, allowedValues)
	}

	*s = USDEUserStrategy(usdeUserStrategyStr)

	return nil
}

func (s USDEUserStrategy) IsValid() bool {
	switch s {
	case ProgressiveStrategyID:
		return true
	case PPNDStrategyID:
		return true
	case NoStrategyID:
		return true
	default:
		return false
	}
}
