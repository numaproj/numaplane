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
