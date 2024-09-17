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

func (s *USDEUserStrategy) UnmarshalJSON(data []byte) (err error) {
	var usdeUserStrategyStr string
	if err := json.Unmarshal(data, &usdeUserStrategyStr); err != nil {
		return err
	}

	allowedValues := map[USDEUserStrategy]struct{}{
		ProgressiveStrategyID: {},
		PPNDStrategyID:        {},
		NoStrategyID:          {}} // TODO: this will no longer be allowed in the future

	// Make sure the string is one of the possible strategy values
	_, found := allowedValues[USDEUserStrategy(usdeUserStrategyStr)]
	if !found {
		return fmt.Errorf("invalid strategy '%s' (allowed values: %+v)", usdeUserStrategyStr, allowedValues)
	}
	// TODO-PROGRESSIVE: replace if-statement above for if-statement below
	// if usdeUserStrategyStr != string(ProgressiveStrategyID) && usdeUserStrategyStr != string(PPNDStrategyID) {
	// 	return fmt.Errorf("invalid strategy '%s' (allowed values are: %s or %s)", usdeUserStrategyStr, ProgressiveStrategyID, PPNDStrategyID)
	// }

	*s = USDEUserStrategy(usdeUserStrategyStr)

	return nil
}

func (s USDEUserStrategy) IsValid() bool {
	switch s {
	case ProgressiveStrategyID:
		return true
	case PPNDStrategyID:
		return true
	default:
		return false
	}
}
