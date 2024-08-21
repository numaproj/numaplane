package usde

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	progressiveStrategyID = "progressive"
	ppndStrategyID        = "pause-and-drain"
)

type USDEStrategy string

func (s *USDEStrategy) UnmarshalJSON(data []byte) (err error) {
	// Trim spaces and check length
	dataStrNoSpaces := strings.TrimSpace(string(data[:]))
	if len(dataStrNoSpaces) == 0 {
		return fmt.Errorf("empty strategy (allowed values are: %s or %s)", progressiveStrategyID, ppndStrategyID)
	}

	// Remove the double quotes around the string
	strategyStr := string(data[1 : len(data)-1])

	// Make sure the string is one of the possible strategy values
	if strategyStr != progressiveStrategyID && strategyStr != ppndStrategyID {
		return fmt.Errorf("invalid strategy %s (allowed values are: %s or %s)", string(data[:]), progressiveStrategyID, ppndStrategyID)
	}

	var usdeStrategyStr string
	if err := json.Unmarshal(data, &usdeStrategyStr); err != nil {
		return err
	}

	*s = USDEStrategy(usdeStrategyStr)

	return nil
}
