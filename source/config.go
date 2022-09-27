// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-hubspot/config"
)

// ConfigKeyPollingPeriod is a config name for a polling period.
const ConfigKeyPollingPeriod = "pollingPeriod"

// defaultPollingPeriod is a default PollingPeriod's value used if the PollingPeriod field is empty.
const defaultPollingPeriod = time.Second * 5

// Config holds source-specific configurable values.
type Config struct {
	config.Config

	// PollingPeriod is the duration that defines a period of polling
	// new items if CDC is not available for a resource.
	PollingPeriod time.Duration `key:"pollingPeriod"`
}

// ParseConfig seeks to parse a provided map[string]string into a Config struct.
func ParseConfig(cfg map[string]string) (Config, error) {
	commonConfig, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:        commonConfig,
		PollingPeriod: defaultPollingPeriod,
	}

	// parse pollingPeriod if it's not empty.
	if pollingPeriodStr := cfg[ConfigKeyPollingPeriod]; pollingPeriodStr != "" {
		pollingPeriod, err := time.ParseDuration(pollingPeriodStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse polling period: %w", err)
		}

		sourceConfig.PollingPeriod = pollingPeriod
	}

	return sourceConfig, nil
}
