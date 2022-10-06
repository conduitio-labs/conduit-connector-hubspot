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
	"strconv"
	"time"

	"github.com/conduitio-labs/conduit-connector-hubspot/config"
	"github.com/conduitio-labs/conduit-connector-hubspot/validator"
)

const (
	// ConfigKeyPollingPeriod is a config name for a polling period.
	ConfigKeyPollingPeriod = "pollingPeriod"
	// ConfigKeyBufferSize is a config name for a buffer size.
	ConfigKeyBufferSize = "bufferSize"
)

const (
	// defaultPollingPeriod is a default PollingPeriod's value used if the PollingPeriod field is empty.
	defaultPollingPeriod = time.Second * 5
	// defaultBufferSize is a default BufferSize's value used if the BufferSize field is empty.
	defaultBufferSize = 100
)

// Config holds source-specific configurable values.
type Config struct {
	config.Config

	// PollingPeriod is the duration that defines a period of polling
	// new items if CDC is not available for a resource.
	PollingPeriod time.Duration `key:"pollingPeriod"`
	// BufferSize is the buffer size for consumed items.
	// It will also be used as a limit when retrieving items from the HubSpot API.
	BufferSize int `key:"bufferSize" validate:"gte=1,lte=1000"`
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
		BufferSize:    defaultBufferSize,
	}

	// parse pollingPeriod if it's not empty.
	if pollingPeriodStr := cfg[ConfigKeyPollingPeriod]; pollingPeriodStr != "" {
		pollingPeriod, err := time.ParseDuration(pollingPeriodStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse polling period: %w", err)
		}

		sourceConfig.PollingPeriod = pollingPeriod
	}

	// parse bufferSize if it's not empty.
	if bufferSizeStr := cfg[ConfigKeyBufferSize]; bufferSizeStr != "" {
		bufferSize, err := strconv.Atoi(bufferSizeStr)
		if err != nil {
			return Config{}, fmt.Errorf("parse buffer size: %w", err)
		}

		sourceConfig.BufferSize = bufferSize
	}

	if err := validator.ValidateStruct(sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}