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

// Package config consists of a common config definition.
package config

import (
	"fmt"

	"github.com/conduitio-labs/conduit-connector-hubspot/validator"
)

const (
	// KeyAccessToken is a config name for an access token.
	KeyAccessToken = "accessToken"
	// KeyResource is a config name for a resource.
	KeyResource = "resource"
)

// Config contains configurable values
// shared between source and destination.
type Config struct {
	// AccessToken is a private app's access token for accessing the HubSpot API.
	AccessToken string `key:"accessToken" validate:"required"`
	// Resource defines a HubSpot resource that the connector will work with.
	Resource string `key:"resource" validate:"required"`
}

// Parse seeks to parse a provided map[string]string into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		AccessToken: cfg[KeyAccessToken],
		Resource:    cfg[KeyResource],
	}

	if err := validator.ValidateStruct(config); err != nil {
		return Config{}, fmt.Errorf("validate common config: %w", err)
	}

	return config, nil
}
