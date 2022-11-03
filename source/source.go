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
	"context"
	"errors"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-hubspot/config"
	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	"github.com/conduitio-labs/conduit-connector-hubspot/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hashicorp/go-retryablehttp"
)

// Iterator defines an Iterator interface needed for the [Source].
type Iterator interface {
	HasNext(ctx context.Context) (bool, error)
	Next(ctx context.Context) (sdk.Record, error)
	Stop()
}

// Source is a HubSpot source plugin.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
}

// NewSource creates a new instance of the [Source].
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named [sdk.Parameter] that describe how to configure the [Source].
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyAccessToken: {
			Default:     "",
			Required:    true,
			Description: "The private app’s access token for accessing the HubSpot API.",
		},
		config.KeyResource: {
			Default:     "",
			Required:    true,
			Description: "The name of a HubSpot resource the connector will work with.",
		},
		config.KeyMaxRetries: {
			Default:  "4",
			Required: false,
			Description: "The number of HubSpot API request retries " +
				"that will be tried before giving up if a request fails.",
		},
		ConfigKeyPollingPeriod: {
			Default:     "5s",
			Required:    false,
			Description: "The duration defines a period of polling new items if CDC is not available for a resource.",
		},
		ConfigKeyBufferSize: {
			Default:  "100",
			Required: false,
			Description: "The buffer size for consumed items. " +
				"It will also be used as a limit when retrieving items from the HubSpot API.",
		},
		ConfigKeyExtraProperties: {
			Default:  "",
			Required: false,
			Description: "The list of HubSpot resource properties to include in addition to the default. " +
				"If any of the specified properties are not present on the requested HubSpot resource, " +
				"they will be ignored. Only CRM resources support this.",
		},
	}
}

// Configure parses and initializes the config.
func (s *Source) Configure(ctx context.Context, cfg map[string]string) (err error) {
	s.config, err = ParseConfig(cfg)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to read records.
func (s *Source) Open(ctx context.Context, sdkPosition sdk.Position) error {
	retryableHTTPClient := retryablehttp.NewClient()
	retryableHTTPClient.RetryMax = s.config.MaxRetries
	retryableHTTPClient.Logger = sdk.Logger(ctx)

	hubspotClient := hubspot.NewClient(s.config.AccessToken, retryableHTTPClient.StandardClient())

	position, err := iterator.ParsePosition(sdkPosition)
	if err != nil && !errors.Is(err, iterator.ErrEmptyPosition) {
		return fmt.Errorf("parse position: %w", err)
	}

	s.iterator, err = iterator.NewCombined(ctx, iterator.CombinedParams{
		HubSpotClient:   hubspotClient,
		Resource:        s.config.Resource,
		BufferSize:      s.config.BufferSize,
		PollingPeriod:   s.config.PollingPeriod,
		Position:        position,
		ExtraProperties: s.config.ExtraProperties,
	})
	if err != nil {
		return fmt.Errorf("initialize combined iterator: %w", err)
	}

	return nil
}

// Read fetches a new record from an iterator.
// If there's no record the method will return the [sdk.ErrBackoffRetry].
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get next record: %w", err)
	}

	return record, nil
}

// Ack does nothing. We don't need acks for the Snapshot or CDC iterators.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}

// Teardown does nothing.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		s.iterator.Stop()
	}

	return nil
}
