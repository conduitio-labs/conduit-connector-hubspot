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

//go:generate mockgen -destination=mock/destination.go -package=mock . Writer

package destination

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-hubspot/config"
	"github.com/conduitio-labs/conduit-connector-hubspot/destination/writer"
	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hashicorp/go-retryablehttp"
)

// Writer is a writer interface needed for the [Destination].
type Writer interface {
	Write(ctx context.Context, record sdk.Record) error
}

// Destination is a HubSpot destination plugin.
type Destination struct {
	sdk.UnimplementedDestination

	config config.Config
	writer Writer
}

// NewDestination creates a new instance of the [Destination].
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named [sdk.Parameter] that describe how to configure the [Destination].
func (d *Destination) Parameters() map[string]sdk.Parameter {
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
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(_ context.Context, cfg map[string]string) (err error) {
	d.config, err = config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse destination config: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to write records.
func (d *Destination) Open(ctx context.Context) error {
	retryableHTTPClient := retryablehttp.NewClient()
	retryableHTTPClient.RetryMax = d.config.MaxRetries
	retryableHTTPClient.Logger = sdk.Logger(ctx)

	hubspotClient := hubspot.NewClient(d.config.AccessToken, retryableHTTPClient.StandardClient())

	d.writer = writer.NewWriter(writer.Params{
		HubSpotClient: hubspotClient,
		Resource:      d.config.Resource,
	})

	return nil
}

// Write needs to be overridden in the actual implementation.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		if err := d.writer.Write(ctx, record); err != nil {
			return i, fmt.Errorf("write record: %w", err)
		}
	}

	return len(records), nil
}

// Teardown does nothing.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msg("got teardown")

	return nil
}
