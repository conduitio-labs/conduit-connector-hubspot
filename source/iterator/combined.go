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

package iterator

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Combined is a combined iterator that contains both snapshot and cdc iterators.
type Combined struct {
	snapshot *Snapshot
	cdc      *CDC

	hubspotClient   *hubspot.Client
	resource        string
	bufferSize      int
	pollingPeriod   time.Duration
	extraProperties []string
}

// CombinedParams is an incoming params for the NewCombined function.
type CombinedParams struct {
	HubSpotClient   *hubspot.Client
	Resource        string
	BufferSize      int
	PollingPeriod   time.Duration
	Position        *Position
	ExtraProperties []string
	Snapshot        bool
}

// NewCombined creates new instance of the Combined.
func NewCombined(ctx context.Context, params CombinedParams) (*Combined, error) {
	combined := &Combined{
		hubspotClient:   params.HubSpotClient,
		resource:        params.Resource,
		bufferSize:      params.BufferSize,
		pollingPeriod:   params.PollingPeriod,
		extraProperties: params.ExtraProperties,
	}

	var err error
	switch position := params.Position; {
	case params.Snapshot && (position == nil || position.Mode == SnapshotPositionMode):
		combined.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			HubSpotClient:   params.HubSpotClient,
			Resource:        params.Resource,
			BufferSize:      params.BufferSize,
			PollingPeriod:   params.PollingPeriod,
			Position:        params.Position,
			ExtraProperties: params.ExtraProperties,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}

	case !params.Snapshot || (position != nil && position.Mode == CDCPositionMode):
		combined.cdc, err = NewCDC(ctx, CDCParams{
			HubSpotClient:   params.HubSpotClient,
			Resource:        params.Resource,
			BufferSize:      params.BufferSize,
			PollingPeriod:   params.PollingPeriod,
			Position:        params.Position,
			ExtraProperties: params.ExtraProperties,
		})
		if err != nil {
			return nil, fmt.Errorf("init cdc iterator: %w", err)
		}

	default:
		return nil, fmt.Errorf("invalid position mode %q", params.Position.Mode)
	}

	return combined, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
// If the underlying snapshot iterator returns false, the combined iterator will try to switch to the cdc iterator.
func (c *Combined) HasNext(ctx context.Context) (bool, error) {
	switch {
	case c.snapshot != nil:
		hasNext, err := c.snapshot.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if !hasNext {
			sdk.Logger(ctx).Debug().Msgf("switching to the CDC mode")

			if err := c.switchToCDCIterator(ctx); err != nil {
				return false, fmt.Errorf("switch to cdc iterator: %w", err)
			}

			return c.cdc.HasNext(ctx)
		}

		return hasNext, nil

	case c.cdc != nil:
		return c.cdc.HasNext(ctx)

	default:
		return false, nil
	}
}

// Next returns the next record.
func (c *Combined) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.Next(ctx)

	case c.cdc != nil:
		return c.cdc.Next(ctx)

	default:
		return sdk.Record{}, ErrNoInitializedIterator
	}
}

// switchToCDCIterator initializes the cdc iterator, and set the snapshot to nil.
func (c *Combined) switchToCDCIterator(ctx context.Context) error {
	var err error
	c.cdc, err = NewCDC(ctx, CDCParams{
		HubSpotClient: c.hubspotClient,
		Resource:      c.resource,
		BufferSize:    c.bufferSize,
		PollingPeriod: c.pollingPeriod,
		Position: &Position{
			Mode:      CDCPositionMode,
			Timestamp: &c.snapshot.initialTimestamp,
		},
		ExtraProperties: c.extraProperties,
	})
	if err != nil {
		return fmt.Errorf("init cdc iterator: %w", err)
	}

	c.snapshot.Stop()
	c.snapshot = nil

	return nil
}

// Stop stops the underlying iterators.
func (c *Combined) Stop() {
	if c.snapshot != nil {
		c.snapshot.Stop()
	}

	if c.cdc != nil {
		c.cdc.Stop()
	}
}
