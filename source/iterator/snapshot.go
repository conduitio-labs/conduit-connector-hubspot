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
	"strconv"
	"time"

	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Snapshot is an implementation of a Snapshot iterator for the HubSpot API.
type Snapshot struct {
	hubspotClient *hubspot.Client
	resource      string
	bufferSize    int
	pollingPeriod time.Duration
	records       chan sdk.Record
	errC          chan error
	stopC         chan struct{}
	position      *Position
}

// SnapshotParams is an incoming params for the [NewSnapshot] function.
type SnapshotParams struct {
	HubSpotClient *hubspot.Client
	Resource      string
	BufferSize    int
	PollingPeriod time.Duration
	Position      *Position
}

// NewSnapshot creates a new instance of the [Snapshot].
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	snapshot := &Snapshot{
		hubspotClient: params.HubSpotClient,
		resource:      params.Resource,
		bufferSize:    params.BufferSize,
		pollingPeriod: params.PollingPeriod,
		records:       make(chan sdk.Record, params.BufferSize),
		errC:          make(chan error, 1),
		stopC:         make(chan struct{}, 1),
		position:      params.Position,
	}

	if err := snapshot.loadRecords(ctx); err != nil {
		return nil, fmt.Errorf("initial load record: %w", err)
	}

	go snapshot.poll(ctx)

	return snapshot, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (s *Snapshot) HasNext(ctx context.Context) (bool, error) {
	return len(s.records) > 0, nil
}

// Next returns the next record.
func (s *Snapshot) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("context cancelled: %w", ctx.Err())

	case err := <-s.errC:
		return sdk.Record{}, fmt.Errorf("async error: %w", err)

	case record := <-s.records:
		return record, nil
	}
}

// poll polls items at the specified time intervals.
func (s *Snapshot) poll(ctx context.Context) {
	ticker := time.NewTicker(s.pollingPeriod)

	for {
		select {
		case <-ctx.Done():
			return

		case <-s.stopC:
			return

		case <-ticker.C:
			if err := s.loadRecords(ctx); err != nil {
				s.errC <- fmt.Errorf("load records: %w", err)
			}
		}
	}
}

// loadRecords retrieves a new list of the iterator's resource items.
func (s *Snapshot) loadRecords(ctx context.Context) error {
	listOpts := &hubspot.ListOptions{
		Limit: s.bufferSize,
	}

	if s.position != nil {
		// add one here in order to skip
		// this particular item and start from the next one.
		listOpts.After = strconv.Itoa(s.position.ItemID + 1)
	}

	listResponse, err := s.hubspotClient.List(ctx, s.resource, listOpts)
	if err != nil {
		return fmt.Errorf("list %q items: %w", s.resource, err)
	}

	for _, item := range listResponse.Results {
		s.position, err = s.getItemPosition(item)
		if err != nil {
			return fmt.Errorf("get item's position: %w", err)
		}

		sdkPosition, err := s.position.MarshalSDKPosition()
		if err != nil {
			return fmt.Errorf("marshal sdk position: %w", err)
		}

		metadata, err := s.getItemMetadata(item)
		if err != nil {
			return fmt.Errorf("get item's metadata: %w", err)
		}

		s.records <- sdk.Util.Source.NewRecordSnapshot(
			sdkPosition, metadata,
			sdk.StructuredData{hubspot.ResultsFieldID: s.position.ItemID},
			sdk.StructuredData(item),
		)
	}

	return nil
}

// getItemPosition grabs an id field from a provided item and constructs a [Position] based on its value.
func (s *Snapshot) getItemPosition(item map[string]any) (*Position, error) {
	itemIDStr, ok := item[hubspot.ResultsFieldID].(string)
	if !ok {
		// this shouldn't happen cause HubSpot API v3 returns items with string identifiers.
		return nil, ErrItemIDIsNotAString
	}

	itemID, err := strconv.Atoi(itemIDStr)
	if err != nil {
		return nil, fmt.Errorf("convert item's id string to integer: %w", err)
	}

	return &Position{
		Mode:   SnapshotPositionMode,
		ItemID: itemID,
	}, nil
}

// getItemMetadata grabs a createdAt field from a provided item and constructs a [sdk.Metadata] based on that.
// If the createdAt field is empty the method will use the current time.
func (s *Snapshot) getItemMetadata(item map[string]any) (metadata sdk.Metadata, err error) {
	createdAt := time.Now()

	if createdAtStr, ok := item[hubspot.ResultsFieldCreatedAt].(string); ok {
		createdAt, err = time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse createdAt: %w", err)
		}
	}

	metadata = make(sdk.Metadata)
	metadata.SetCreatedAt(createdAt)

	return metadata, nil
}

// Stop stops the iterator.
func (s *Snapshot) Stop() {
	s.stopC <- struct{}{}
}
