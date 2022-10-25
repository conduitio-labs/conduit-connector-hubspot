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

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
)

// CDC is an implementation of a CDC iterator for the HubSpot API.
type CDC struct {
	hubspotClient *hubspot.Client
	resource      string
	bufferSize    int
	pollingPeriod time.Duration
	records       chan sdk.Record
	errC          chan error
	stopC         chan struct{}
	position      *Position
}

// CDCParams is an incoming params for the [NewCDC] function.
type CDCParams struct {
	HubSpotClient *hubspot.Client
	Resource      string
	BufferSize    int
	PollingPeriod time.Duration
	Position      *Position
}

// NewCDC creates a new instance of the [CDC].
func NewCDC(ctx context.Context, params CDCParams) (*CDC, error) {
	cdc := &CDC{
		hubspotClient: params.HubSpotClient,
		resource:      params.Resource,
		bufferSize:    params.BufferSize,
		pollingPeriod: params.PollingPeriod,
		records:       make(chan sdk.Record, params.BufferSize),
		errC:          make(chan error, 1),
		stopC:         make(chan struct{}, 1),
		position:      params.Position,
	}

	if cdc.position == nil || cdc.position.Timestamp == nil {
		now := time.Now().UTC()
		cdc.position = &Position{
			Mode:      CDCPositionMode,
			Timestamp: &now,
		}
	}

	if err := cdc.loadRecords(ctx); err != nil {
		return nil, fmt.Errorf("initial load record: %w", err)
	}

	go cdc.poll(ctx)

	return cdc, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (c *CDC) HasNext(ctx context.Context) (bool, error) {
	return len(c.records) > 0, nil
}

// Next returns the next record.
func (c *CDC) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("context cancelled: %w", ctx.Err())

	case err := <-c.errC:
		return sdk.Record{}, fmt.Errorf("async error: %w", err)

	case record := <-c.records:
		return record, nil
	}
}

// poll polls items at the specified time intervals.
func (c *CDC) poll(ctx context.Context) {
	ticker := time.NewTicker(c.pollingPeriod)

	for {
		select {
		case <-ctx.Done():
			return

		case <-c.stopC:
			return

		case <-ticker.C:
			if err := c.loadRecords(ctx); err != nil {
				c.errC <- fmt.Errorf("load records: %w", err)
			}
		}
	}
}

// loadRecords loads HubSpot items using timestamp-based filtering and search endpoints.
// The method tries to retrieve items filtering them by updatedAfter or lastmodifieddate.
func (c *CDC) loadRecords(ctx context.Context) error {
	// add a millisecond here in order to skip the processed item
	*c.position.Timestamp = c.position.Timestamp.Add(time.Millisecond)

	if err := c.processUpdatedItems(ctx, *c.position.Timestamp); err != nil {
		return fmt.Errorf("process updated items: %w", err)
	}

	return nil
}

// processUpdatedItems retrieves items that were updated after the provided timestamp.
func (c *CDC) processUpdatedItems(ctx context.Context, updatedAfter time.Time) error {
	if timestampResource, ok := hubspot.TimestampResources[c.resource]; ok {
		return c.fetchTimestampBasedItems(ctx, timestampResource, updatedAfter)
	}

	if searchResource, ok := hubspot.SearchResources[c.resource]; ok {
		return c.fetchSearchBasedItems(ctx, searchResource, updatedAfter)
	}

	return nil
}

// fetchTimestampBasedItems fetches items by filtering them by updatedAt field.
func (c *CDC) fetchTimestampBasedItems(
	ctx context.Context,
	resource hubspot.TimestampResource,
	updatedAfter time.Time,
) error {
	listOpts := &hubspot.ListOptions{
		Limit:        c.bufferSize,
		UpdatedAfter: &updatedAfter,
		Sort:         hubspot.UpdatedAtListSortKey,
		Archived:     true,
	}

	listResp, err := c.hubspotClient.List(ctx, c.resource, listOpts)
	if err != nil {
		return fmt.Errorf("list items: %w", err)
	}

	for _, item := range listResp.Results {
		err = c.routeItem(item,
			resource.CreatedAtFieldName,
			resource.UpdatedAtFieldName,
			resource.DeletedAtFieldName,
			updatedAfter)
		if err != nil {
			return fmt.Errorf("route timestamp based item: %w", err)
		}
	}

	return nil
}

// fetchSearchBasedItems fetches items using search endpoint.
func (c *CDC) fetchSearchBasedItems(
	ctx context.Context,
	resource hubspot.SearchResource,
	updatedAfter time.Time,
) error {
	listResp, err := c.hubspotClient.SearchByUpdatedAfter(ctx, c.resource, updatedAfter, c.bufferSize)
	if err != nil {
		return fmt.Errorf("list items: %w", err)
	}

	for _, item := range listResp.Results {
		err = c.routeItem(item, resource.CreatedAtFieldName, resource.UpdatedAtFieldName, "", updatedAfter)
		if err != nil {
			return fmt.Errorf("route search based item: %w", err)
		}
	}

	return nil
}

// routeItem retrives createdAt and updatedAt fields from the item, compares them
// and based on the result of the comparison decides to send a Create or Update sdk.Record.
func (c *CDC) routeItem(
	item hubspot.ListResponseResult,
	createdAtFieldName,
	updatedAtFieldName,
	deletedAtFieldName string,
	updatedAfter time.Time,
) error {
	itemCreatedAt, err := item.GetTimeField(createdAtFieldName)
	if err != nil {
		return fmt.Errorf("get item's creation date: %w", err)
	}

	itemUpdatedAt, err := item.GetTimeField(updatedAtFieldName)
	if err != nil {
		return fmt.Errorf("get item's update date: %w", err)
	}

	metadata := make(sdk.Metadata)
	metadata.SetCreatedAt(itemCreatedAt)

	c.position = &Position{
		Mode: CDCPositionMode,
		// set the timestamp to the item's updatedAt
		// as we sort items by their updatedAt values.
		Timestamp: &itemUpdatedAt,
	}

	sdkPosition, err := c.position.MarshalSDKPosition()
	if err != nil {
		return fmt.Errorf("marshal sdk position: %w", err)
	}

	var itemDeletedAt time.Time
	if deletedAtFieldName != "" {
		itemDeletedAt, err = item.GetTimeField(deletedAtFieldName)
		if err != nil {
			return fmt.Errorf("get item's update date: %w", err)
		}
	}

	c.records <- c.GetRecord(item,
		itemCreatedAt,
		itemUpdatedAt,
		itemDeletedAt,
		updatedAfter,
		sdkPosition,
		metadata)

	return nil
}

func (c *CDC) GetRecord(item hubspot.ListResponseResult,
	itemCreatedAt,
	itemUpdatedAt,
	itemDeletedAt,
	updatedAfter time.Time,
	sdkPosition sdk.Position,
	metadata sdk.Metadata,
) sdk.Record {
	if itemDeletedAt != (time.Time{}) {
		if itemDeletedAt == itemUpdatedAt && itemDeletedAt.Unix() > 0 {
			return sdk.Util.Source.NewRecordDelete(sdkPosition, metadata,
				sdk.StructuredData{hubspot.ResultsFieldID: item[hubspot.ResultsFieldID]},
			)
		}
	}

	// if the item's createdAt is after the timestamp after which we're searching items
	// we consider the item's operation to be sdk.OperationCreate.
	if itemCreatedAt.After(updatedAfter) {
		return sdk.Util.Source.NewRecordCreate(sdkPosition, metadata,
			sdk.StructuredData{hubspot.ResultsFieldID: item[hubspot.ResultsFieldID]},
			sdk.StructuredData(item),
		)
	}

	return sdk.Util.Source.NewRecordUpdate(sdkPosition, metadata,
		sdk.StructuredData{hubspot.ResultsFieldID: item[hubspot.ResultsFieldID]},
		nil, sdk.StructuredData(item),
	)
}

// Stop stops the iterator.
func (c *CDC) Stop() {
	c.stopC <- struct{}{}
}
