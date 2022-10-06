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

package writer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Writer implements a writer logic for HubSpot destination.
type Writer struct {
	hubspotClient *hubspot.Client
	resource      string
}

// Params holds incoming params for the [NewWriter] function.
type Params struct {
	HubSpotClient *hubspot.Client
	Resource      string
}

// NewWriter creates a new instance of the [Writer].
func NewWriter(params Params) *Writer {
	return &Writer{
		hubspotClient: params.HubSpotClient,
		resource:      params.Resource,
	}
}

// Write routes a provided record to different methods based on its [sdk.Operation].
//   - If the operation is [sdk.OperationCreate] or [sdk.OperationSnapshot]
//     the record will be plainly inserted;
//   - If the operation is [sdk.OperationUpdate]
//     the method will try to update an existing record using the record payload;
//   - If the operation is [sdk.OperationDelete]
//     the method will try to delete an existing record using the record key.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	err := sdk.Util.Destination.Route(ctx, record,
		w.insert,
		w.update,
		w.delete,
		w.insert,
	)
	if err != nil {
		return fmt.Errorf("route record: %w", err)
	}

	return nil
}

// insert inserts a record to a destination.
func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
	}

	if err := w.hubspotClient.Create(ctx, w.resource, payload); err != nil {
		return fmt.Errorf("create %q item: %w", w.resource, err)
	}

	return nil
}

// update updates a record in a destination.
func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyValue, err := w.getKeyValue(key)
	if err != nil {
		return fmt.Errorf("get key's value: %w", err)
	}

	if keyValue == "" {
		return ErrEmptyKey
	}

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
	}

	if err := w.hubspotClient.Update(ctx, w.resource, keyValue, payload); err != nil {
		return fmt.Errorf("update %q item: %w", w.resource, err)
	}

	return nil
}

// delete deletes a record from a destination.
func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyValue, err := w.getKeyValue(key)
	if err != nil {
		return fmt.Errorf("get key's value: %w", err)
	}

	if keyValue == "" {
		return ErrEmptyKey
	}

	if err := w.hubspotClient.Delete(ctx, w.resource, keyValue); err != nil {
		return fmt.Errorf("delete %q item: %w", w.resource, err)
	}

	return nil
}

// structurizeData tries to convert [sdk.Data] to [sdk.StructuredData].
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// getKeyValue returns either the first key within the Key structured data
// or the default key configured value for key.
func (w *Writer) getKeyValue(key sdk.StructuredData) (string, error) {
	if len(key) > 1 {
		return "", ErrCompositeKeysNotSupported
	}

	for _, val := range key {
		if valStr, ok := val.(string); ok {
			return valStr, nil
		}

		return "", ErrKeyIsNotAString
	}

	return "", nil
}
