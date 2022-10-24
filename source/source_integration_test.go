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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/conduitio-labs/conduit-connector-hubspot/config"
	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// testResource is a test resource that we use for integration tests.
const testResource = "crm.contacts"

var (
	// testAccessToken will be used if a provided access token is empty,
	// if both a provided access token and this value are empty an integration test will be skipped.
	testAccessToken = os.Getenv("HUBSPOT_ACCESS_TOKEN")
	// testHTTPClientTimeout is a HTTP timeout for test HTTP client.
	testHTTPClientTimeout = 5 * time.Second

	// consts for retries.
	maxCheckRetries   = 5
	checkRetryTimeout = time.Second * 2
)

func TestSource_Read_successSnapshot(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// create a test contact with a random properties
	testContact, err := createTestContact(ctx, hubspotClient)
	is.NoErr(err)

	// open the source
	err = source.Open(ctx, nil)
	is.NoErr(err)

	// read a record
	record, err := source.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	testContactID := compareTestContactWithRecord(t, is, testContact, record)
	err = hubspotClient.Delete(ctx, testResource, testContactID)
	is.NoErr(err)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_successSnapshotContinue(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// create a couple of test contacts with a random properties
	firstTestContact, err := createTestContact(ctx, hubspotClient)
	is.NoErr(err)

	secondTestContact, err := createTestContact(ctx, hubspotClient)
	is.NoErr(err)

	// open the source
	err = source.Open(ctx, nil)
	is.NoErr(err)

	// read a record
	record, err := source.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	firstTestContactID := compareTestContactWithRecord(t, is, firstTestContact, record)
	err = hubspotClient.Delete(ctx, testResource, firstTestContactID)
	is.NoErr(err)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// reopen the source from the last position
	is.NoErr(source.Open(ctx, record.Position))

	// read a record
	record, err = source.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	secondTestContactID := compareTestContactWithRecord(t, is, secondTestContact, record)
	err = hubspotClient.Delete(ctx, testResource, secondTestContactID)
	is.NoErr(err)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_successCDC(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// create a test contact with a random properties
	firstTestContact, err := createTestContact(ctx, hubspotClient)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	// read a record
	record, err := source.Read(ctx)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	firstTestContactID := compareTestContactWithRecord(t, is, firstTestContact, record)
	err = hubspotClient.Delete(ctx, testResource, firstTestContactID)
	is.NoErr(err)

	// we expect backoff retry and switch to CDC mode here
	_, err = source.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// create a test contact with a random properties
	secondTestContact, err := createTestContact(ctx, hubspotClient)
	is.NoErr(err)

	record, err = readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationCreate)

	secondTestContactID := compareTestContactWithRecord(t, is, secondTestContact, record)

	// update the second test contact
	updatedTestContact, err := updateTestContact(ctx, hubspotClient, secondTestContactID)
	is.NoErr(err)

	record, err = readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationUpdate)

	compareTestContactWithRecord(t, is, updatedTestContact, record)
	err = hubspotClient.Delete(ctx, testResource, secondTestContactID)
	is.NoErr(err)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_failBackoffRetry(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	ctx := context.Background()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	// since we didn't insert anything we expect that the source
	// will return the [sdk.ErrBackoffRetry] error
	_, err = source.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)
}

func TestSource_Read_failInvalidToken(t *testing.T) {
	is := is.New(t)

	// prepare a config with invalid access token, configure and open a new source
	config := prepareConfig(t, "invalid")

	source := NewSource()

	ctx := context.Background()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// we expect to get a 401 error because the access token we provided is invalid
	err = source.Open(ctx, nil)
	is.True(err != nil)
}

func prepareConfig(t *testing.T, accessToken string) map[string]string {
	t.Helper()

	if accessToken == "" {
		if testAccessToken == "" {
			t.Skip("HUBSPOT_ACCESS_TOKEN env var must be set")
		}

		accessToken = testAccessToken
	}

	return map[string]string{
		config.KeyAccessToken: accessToken,
		config.KeyResource:    testResource,
	}
}

// createTestContact writes a random contact and returns it.
func createTestContact(ctx context.Context, hubspotClient *hubspot.Client) (map[string]any, error) {
	testContact := map[string]any{
		"properties": map[string]any{
			"email":     gofakeit.Email(),
			"firstname": gofakeit.FirstName(),
			"lastname":  gofakeit.LastName(),
		},
	}

	err := hubspotClient.Create(ctx, testResource, testContact)
	if err != nil {
		return nil, fmt.Errorf("create contact: %w", err)
	}

	return testContact, nil
}

// updateTestContact updates a random contact's email, firstname and lastname and returns it.
func updateTestContact(
	ctx context.Context,
	hubspotClient *hubspot.Client,
	itemID string,
) (map[string]any, error) {
	updatedTestContact := map[string]any{
		"properties": map[string]any{
			"email":     gofakeit.Email(),
			"firstname": gofakeit.FirstName(),
			"lastname":  gofakeit.LastName(),
		},
	}

	err := hubspotClient.Update(ctx, testResource, itemID, updatedTestContact)
	if err != nil {
		return nil, fmt.Errorf("update contact: %w", err)
	}

	return updatedTestContact, nil
}

// readWithRetry tries to read a record from a source with retry.
func readWithRetry(ctx context.Context, source sdk.Source) (sdk.Record, error) {
	ticker := time.NewTicker(checkRetryTimeout)

	for i := 0; i < maxCheckRetries; i++ {
		select {
		case <-ctx.Done():
			return sdk.Record{}, fmt.Errorf("context canceled: %w", ctx.Err())

		case <-ticker.C:
			record, err := source.Read(ctx)
			if err != nil {
				if errors.Is(err, sdk.ErrBackoffRetry) {
					continue
				}

				return sdk.Record{}, fmt.Errorf("source read: %w", err)
			}

			return record, nil
		}
	}

	return sdk.Record{}, sdk.ErrBackoffRetry
}

// compareTestContactWithRecord parses and compares a testContact
// represented as a map[string]any with a record's payload.
// The method returns the test contact's id as a string.
func compareTestContactWithRecord(
	t *testing.T,
	is *is.I,
	testContact map[string]any,
	record sdk.Record,
) string {
	t.Helper()

	// unmarshal the item that was read by the source
	var snapshotContactItem map[string]any
	err := json.Unmarshal(record.Payload.After.Bytes(), &snapshotContactItem)
	is.NoErr(err)

	snapshotContactItemProperties, ok := snapshotContactItem["properties"].(map[string]any)
	is.True(ok)

	// take the object's id to delete the object when the test is completed
	hsObjectID, ok := snapshotContactItemProperties["hs_object_id"].(string)
	is.True(ok)

	// check that the record's key contains the item's hs_object_id
	hsObjectIDInt, err := strconv.Atoi(hsObjectID)
	is.NoErr(err)

	parsedKey := make(sdk.StructuredData)
	err = json.Unmarshal(record.Key.Bytes(), &parsedKey)
	is.NoErr(err)

	// convert to float64 because int is converted to float64 when unmarshaling json into map[string]any
	parsedKeyID, ok := parsedKey[hubspot.ResultsFieldID].(float64)
	is.True(ok)

	is.Equal(hsObjectIDInt, int(parsedKeyID))

	// remove fields that we don't know when creating a new item
	delete(snapshotContactItemProperties, "hs_object_id")
	delete(snapshotContactItemProperties, "createdate")
	delete(snapshotContactItemProperties, "lastmodifieddate")

	is.Equal(snapshotContactItem["properties"], testContact["properties"])

	return strconv.Itoa(hsObjectIDInt)
}
