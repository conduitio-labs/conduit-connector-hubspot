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
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-hubspot/config"
	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	"github.com/conduitio-labs/conduit-connector-hubspot/test"
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
	checkRetryTimeout = time.Second * 5
)

func TestSource_Read_successSnapshot(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// create a test sdk.Record
	trc := test.NewRecordCreator(t, testResource, true)
	testContact := trc.NewTestCreateRecord()

	// give HubSpot some time to process the HTTP request we sent
	// and create the contact
	err = waitTestContacts(ctx, hubspotClient, testContact)
	is.NoErr(err)

	// open the source
	err = source.Open(ctx, nil)
	is.NoErr(err)

	// read a record
	record, err := readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	compareTestContactWithRecord(is, testContact, record)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_successSnapshotContinue(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// create a couple of test contacts with a random properties
	trc := test.NewRecordCreator(t, testResource, true)
	firstTestContact := trc.NewTestCreateRecord()
	secondTestContact := trc.NewTestCreateRecord()

	// give HubSpot some time to process the HTTP request we sent
	// and create the contact
	err = waitTestContacts(ctx, hubspotClient, firstTestContact, secondTestContact)
	is.NoErr(err)

	// open the source
	err = source.Open(ctx, nil)
	is.NoErr(err)

	// read a record
	record, err := readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	compareTestContactWithRecord(is, firstTestContact, record)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// reopen the source from the last position
	is.NoErr(source.Open(ctx, record.Position))

	// read a record
	record, err = readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	compareTestContactWithRecord(is, secondTestContact, record)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_successCDC(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// create a test contact with a random properties
	trc := test.NewRecordCreator(t, testResource, true)
	firstTestContact := trc.NewTestCreateRecord()

	// give HubSpot some time to process the HTTP request we sent
	// and create the contact
	err = waitTestContacts(ctx, hubspotClient, firstTestContact)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	// read a record
	record, err := readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationSnapshot)

	compareTestContactWithRecord(is, firstTestContact, record)

	// we expect backoff retry and switch to CDC mode here
	_, err = source.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// create a test contact with a random properties
	secondTestContact := trc.NewTestCreateRecord()

	// give HubSpot some time to process the HTTP request we sent
	// and create the contact
	err = waitTestContacts(ctx, hubspotClient, secondTestContact)
	is.NoErr(err)

	record, err = readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationCreate)

	secondTestContactID := compareTestContactWithRecord(is, secondTestContact, record)

	// update the second test contact
	updatedTestContact := trc.NewTestUpdateRecord(secondTestContactID)

	record, err = readWithRetry(ctx, source)
	is.NoErr(err)

	is.Equal(record.Operation, sdk.OperationUpdate)

	compareTestContactWithRecord(is, updatedTestContact, record)

	cancel()
	err = source.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_failBackoffRetry(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	// prepare a config, configure and open a new source
	config := prepareConfig(t, "")

	source := NewSource()

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
	ctx := context.Background()

	// prepare a config with invalid access token, configure and open a new source
	config := prepareConfig(t, "invalid")

	source := NewSource()

	err := source.Configure(ctx, config)
	is.NoErr(err)

	// we expect to get a 401 error because the access token we provided is invalid
	err = source.Open(ctx, nil)
	is.True(err != nil)

	var unexpectedStatusCode *hubspot.UnexpectedStatusCodeError
	is.True(errors.As(err, &unexpectedStatusCode))
	is.Equal(unexpectedStatusCode.StatusCode, http.StatusUnauthorized)
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

// waitTestContacts waits until the expected count of contacts will be present
// in HubSpot.
func waitTestContacts(
	ctx context.Context,
	hubspotClient *hubspot.Client,
	wantRecs ...sdk.Record,
) error {
	ticker := time.NewTicker(checkRetryTimeout)

	filterGroups := make([]hubspot.SearchRequestFilterGroup, len(wantRecs))
	for i, wantRec := range wantRecs {
		sd := wantRec.Payload.After.(sdk.StructuredData)
		properties := sd["properties"].(map[string]any)
		filters := make([]hubspot.SearchRequestFilterGroupFilter, 0)
		for k, v := range properties {
			filters = append(filters, hubspot.SearchRequestFilterGroupFilter{
				PropertyName: k,
				Operator:     "EQ",
				Value:        v.(string),
			})
		}
		filterGroups[i].Filters = filters
	}

	req := &hubspot.SearchRequest{FilterGroups: filterGroups}

	for i := 0; i < maxCheckRetries; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled: %w", ctx.Err())

		case <-ticker.C:
			listResp, err := hubspotClient.Search(ctx, testResource, req)
			if err != nil {
				return fmt.Errorf("search contacts: %w", err)
			}

			if listResp.Total == len(wantRecs) {
				return nil
			}
		}
	}

	return fmt.Errorf("did not find records")
}

// readWithRetry tries to read a record from a source with retry.
func readWithRetry(ctx context.Context, source sdk.Source) (sdk.Record, error) {
	ticker := time.NewTicker(checkRetryTimeout)

	var record sdk.Record
	var err error
	for i := 0; i < maxCheckRetries; i++ {
		select {
		case <-ctx.Done():
			return sdk.Record{}, ctx.Err()
		case <-ticker.C:
			record, err = source.Read(ctx)
			if errors.Is(err, sdk.ErrBackoffRetry) {
				continue
			}
			return record, err
		}
	}
	return record, err
}

// compareTestContactWithRecord parses and compares a testContact
// represented as a map[string]any with a record's payload.
// The method returns the test contact's id as a string.
func compareTestContactWithRecord(
	is *is.I,
	want sdk.Record,
	got sdk.Record,
) string {
	is.Helper()

	// unmarshal the item that was read by the source
	gotPayload, ok := got.Payload.After.(sdk.StructuredData)
	is.True(ok)

	gotProperties, ok := gotPayload["properties"].(map[string]any)
	is.True(ok)

	// take the object's id to delete the object when the test is completed
	hsObjectID, ok := gotProperties["hs_object_id"].(string)
	is.True(ok)

	gotKey, ok := got.Key.(sdk.StructuredData)
	is.True(ok)

	// convert to float64 because int is converted to float64 when unmarshaling json into map[string]any
	gotKeyID, ok := gotKey[hubspot.ResultsFieldID].(string)
	is.True(ok)

	is.Equal(hsObjectID, gotKeyID)

	// remove fields that we don't know when creating a new item
	delete(gotProperties, "hs_object_id")
	delete(gotProperties, "createdate")
	delete(gotProperties, "lastmodifieddate")

	is.Equal(gotProperties, want.Payload.After.(sdk.StructuredData)["properties"])

	return hsObjectID
}
