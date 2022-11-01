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

package destination

import (
	"context"
	"errors"
	"net/http"
	"os"
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
)

func TestDestination_Write_successCreate(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new destination
	config := prepareConfig(t, "")

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// create a test sdk.Record
	testCreateRecord, testCreateRecordProperties := createTestCreateRecord()

	// write the test record and check if the returned err is nil and n is equal to one
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// list test resource items
	listResp, err := hubspotClient.List(ctx, testResource, nil)
	is.NoErr(err)

	// check that there's exactly one item in HubSpot
	is.Equal(len(listResp.Results), 1)

	t.Cleanup(func() {
		itemID, ok := listResp.Results[0]["id"].(string)
		is.True(ok)

		err = hubspotClient.Delete(context.Background(), testResource, itemID)
		is.NoErr(err)
	})

	// check that the item's properties are equal to the test record properties
	actualProperties, ok := listResp.Results[0]["properties"].(map[string]any)
	is.True(ok)
	is.Equal(actualProperties["firstname"], testCreateRecordProperties["firstname"])
	is.Equal(actualProperties["lastname"], testCreateRecordProperties["lastname"])

	// teardown the destination
	cancel()
	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_successCreateUpdate(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new destination
	config := prepareConfig(t, "")

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// create a test sdk.Record
	testCreateRecord, testCreateRecordProperties := createTestCreateRecord()

	// write the test record and check if the returned err is nil and n is equal to one
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// list test resource items
	listResp, err := hubspotClient.List(ctx, testResource, nil)
	is.NoErr(err)

	// check that there's exactly one item in HubSpot
	is.Equal(len(listResp.Results), 1)

	itemID, ok := listResp.Results[0]["id"].(string)
	is.True(ok)

	t.Cleanup(func() {
		err = hubspotClient.Delete(context.Background(), testResource, itemID)
		is.NoErr(err)
	})

	// check that the item's properties are equal to the test record properties
	actualProperties, ok := listResp.Results[0]["properties"].(map[string]any)
	is.True(ok)
	is.Equal(actualProperties["firstname"], testCreateRecordProperties["firstname"])
	is.Equal(actualProperties["lastname"], testCreateRecordProperties["lastname"])

	// create a test record with update operation
	testUpdateRecord, testUpdateRecordProperties := createTestUpdateRecord(itemID)

	n, err = destination.Write(ctx, []sdk.Record{testUpdateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// list test resource items
	listResp, err = hubspotClient.List(ctx, testResource, nil)
	is.NoErr(err)

	// check that there's exactly one item in HubSpot
	is.Equal(len(listResp.Results), 1)

	// check that the item's properties are equal to the updated test record properties
	actualProperties, ok = listResp.Results[0]["properties"].(map[string]any)
	is.True(ok)
	is.Equal(actualProperties["firstname"], testUpdateRecordProperties["firstname"])
	is.Equal(actualProperties["lastname"], testUpdateRecordProperties["lastname"])

	// teardown the destination
	cancel()
	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_successCreateDelete(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new destination
	config := prepareConfig(t, "")

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// create a test sdk.Record
	testCreateRecord, testCreateRecordProperties := createTestCreateRecord()

	// write the test record and check if the returned err is nil and n is equal to one
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// create a test hubspot client
	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: testHTTPClientTimeout,
	})

	// list test resource items
	listResp, err := hubspotClient.List(ctx, testResource, nil)
	is.NoErr(err)

	// check that there's exactly one item in HubSpot
	is.Equal(len(listResp.Results), 1)

	itemID, ok := listResp.Results[0]["id"].(string)
	is.True(ok)

	t.Cleanup(func() {
		err = hubspotClient.Delete(context.Background(), testResource, itemID)
		is.NoErr(err)
	})

	// check that the item's properties are equal to the test record properties
	actualProperties, ok := listResp.Results[0]["properties"].(map[string]any)
	is.True(ok)
	is.Equal(actualProperties["firstname"], testCreateRecordProperties["firstname"])
	is.Equal(actualProperties["lastname"], testCreateRecordProperties["lastname"])

	// create a test record with delete operation
	testDeleteRecord := createTestDeleteRecord(itemID)

	n, err = destination.Write(ctx, []sdk.Record{testDeleteRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// list test resource items
	listResp, err = hubspotClient.List(ctx, testResource, nil)
	is.NoErr(err)

	// check that there's no items in HubSpot
	is.Equal(len(listResp.Results), 0)

	// teardown the destination
	cancel()
	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failInvalidToken(t *testing.T) {
	is := is.New(t)

	// prepare a config with invalid access token, configure and open a new destination
	config := prepareConfig(t, "invalid")

	destination := NewDestination()

	ctx := context.Background()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	testCreateRecord, _ := createTestCreateRecord()

	// we expect to get a 401 error because the access token we provided is invalid
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.True(err != nil)
	is.Equal(n, 0)

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

// createTestRecord creates a test record with [sdk.OperationCreate].
func createTestCreateRecord() (sdk.Record, map[string]any) {
	var (
		id         = gofakeit.Int32()
		properties = map[string]any{
			"firstname": gofakeit.FirstName(),
			"lastname":  gofakeit.LastName(),
		}
	)

	return sdk.Util.Source.NewRecordCreate(
		nil, nil,
		sdk.StructuredData{
			// HubSpot doesn't allow to specify a custom identifier, so it'll be ignored.
			"id": id,
		},
		sdk.StructuredData{
			"id":         id,
			"archived":   false,
			"createdAt":  time.Now().Format(time.RFC3339),
			"updatedAt":  time.Now().Format(time.RFC3339),
			"properties": properties,
		},
	), properties
}

// createTestUpdateRecord creates a test record with [sdk.OperationUpdate].
func createTestUpdateRecord(id string) (sdk.Record, map[string]any) {
	properties := map[string]any{
		"firstname": gofakeit.FirstName(),
		"lastname":  gofakeit.LastName(),
	}

	return sdk.Util.Source.NewRecordUpdate(
		nil, nil,
		sdk.StructuredData{
			"id": id,
		},
		nil,
		sdk.StructuredData{
			"id":         id,
			"archived":   false,
			"createdAt":  time.Now().Format(time.RFC3339),
			"updatedAt":  time.Now().Format(time.RFC3339),
			"properties": properties,
		},
	), properties
}

// createTestDeleteRecord creates a test record with [sdk.OperationDelete].
func createTestDeleteRecord(id string) sdk.Record {
	return sdk.Util.Source.NewRecordDelete(
		nil, nil, sdk.StructuredData{"id": id},
	)
}
