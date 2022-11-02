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

package hubspot

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/conduitio-labs/conduit-connector-hubspot/config"
	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	"github.com/conduitio-labs/conduit-connector-hubspot/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"
)

// testResource is a test resource that we use for integration tests.
const testResource = "cms.blogs.authors"

// acceptanceTestTimeout is a timeout used for both read and write.
const acceptanceTestTimeout = time.Second * 20

// The list of HubSpot field names that are used in acceptance tests.
const (
	testIDFieldName       = "id"
	testEmailFieldName    = "email"
	testFullNameFieldName = "fullName"
)

// testAccessToken holds a value of HUBSPOT_ACCESS_TOKEN which is required for the acceptance tests.
var testAccessToken = os.Getenv("HUBSPOT_ACCESS_TOKEN")

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	hubspotClient *hubspot.Client
}

// GenerateRecord overides the [sdk.ConfigurableAcceptanceTestDriver] [GenerateRecord] method.
func (d driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	t.Helper()

	return sdk.Record{
		Operation: operation,
		Payload: sdk.Change{
			After: sdk.StructuredData{
				testFullNameFieldName: gofakeit.Name(),
				testEmailFieldName:    gofakeit.Email(),
			},
		},
	}
}

// ReadFromDestination overrides the [sdk.ConfigurableAcceptanceTestDriver] [ReadFromDestination] method.
// It removes some redundant fields that are unknown when we insert data.
func (d driver) ReadFromDestination(t *testing.T, records []sdk.Record) []sdk.Record {
	t.Helper()

	newRecords := d.ConfigurableAcceptanceTestDriver.ReadFromDestination(t, records)

	out := make([]sdk.Record, len(newRecords))

	for i, newRecord := range newRecords {
		var newRecordStructuredPayload sdk.StructuredData
		if err := json.Unmarshal(newRecord.Payload.After.Bytes(), &newRecordStructuredPayload); err != nil {
			// this shouldn't happen
			panic(err)
		}

		out[i] = sdk.Record{
			Operation: newRecord.Operation,
			Position:  newRecord.Position,
			Payload: sdk.Change{
				After: sdk.StructuredData{
					testIDFieldName:       newRecordStructuredPayload[testIDFieldName],
					testFullNameFieldName: newRecordStructuredPayload[testFullNameFieldName],
					testEmailFieldName:    newRecordStructuredPayload[testEmailFieldName],
				},
			},
		}
	}

	return records
}

// WriteToSource overrides the [sdk.ConfigurableAcceptanceTestDriver] [WriteToSource] method.
// It takes items from HubSpot and returns them as [sdk.Record]s, because Source returns fields
// that are unknown when we insert them.
func (d driver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	t.Helper()

	newRecords := d.ConfigurableAcceptanceTestDriver.WriteToSource(t, records)

	listResp, err := d.hubspotClient.List(context.Background(), testResource, nil)
	if err != nil {
		t.Errorf("list items: %v", err)
	}

	// create a map that holds emails and coresponding list response results
	listRespMap := make(map[string]hubspot.ListResponseResult)

	for _, listRespResult := range listResp.Results {
		listRespResultName, ok := listRespResult[testEmailFieldName].(string)
		if !ok {
			t.Errorf("list resp result email is not a string or doesn't exist")
		}

		listRespMap[listRespResultName] = listRespResult
	}

	// fill records payload with the newly created HubSpot items properties
	for i := range newRecords {
		recordPayloadAfter, ok := newRecords[i].Payload.After.(sdk.StructuredData)
		if !ok {
			t.Errorf("record's payload after is not structure")
		}

		recordPayloadAfterEmail, ok := recordPayloadAfter[testEmailFieldName].(string)
		if !ok {
			t.Errorf("record's payload after email is not a string or doesn't exist")
		}

		listRespResult, ok := listRespMap[recordPayloadAfterEmail]
		if !ok {
			t.Errorf("can't find a list resp result by email")
		}

		newRecords[i].Payload.After = sdk.StructuredData(listRespResult)
	}

	return newRecords
}

//nolint:paralleltest // we don't need the paralleltest here
func TestAcceptance(t *testing.T) {
	if testAccessToken == "" {
		t.Skip("HUBSPOT_ACCESS_TOKEN env var must be set")
	}

	cfg := map[string]string{
		config.KeyAccessToken:         testAccessToken,
		config.KeyResource:            testResource,
		source.ConfigKeyPollingPeriod: "1s",
	}

	sdk.AcceptanceTest(t, driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				GoleakOptions: []goleak.Option{
					// these leaks mainly come from the go-retryablehttp
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				},
				AfterTest:    afterTest,
				ReadTimeout:  acceptanceTestTimeout,
				WriteTimeout: acceptanceTestTimeout,
			},
		},
		hubspotClient: hubspot.NewClient(testAccessToken, &http.Client{
			Timeout: acceptanceTestTimeout,
		}),
	})
}

// afterTest is a test helper that deletes all resource items after each test.
func afterTest(t *testing.T) {
	t.Helper()

	hubspotClient := hubspot.NewClient(testAccessToken, &http.Client{
		Timeout: acceptanceTestTimeout,
	})

	listResp, err := hubspotClient.List(context.Background(), testResource, nil)
	if err != nil {
		t.Errorf("hubspot list: %v", err)
	}

	for _, item := range listResp.Results {
		itemID, ok := item[testIDFieldName].(string)
		if !ok {
			t.Errorf("item is not a string")
		}

		if err := hubspotClient.Delete(context.Background(), testResource, itemID); err != nil {
			t.Errorf("hubspot delete: %v", err)
		}
	}
}
