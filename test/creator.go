// Copyright © 2023 Meroxa, Inc.
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

package test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var (
	// testAccessToken will be used if a provided access token is empty,
	// if both a provided access token and this value are empty an integration test will be skipped.
	testAccessToken = os.Getenv("HUBSPOT_ACCESS_TOKEN")
	// testHTTPClientTimeout is a HTTP timeout for test HTTP client.
	testHTTPClientTimeout = 5 * time.Second
)

type RecordCreator struct {
	is            *is.I
	resource      string
	testRecords   []opencdc.Record
	flushToServer bool
	client        *hubspot.Client
}

func NewRecordCreator(t *testing.T, resource string, flushToServer bool) *RecordCreator {
	t.Helper()
	rc := &RecordCreator{
		is:            is.New(t),
		resource:      resource,
		flushToServer: flushToServer,
		client: hubspot.NewClient(
			testAccessToken,
			&http.Client{
				Timeout: testHTTPClientTimeout,
			},
		),
	}
	t.Cleanup(rc.Cleanup)
	return rc
}

// NewTestCreateRecord creates a test record with [opencdc.OperationCreate].
func (rc *RecordCreator) NewTestCreateRecord() opencdc.Record {
	id := gofakeit.Int32()
	rec := sdk.Util.Source.NewRecordCreate(
		nil, nil,
		opencdc.StructuredData{
			// HubSpot doesn't allow to specify a custom identifier, so it'll be ignored.
			"id": id,
		},
		opencdc.StructuredData{
			"id":        id,
			"archived":  false,
			"createdAt": time.Now().Format(time.RFC3339),
			"updatedAt": time.Now().Format(time.RFC3339),
			"properties": map[string]any{
				"email":     gofakeit.Email(),
				"firstname": gofakeit.FirstName(),
				"lastname":  gofakeit.LastName(),
			},
		},
	)

	if rc.flushToServer {
		//nolint:forcetypeassert // we just created the record, we can type assert without a check
		err := rc.client.Create(context.Background(), rc.resource, rec.Payload.After.(opencdc.StructuredData))
		rc.is.NoErr(err)
	}

	rc.testRecords = append(rc.testRecords, rec)
	return rec
}

// NewTestUpdateRecord creates a test record with [opencdc.OperationUpdate].
func (rc *RecordCreator) NewTestUpdateRecord(id string) opencdc.Record {
	rec := sdk.Util.Source.NewRecordUpdate(
		nil, nil,
		opencdc.StructuredData{
			"id": id,
		},
		nil,
		opencdc.StructuredData{
			"id":        id,
			"archived":  false,
			"createdAt": time.Now().Format(time.RFC3339),
			"updatedAt": time.Now().Format(time.RFC3339),
			"properties": map[string]any{
				"email":     gofakeit.Email(),
				"firstname": gofakeit.FirstName(),
				"lastname":  gofakeit.LastName(),
			},
		},
	)
	rc.testRecords = append(rc.testRecords, rec)
	return rec
}

// NewTestDeleteRecord creates a test record with [opencdc.OperationDelete].
func (rc *RecordCreator) NewTestDeleteRecord(id string) opencdc.Record {
	return sdk.Util.Source.NewRecordDelete(
		nil, nil, opencdc.StructuredData{"id": id},
	)
}

// Cleanup deletes all created records from hubspot.
func (rc *RecordCreator) Cleanup() {
	if len(rc.testRecords) == 0 {
		return // nothing to clean up
	}

	ctx := context.Background()

	// list test resource items
	listResp, err := rc.client.List(ctx, rc.resource, nil)
	rc.is.NoErr(err)

	// (ab)use asserter to find the correct record in the list
	asserter := &RecordAsserter{}

	for _, want := range rc.testRecords {
		for _, got := range listResp.Results {
			if asserter.isEqual(want, got) {
				err = rc.client.Delete(ctx, rc.resource, asserter.getID(got))
				rc.is.NoErr(err)
			}
		}
	}
}
