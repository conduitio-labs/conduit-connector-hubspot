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
)

func TestDestination_Write_successCreate(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new destination
	config := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// create a test sdk.Record
	trc := test.NewRecordCreator(t, testResource, false)
	testCreateRecord := trc.NewTestCreateRecord()

	// write the test record and check if the returned err is nil and n is equal to one
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// assert record was created
	tra := test.NewRecordAsserter(t, testResource)
	tra.Exists(testCreateRecord)

	// teardown the destination
	cancel()
	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_successCreateUpdate(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new destination
	config := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// create a test sdk.Record
	trc := test.NewRecordCreator(t, testResource, false)
	testCreateRecord := trc.NewTestCreateRecord()

	// write the test record and check if the returned err is nil and n is equal to one
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// assert record was created
	tra := test.NewRecordAsserter(t, testResource)
	ids := tra.Exists(testCreateRecord)

	// create a test record with update operation
	testUpdateRecord := trc.NewTestUpdateRecord(ids[0])

	n, err = destination.Write(ctx, []sdk.Record{testUpdateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	tra.Exists(testUpdateRecord)

	// teardown the destination
	cancel()
	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_successCreateDelete(t *testing.T) {
	is := is.New(t)

	// prepare a config, configure and open a new destination
	config := prepareConfig(t)

	destination := NewDestination()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// create a test sdk.Record
	trc := test.NewRecordCreator(t, testResource, false)
	testCreateRecord := trc.NewTestCreateRecord()

	// write the test record and check if the returned err is nil and n is equal to one
	n, err := destination.Write(ctx, []sdk.Record{testCreateRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// assert record was created
	tra := test.NewRecordAsserter(t, testResource)
	ids := tra.Exists(testCreateRecord)

	// create a test record with delete operation
	testDeleteRecord := trc.NewTestDeleteRecord(ids[0])

	n, err = destination.Write(ctx, []sdk.Record{testDeleteRecord})
	is.NoErr(err)
	is.Equal(n, 1)

	// assert record was deleted
	tra.NotExists(ids[0])

	// teardown the destination
	cancel()
	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failInvalidToken(t *testing.T) {
	is := is.New(t)

	// prepare a config with invalid access token, configure and open a new destination
	config := map[string]string{
		config.KeyAccessToken: "invalid",
		config.KeyResource:    testResource,
	}

	destination := NewDestination()

	ctx := context.Background()

	err := destination.Configure(ctx, config)
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	// we expect to get a 401 error because the access token we provided is invalid
	n, err := destination.Write(ctx, []sdk.Record{{
		Operation: sdk.OperationCreate,
		Payload:   sdk.Change{After: sdk.StructuredData{}},
	}})
	is.True(err != nil)
	is.Equal(n, 0)

	var unexpectedStatusCode *hubspot.UnexpectedStatusCodeError
	is.True(errors.As(err, &unexpectedStatusCode))
	is.Equal(unexpectedStatusCode.StatusCode, http.StatusUnauthorized)
}

func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	if testAccessToken == "" {
		t.Skip("HUBSPOT_ACCESS_TOKEN env var must be set")
	}

	return map[string]string{
		config.KeyAccessToken: testAccessToken,
		config.KeyResource:    testResource,
	}
}
