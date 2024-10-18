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
	"testing"

	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

type RecordAsserter struct {
	is       *is.I
	client   *hubspot.Client
	resource string
}

func NewRecordAsserter(t *testing.T, resource string) *RecordAsserter {
	t.Helper()
	ra := &RecordAsserter{
		is: is.New(t),
		client: hubspot.NewClient(
			testAccessToken,
			&http.Client{
				Timeout: testHTTPClientTimeout,
			},
		),
		resource: resource,
	}
	return ra
}

func (ra *RecordAsserter) Exists(wantRecs ...opencdc.Record) []string {
	ctx := context.Background()
	listResp, err := ra.client.List(ctx, ra.resource, nil)
	ra.is.NoErr(err)

	ids := make([]string, len(wantRecs))
REC:
	for i, wantRec := range wantRecs {
		for _, gotResp := range listResp.Results {
			if ra.isEqual(wantRec, gotResp) {
				ids[i] = ra.getID(gotResp)
				continue REC // found expected record, continue searching for next
			}
		}
		ra.is.Fail() // did not find record
	}
	return ids
}

func (ra *RecordAsserter) NotExists(ids ...string) {
	ctx := context.Background()
	listResp, err := ra.client.List(ctx, ra.resource, nil)
	ra.is.NoErr(err)

	for _, wantID := range ids {
		for _, gotResp := range listResp.Results {
			if wantID == ra.getID(gotResp) {
				ra.is.Fail() // did not expect record to exist
			}
		}
	}
}

func (*RecordAsserter) isEqual(want opencdc.Record, got hubspot.ListResponseResult) bool {
	sd, ok := want.Payload.After.(opencdc.StructuredData)
	if !ok {
		return false
	}
	wantPropertiesRaw, ok := sd["properties"]
	if !ok {
		return false
	}
	wantProperties, ok := wantPropertiesRaw.(map[string]any)
	if !ok {
		return false
	}

	gotPropertiesRaw, ok := got["properties"]
	if !ok {
		return false
	}
	gotProperties, ok := gotPropertiesRaw.(map[string]any)
	if !ok {
		return false
	}

	for k, wantProperty := range wantProperties {
		if gotProperties[k] != wantProperty {
			return false
		}
	}
	return true
}

func (*RecordAsserter) getID(resp hubspot.ListResponseResult) string {
	idRaw, ok := resp["id"]
	if !ok {
		return ""
	}
	id, ok := idRaw.(string)
	if !ok {
		return ""
	}
	return id
}
