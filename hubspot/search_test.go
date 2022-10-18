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
	"errors"
	"io"
	"net/http"
	"reflect"
	"testing"
)

func TestClient_Search_success(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	expectedReqBody := []byte(
		`{"filterGroups":[{"filters":[{"propertyName":"lastmodifieddate","operator":"GTE","value":"1664551127170"}]}]}`,
	)

	mux.HandleFunc("/crm/v3/objects/contacts/search", func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}

		if reflect.DeepEqual(reqBody, expectedReqBody) {
			t.Errorf("expected body to be %s, but got %s", expectedReqBody, reqBody)
		}

		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(
			[]byte(`{"total":5,"results": [{"id": "1", "name": "hello"}], "paging": {"next": {"after": "2"}}}`),
		)
		if err != nil {
			t.Errorf("write body: %v", err)
		}
	})

	got, err := client.Search(context.Background(), "crm.contacts", &SearchRequest{
		FilterGroups: []SearchRequestFilterGroup{
			{
				Filters: []SearchRequestFilterGroupFilter{
					{
						PropertyName: "hs_lastmodifieddate",
						Operator:     "GTE",
						Value:        "1664551127170",
					},
				},
			},
		},
	})
	if err != nil {
		t.Errorf("expected error to be nil, but got %v", err)
	}

	want := &ListResponse{
		Total:   5,
		Results: []ListResponseResult{{"id": "1", "name": "hello"}},
		Paging: &ListResponsePaging{
			Next: ListResponsePagingNext{
				After: "2",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Response body = %v, expected %v", got, want)
	}
}

func TestClient_Search_unsupportedResource(t *testing.T) {
	t.Parallel()

	client, _, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	_, err := client.Search(context.Background(), "wrong", nil)
	if err == nil {
		t.Errorf("expected error, but got nil")
	}

	var unsupportedResourceEerr *UnsupportedResourceError
	if !errors.As(err, &unsupportedResourceEerr) {
		t.Errorf("expected error to be UnsupportedResourceError, but got %v", err)
	}
}
