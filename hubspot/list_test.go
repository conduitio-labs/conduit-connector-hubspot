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
	"net/http"
	"reflect"
	"testing"
)

func TestClient_List_success(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	mux.HandleFunc("/crm/v3/objects/quotes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write(
			[]byte(`{"results": [{"id": "1", "name": "hello"}], "paging": {"next": {"after": "2"}}}`),
		)
		if err != nil {
			t.Errorf("write body: %v", err)
		}
	})

	got, err := client.List(context.Background(), "crm.quotes", nil)
	if err != nil {
		t.Errorf("expected error to be nil, but got %v", err)
	}

	want := &ListResponse{
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

func TestClient_List_withOptions(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	mux.HandleFunc("/crm/v3/objects/quotes", func(w http.ResponseWriter, r *http.Request) {
		expectedQuery := "after=2&limit=1"

		if r.URL.RawQuery != expectedQuery {
			t.Errorf("r.URL.Path = %v, want = %v", r.URL.RawQuery, expectedQuery)
		}
	})

	_, err := client.List(context.Background(), "crm.quotes", &ListOptions{
		Limit: 1,
		After: "2",
	})
	if err != nil {
		t.Errorf("expected error to be nil, but got %v", err)
	}
}

func TestClient_List_unsupportedResource(t *testing.T) {
	t.Parallel()

	client, _, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	_, err := client.List(context.Background(), "wrong", nil)
	if err == nil {
		t.Errorf("expected error, but got nil")
	}

	var unsupportedResourceEerr *UnsupportedResourceError
	if !errors.As(err, &unsupportedResourceEerr) {
		t.Errorf("expected error to be UnsupportedResourceError, but got %v", err)
	}
}
