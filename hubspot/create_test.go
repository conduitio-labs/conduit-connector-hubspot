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
	"testing"
)

func TestClient_Create_success(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	mux.HandleFunc("/crm/v3/objects/quotes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected method to be %s, but got %s", http.MethodDelete, r.Method)
		}

		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("expected error to be nil, but got %v", err)
		}

		expected := `{"name":"Bob"}` + "\n"
		if string(reqBody) != expected {
			t.Errorf("Request body = %v, expected %v", string(reqBody), expected)
		}

		w.WriteHeader(http.StatusOK)
	})

	err := client.Create(context.Background(), "crm.quotes", map[string]any{"name": "Bob"})
	if err != nil {
		t.Errorf("expected error to be nil, but got %v", err)
	}
}

func TestClient_Create_unsupportedResource(t *testing.T) {
	t.Parallel()

	client, _, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	err := client.Create(context.Background(), "wrong", map[string]any{"name": "Bob"})
	if err == nil {
		t.Errorf("expected error, but got nil")
	}

	var unsupportedResourceEerr *UnsupportedResourceError
	if !errors.As(err, &unsupportedResourceEerr) {
		t.Errorf("expected error to be UnsupportedResourceError, but got %v", err)
	}
}
