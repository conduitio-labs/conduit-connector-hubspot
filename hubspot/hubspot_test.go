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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

// setup sets up a test HTTP server along with a hubspot.Client that is
// configured to talk to that test server.
func setup() (client *Client, mux *http.ServeMux, teardown func()) {
	mux = http.NewServeMux()
	server := httptest.NewServer(mux)

	client = NewClient("secret", server.Client())
	url, _ := url.Parse(server.URL)
	client.baseURL = url

	return client, mux, server.Close
}

func TestClient_newRequest(t *testing.T) {
	t.Parallel()

	client := NewClient("secret", http.DefaultClient)

	inURL, outURL := "/cms/v3/blogs/authors", defaultBaseURL+"/cms/v3/blogs/authors"

	type FiscalCommand struct {
		Task int `json:"task"`
	}

	inBody, outBody := &FiscalCommand{Task: 1}, `{"task":1}`+"\n"

	req, err := client.newRequest(context.Background(), "GET", inURL, inBody)
	if err != nil {
		t.Fatalf("NewRequest unexpected error: %v", err)
	}

	// test that relative URL was expanded
	if got, want := req.URL.String(), outURL; got != want {
		t.Errorf("NewRequest(%q) URL is %v, want %v", inURL, got, want)
	}

	// test that body was JSON encoded
	body, _ := io.ReadAll(req.Body)
	if got, want := string(body), outBody; got != want {
		t.Errorf("NewRequest(%q) Body is %v, want %v", inBody, got, want)
	}

	// test that Authorization HTTP headers is applied
	if got, want := req.Header.Get("Authorization"), "Bearer secret"; got != want {
		t.Errorf("NewRequest() Authorization is %v, want %v", got, want)
	}
}

func TestClient_newRequest_invalidJSON(t *testing.T) {
	t.Parallel()

	client, _, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	_, err := client.newRequest(context.Background(), http.MethodGet, ".", map[any]any{})
	if err == nil {
		t.Error("Expected error to be returned.")
	}

	var unsupportedTypeError *json.UnsupportedTypeError
	if !errors.As(err, &unsupportedTypeError) {
		t.Errorf("Expected a JSON error; got %#v.", errors.Unwrap(err))
	}
}

func TestClient_newRequest_badURL(t *testing.T) {
	t.Parallel()

	client, _, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	_, err := client.newRequest(context.Background(), http.MethodGet, ":", nil)
	if err == nil {
		t.Errorf("Expected error to be returned")
	}

	var urlError *url.Error
	if !errors.As(err, &urlError) || urlError.Op != "parse" {
		t.Errorf("Expected URL parse error, got %+v", err)
	}
}

func TestClient_newRequest_badMethod(t *testing.T) {
	t.Parallel()

	client, _, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	if _, err := client.newRequest(context.Background(), "unk\nnown", ".", nil); err == nil {
		t.Fatal("NewRequest returned nil; expected error")
	}
}

func TestClient_do_get(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	type command struct {
		Tag int `json:"key"`
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Request method = %v, expected %v", r.Method, http.MethodGet)
		}

		fmt.Fprint(w, `{"key":1}`)
	})

	// test with a custom struct
	req, _ := client.newRequest(context.Background(), http.MethodGet, "/", nil)
	body := new(command)

	if err := client.do(req, body); err != nil {
		t.Fatalf("Do(): %v", err)
	}

	expectedBody := &command{Tag: 1}
	if !reflect.DeepEqual(body, expectedBody) {
		t.Errorf("Response body = %v, expected %v", body, expectedBody)
	}

	// test with an io.Writer
	req, _ = client.newRequest(context.Background(), http.MethodGet, "/", nil)
	buf := bytes.NewBuffer(nil)

	if err := client.do(req, buf); err != nil {
		t.Fatalf("Do(): %v", err)
	}

	expectedBufString := `{"key":1}`
	if !reflect.DeepEqual(buf.String(), expectedBufString) {
		t.Errorf("Response body = %v, expected %v", buf.String(), expectedBufString)
	}
}

func TestClient_do_post(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	type command struct {
		Tag  int `json:"key"`
		Task int `json:"task"`
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Request method = %v, expected %v", r.Method, http.MethodGet)
		}

		var reqBody command
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Fatalf("json Decode returned unexpected error: %v", err)
		}

		expected := command{
			Tag:  1,
			Task: 2,
		}

		if !reflect.DeepEqual(reqBody, expected) {
			t.Errorf("Response body = %v, expected %v", reqBody, expected)
		}

		w.WriteHeader(http.StatusOK)
	})

	// test with a custom struct
	reqBody := &command{
		Tag:  1,
		Task: 2,
	}

	req, _ := client.newRequest(context.Background(), http.MethodPost, "/", reqBody)

	if err := client.do(req, nil); err != nil {
		t.Fatalf("Do(): %v", err)
	}
}

func TestClient_do_httpError(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	})

	req, _ := client.newRequest(context.Background(), http.MethodGet, "/", nil)

	err := client.do(req, nil)
	if err == nil {
		t.Errorf("Expected error to be returned")
	}

	var expectedError *UnexpectedStatusCodeError
	if !errors.As(err, &expectedError) {
		t.Errorf("Expected Unexpected Status Code error, got %+v", err)
	}
}

func TestClient_do_nilURL(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req, _ := client.newRequest(context.Background(), http.MethodGet, "/", nil)
	req.URL = nil

	err := client.do(req, nil)
	if err == nil {
		t.Errorf("Expected error to be returned")
	}

	var urlError *url.Error
	if !errors.As(err, &urlError) {
		t.Errorf("Expected URL error, got %+v", err)
	}
}

func TestClient_do_invalidJSON(t *testing.T) {
	t.Parallel()

	client, mux, teardown := setup()

	t.Cleanup(func() {
		teardown()
	})

	type command struct {
		Tag int `json:"key"`
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// we expect the key to be an integer, but get a string
		fmt.Fprint(w, `{"key":"1"}`)
	})

	// test with a custom struct
	req, _ := client.newRequest(context.Background(), http.MethodGet, "/", nil)
	body := new(command)

	err := client.do(req, &body)
	if err == nil {
		t.Errorf("Expected error to be returned")
	}

	var unmarshalTypeError *json.UnmarshalTypeError
	if !errors.As(err, &unmarshalTypeError) {
		t.Errorf("Expected a JSON error; got %#v.", errors.Unwrap(err))
	}
}
