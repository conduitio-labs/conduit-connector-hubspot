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

// Package hubspot is a Go client library for accessing the HubSpot API v3.
package hubspot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"

	"github.com/google/go-querystring/query"
)

// defaultBaseURL is a default HubSpot API base URL.
const defaultBaseURL = "https://api.hubapi.com"

// A Client manages communication with the HubSpot API.
type Client struct {
	accessToken string
	httpClient  *http.Client
	baseURL     *url.URL
}

// NewClient creates a new instance of the Client.
func NewClient(accessToken string, httpClient *http.Client) *Client {
	client := &Client{
		accessToken: accessToken,
		httpClient:  httpClient,
	}

	// ignore the error cause we'll never get it here
	client.baseURL, _ = url.Parse(defaultBaseURL)

	return client
}

// newRequest creates an API request.
func (c *Client) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	reqURL, err := c.baseURL.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("parse request path: %w", err)
	}

	var buf io.ReadWriter
	if body != nil {
		buf = &bytes.Buffer{}
		if err = json.NewEncoder(buf).Encode(body); err != nil {
			return nil, fmt.Errorf("json encode body: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), buf)
	if err != nil {
		return nil, fmt.Errorf("create request with context: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.accessToken))

	return req, nil
}

// do sends an API request and returns the API response. The API response is
// JSON decoded and stored in the value pointed to by out, or returned as an
// error if an API error has occurred.
func (c *Client) do(req *http.Request, out any) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http client do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		unexpectedStatusCodeErr := &UnexpectedStatusCodeError{
			StatusCode: resp.StatusCode,
		}

		unexpectedStatusCodeErr.Body, err = io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read resp body: %w", err)
		}

		return unexpectedStatusCodeErr
	}

	switch out := out.(type) {
	case nil:
	case io.Writer:
		if _, err = io.Copy(out, resp.Body); err != nil {
			return fmt.Errorf("copy resp.Body: %w", err)
		}

	default:
		if err = json.NewDecoder(resp.Body).Decode(out); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("decode resp.Body: %w", err)
		}
	}

	return nil
}

// addOptions adds the parameters in opts as URL query parameters to s. opts
// must be a struct whose fields may contain "url" tags.
func addOptions(s string, opts any) (string, error) {
	v := reflect.ValueOf(opts)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return s, nil
	}

	u, err := url.Parse(s)
	if err != nil {
		return s, fmt.Errorf("parse path: %w", err)
	}

	qs, err := query.Values(opts)
	if err != nil {
		return s, fmt.Errorf("get url values: %w", err)
	}

	u.RawQuery = qs.Encode()

	return u.String(), nil
}
