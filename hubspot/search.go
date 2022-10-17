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
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// gteOperator is a greater then operator for search endpoints.
const gteOperator = "GTE"

// ascSortDirection stands for ascending sorting order.
const ascSortDirection = "ASCENDING"

// SearchResource holds a path, createdAt, and updatedAt field names.
type SearchResource struct {
	Path               string
	CreatedAtFieldName string
	UpdatedAtFieldName string
	UpdatedAtSortName  string
}

// SearchResources holds a mapping of resources that have search endpoints.
var SearchResources = map[string]SearchResource{
	// https://developers.hubspot.com/docs/api/crm/companies
	"crm.companies": {
		Path:               "/crm/v3/objects/companies/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/contacts
	"crm.contacts": {
		Path:               "/crm/v3/objects/contacts/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/deals
	"crm.deals": {
		Path:               "/crm/v3/objects/deals/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/feedback-submissions
	"crm.feedbackSubmissions": {
		Path:               "/crm/v3/objects/feedback_submissions/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/line-items
	"crm.lineItems": {
		Path:               "/crm/v3/objects/line_items/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/products
	"crm.products": {
		Path:               "/crm/v3/objects/products/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/tickets
	"crm.tickets": {
		Path:               "/crm/v3/objects/tickets/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/quotes
	"crm.quotes": {
		Path:               "/crm/v3/objects/quotes/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/calls
	"crm.calls": {
		Path:               "/crm/v3/objects/calls/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/email
	"crm.emails": {
		Path:               "/crm/v3/objects/emails/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/meetings
	"crm.meetings": {
		Path:               "/crm/v3/objects/meetings/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/notes
	"crm.notes": {
		Path:               "/crm/v3/objects/notes/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
	// https://developers.hubspot.com/docs/api/crm/tasks
	"crm.tasks": {
		Path:               "/crm/v3/objects/tasks/search",
		CreatedAtFieldName: "createdAt",
		UpdatedAtFieldName: "updatedAt",
		UpdatedAtSortName:  "hs_lastmodifieddate",
	},
}

// SearchRequest is a request model for the [Search] method.
type SearchRequest struct {
	Limit        string                     `json:"limit,omitempty"`
	FilterGroups []SearchRequestFilterGroup `json:"filterGroups,omitempty"`
	Sorts        []SearchRequestSort        `json:"sorts,omitempty"`
}

// SearchRequestFilterGroup is a fiterGroup object for the [SearchRequest].
type SearchRequestFilterGroup struct {
	Filters []SearchRequestFilterGroupFilter `json:"filters"`
}

// SearchRequestFilterGroupFilter is a filter object for the [SearchRequestFilterGroup].
type SearchRequestFilterGroupFilter struct {
	PropertyName string `json:"propertyName"`
	Operator     string `json:"operator"`
	Value        string `json:"value"`
}

// SearchRequestSort is a sort object for the [SearchRequest].
type SearchRequestSort struct {
	PropertyName string `json:"propertyName"`
	Direction    string `json:"direction"`
}

// Search performs an object search with filtering and returns the [ListResponse].
func (c *Client) Search(ctx context.Context, resource string, request *SearchRequest) (*ListResponse, error) {
	searchResource, ok := SearchResources[resource]
	if !ok {
		return nil, &UnsupportedResourceError{
			Resource: resource,
		}
	}

	req, err := c.newRequest(ctx, http.MethodPost, searchResource.Path, request)
	if err != nil {
		return nil, fmt.Errorf("create new request: %w", err)
	}

	var resp ListResponse
	if err := c.do(req, &resp); err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	return &resp, nil
}

// SearchByUpdatedAfter is a wrapper that calls the [Search] method returning only those results
// that were updated after a specific date and ordering them ascendingly by updatedAt field.
func (c *Client) SearchByUpdatedAfter(
	ctx context.Context,
	resource string,
	updatedAfter time.Time,
	limit int,
) (*ListResponse, error) {
	searchResource, ok := SearchResources[resource]
	if !ok {
		return nil, &UnsupportedResourceError{
			Resource: resource,
		}
	}

	return c.Search(ctx, resource, &SearchRequest{
		Limit: strconv.Itoa(limit),
		FilterGroups: []SearchRequestFilterGroup{
			{
				Filters: []SearchRequestFilterGroupFilter{
					{
						PropertyName: searchResource.UpdatedAtSortName,
						Operator:     gteOperator,
						Value:        strconv.Itoa(int(updatedAfter.UnixMilli())),
					},
				},
			},
		},
		Sorts: []SearchRequestSort{
			{
				PropertyName: searchResource.UpdatedAtSortName,
				Direction:    ascSortDirection,
			},
		},
	})
}
