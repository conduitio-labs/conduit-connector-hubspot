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
)

// ResourcesPaths holds a mapping of resources and their list endpoints.
var ResourcesPaths = map[string]string{
	// https://developers.hubspot.com/docs/api/cms/blog-authors
	"cms.blogs.authors": "/cms/v3/blogs/authors",
	// https://developers.hubspot.com/docs/api/cms/blog-post
	"cms.blogs.posts": "/cms/v3/blogs/posts",
	// https://developers.hubspot.com/docs/api/cms/blog-tags
	"cms.blogs.tags": "/cms/v3/blogs/tags",
	// https://developers.hubspot.com/docs/api/cms/pages
	"cms.pages.landing": "/cms/v3/pages/landing-pages",
	"cms.pages.site":    "/cms/v3/pages/site-pages",
	// https://developers.hubspot.com/docs/api/cms/hubdb
	"cms.hubdb.tables": "/cms/v3/hubdb/tables",
	// https://developers.hubspot.com/docs/api/cms/domains
	"cms.domains": "/cms/v3/domains",
	// https://developers.hubspot.com/docs/api/cms/url-redirects
	"cms.urlRedirects": "/cms/v3/url-redirects",
	// https://developers.hubspot.com/docs/api/cms/content-audit
	"cms.auditLogs": "/cms/v3/audit-logs",
	// https://developers.hubspot.com/docs/api/conversations/conversations
	"conversations.channels":        "/conversations/v3/conversations/channels",
	"conversations.channelAccounts": "/conversations/v3/conversations/channel-accounts",
	"conversations.inboxes":         "/conversations/v3/conversations/inboxes",
	"conversations.threads":         "/conversations/v3/conversations/threads",
	// https://developers.hubspot.com/docs/api/crm/companies
	"crm.companies": "/crm/v3/objects/companies",
	// https://developers.hubspot.com/docs/api/crm/contacts
	"crm.contacts": "/crm/v3/objects/contacts",
	// https://developers.hubspot.com/docs/api/crm/deals
	"crm.deals": "/crm/v3/objects/deals",
	// https://developers.hubspot.com/docs/api/crm/feedback-submissions
	"crm.feedbackSubmissions": "/crm/v3/objects/feedback_submissions",
	// https://developers.hubspot.com/docs/api/crm/line-items
	"crm.lineItems": "/crm/v3/objects/line_items",
	// https://developers.hubspot.com/docs/api/crm/products
	"crm.products": "/crm/v3/objects/products",
	// https://developers.hubspot.com/docs/api/crm/tickets
	"crm.tickets": "/crm/v3/objects/tickets",
	// https://developers.hubspot.com/docs/api/crm/quotes
	"crm.quotes": "/crm/v3/objects/quotes",
	// https://developers.hubspot.com/docs/api/crm/calls
	"crm.calls": "/crm/v3/objects/calls",
	// https://developers.hubspot.com/docs/api/crm/email
	"crm.emails": "/crm/v3/objects/emails",
	// https://developers.hubspot.com/docs/api/crm/meetings
	"crm.meetings": "/crm/v3/objects/meetings",
	// https://developers.hubspot.com/docs/api/crm/notes
	"crm.notes": "/crm/v3/objects/notes",
	// https://developers.hubspot.com/docs/api/crm/tasks
	"crm.tasks": "/crm/v3/objects/tasks",
	// https://developers.hubspot.com/docs/api/crm/imports
	"crm.imports": "/crm/v3/imports",
	// https://developers.hubspot.com/docs/api/crm/owners
	"crm.owners": "/crm/v3/owners",
	// https://developers.hubspot.com/docs/api/events/web-analytics
	"events.web": "/events/v3/events",
	// https://developers.hubspot.com/docs/api/marketing/forms
	"marketing.forms": "/marketing/v3/forms",
	// https://developers.hubspot.com/docs/api/settings/user-provisioning
	"settings.users": "/settings/v3/users",
}

// ListOptions holds optional params for the [List] method.
type ListOptions struct {
	Limit int    `url:"limit,omitempty"`
	After string `url:"after,omitempty"`
}

// ListResponse is a common response model for endpoints that returns a list of results.
// It consists of a results list, paging info, and the total number of elements.
type ListResponse struct {
	Total   int                 `json:"total,omitempty"`
	Results []map[string]any    `json:"results"`
	Paging  *ListResponsePaging `json:"paging,omitempty"`
}

// ListResponsePaging is a paging info model for the [ListResponse].
type ListResponsePaging struct {
	Next ListResponsePagingNext `json:"next"`
}

// ListResponsePagingNext is a next model for the [ListResponsePaging].
type ListResponsePagingNext struct {
	After string `json:"after"`
	Link  string `json:"link"`
}

// List retrieves a list of items of a specific resource.
// The method raises an *[UnsupportedResourceError] if a provided resource is unsupported.
// If everything is okay, the method will return a *[ListResponse].
func (c *Client) List(ctx context.Context, resource string, opts *ListOptions) (*ListResponse, error) {
	resourcePath, ok := ResourcesPaths[resource]
	if !ok {
		return nil, &UnsupportedResourceError{
			Resource: resource,
		}
	}

	resourcePath, err := addOptions(resourcePath, opts)
	if err != nil {
		return nil, fmt.Errorf("add options: %w", err)
	}

	req, err := c.newRequest(ctx, http.MethodGet, resourcePath, nil)
	if err != nil {
		return nil, fmt.Errorf("create new request: %w", err)
	}

	var resp ListResponse
	if err := c.do(req, &resp); err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	return &resp, nil
}