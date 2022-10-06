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
	"strings"
)

// ResourceUpdatePath holds a path and a corresponding method
// for updating a specific resource.
type ResourceUpdatePath struct {
	Path   string
	Method string
}

// ResourcesUpdatePaths holds a mapping of supported resources, their update endpoints and HTTP methods.
var ResourcesUpdatePaths = map[string]ResourceUpdatePath{
	// https://developers.hubspot.com/docs/api/cms/blog-authors
	"cms.blogs.authors": {
		Path: "/cms/v3/blogs/authors/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/cms/blog-post
	"cms.blogs.posts": {
		Path: "/cms/v3/blogs/posts/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/cms/blog-tags
	"cms.blogs.tags": {
		Path: "/cms/v3/blogs/tags/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/cms/pages
	"cms.pages.landing": {
		Path: "/cms/v3/pages/landing-pages/{objectId}", Method: http.MethodPatch,
	},
	"cms.pages.site": {
		Path: "/cms/v3/pages/site-pages/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/cms/hubdb
	"cms.hubdb.tables": {
		Path: "/cms/v3/hubdb/tables/{objectId}/draft", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/cms/url-redirects
	"cms.urlRedirects": {
		Path: "/cms/v3/url-redirects/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/conversations/conversations
	"conversations.threads": {
		Path: "/conversations/v3/conversations/threads/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/companies
	"crm.companies": {
		Path: "/crm/v3/objects/companies/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/contacts
	"crm.contacts": {
		Path: "/crm/v3/objects/contacts/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/deals
	"crm.deals": {
		Path: "/crm/v3/objects/deals/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/feedback-submissions
	"crm.feedbackSubmissions": {
		Path: "/crm/v3/objects/feedback_submissions/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/line-items
	"crm.lineItems": {
		Path: "/crm/v3/objects/line_items/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/products
	"crm.products": {
		Path: "/crm/v3/objects/products/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/tickets
	"crm.tickets": {
		Path: "/crm/v3/objects/tickets/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/quotes
	"crm.quotes": {
		Path: "/crm/v3/objects/quotes/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/calls
	"crm.calls": {
		Path: "/crm/v3/objects/calls/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/email
	"crm.emails": {
		Path: "/crm/v3/objects/emails/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/meetings
	"crm.meetings": {
		Path: "/crm/v3/objects/meetings/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/notes
	"crm.notes": {
		Path: "/crm/v3/objects/notes/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/crm/tasks
	"crm.tasks": {
		Path: "/crm/v3/objects/tasks/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/marketing/forms
	"marketing.forms": {
		Path: "/marketing/v3/forms/{objectId}", Method: http.MethodPatch,
	},
	// https://developers.hubspot.com/docs/api/settings/user-provisioning
	"settings.users": {
		Path: "/settings/v3/users/{objectId}", Method: http.MethodPut,
	},
}

// Update tries to update an existing item of a specific resource.
// The method raises an *[UnsupportedResourceError] if a provided resource is unsupported.
func (c *Client) Update(ctx context.Context, resource, itemID string, item map[string]any) error {
	resourcePath, ok := ResourcesUpdatePaths[resource]
	if !ok {
		return &UnsupportedResourceError{
			Resource: resource,
		}
	}

	resourcePath.Path = strings.ReplaceAll(resourcePath.Path, objectIDPlaceholder, itemID)

	req, err := c.newRequest(ctx, resourcePath.Method, resourcePath.Path, item)
	if err != nil {
		return fmt.Errorf("create new request: %w", err)
	}

	if err := c.do(req, nil); err != nil {
		return fmt.Errorf("do request: %w", err)
	}

	return nil
}
