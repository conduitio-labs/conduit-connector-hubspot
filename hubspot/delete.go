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

// ResourcesDeletePaths holds a mapping of supported resources and their delete endpoints.
var ResourcesDeletePaths = map[string]string{
	// https://developers.hubspot.com/docs/api/cms/blog-authors
	"cms.blogs.authors": "/cms/v3/blogs/authors/{objectId}",
	// https://developers.hubspot.com/docs/api/cms/blog-post
	"cms.blogs.posts": "/cms/v3/blogs/posts/{objectId}",
	// https://developers.hubspot.com/docs/api/cms/blog-tags
	"cms.blogs.tags": "/cms/v3/blogs/tags/{objectId}",
	// https://developers.hubspot.com/docs/api/cms/pages
	"cms.pages.landing": "/cms/v3/pages/landing-pages/{objectId}",
	"cms.pages.site":    "/cms/v3/pages/site-pages/{objectId}",
	// https://developers.hubspot.com/docs/api/cms/hubdb
	"cms.hubdb.tables": "/cms/v3/hubdb/tables/{objectId}/draft",
	// https://developers.hubspot.com/docs/api/cms/url-redirects
	"cms.urlRedirects": "/cms/v3/url-redirects/{objectId}",
	// https://developers.hubspot.com/docs/api/conversations/conversations
	"conversations.threads": "/conversations/v3/conversations/threads/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/companies
	"crm.companies": "/crm/v3/objects/companies/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/contacts
	"crm.contacts": "/crm/v3/objects/contacts/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/deals
	"crm.deals": "/crm/v3/objects/deals/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/feedback-submissions
	"crm.feedbackSubmissions": "/crm/v3/objects/feedback_submissions/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/line-items
	"crm.lineItems": "/crm/v3/objects/line_items/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/products
	"crm.products": "/crm/v3/objects/products/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/tickets
	"crm.tickets": "/crm/v3/objects/tickets/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/quotes
	"crm.quotes": "/crm/v3/objects/quotes/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/calls
	"crm.calls": "/crm/v3/objects/calls/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/email
	"crm.emails": "/crm/v3/objects/emails/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/meetings
	"crm.meetings": "/crm/v3/objects/meetings/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/notes
	"crm.notes": "/crm/v3/objects/notes/{objectId}",
	// https://developers.hubspot.com/docs/api/crm/tasks
	"crm.tasks": "/crm/v3/objects/tasks/{objectId}",
	// https://developers.hubspot.com/docs/api/marketing/forms
	"marketing.forms": "/marketing/v3/forms/{objectId}",
	// https://developers.hubspot.com/docs/api/settings/user-provisioning
	"settings.users": "/settings/v3/users/{objectId}",
}

// Delete tries to dekete an existing item of a specific resource.
// The method raises an *[UnsupportedResourceError] if a provided resource is unsupported.
func (c *Client) Delete(ctx context.Context, resource, itemID string) error {
	resourcePath, ok := ResourcesDeletePaths[resource]
	if !ok {
		return &UnsupportedResourceError{
			Resource: resource,
		}
	}

	resourcePath = strings.ReplaceAll(resourcePath, objectIDPlaceholder, itemID)

	req, err := c.newRequest(ctx, http.MethodDelete, resourcePath, nil)
	if err != nil {
		return fmt.Errorf("create new request: %w", err)
	}

	if err := c.do(req, nil); err != nil {
		return fmt.Errorf("execute request: %w", err)
	}

	return nil
}
