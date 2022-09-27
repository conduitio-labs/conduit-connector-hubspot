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

import "fmt"

// UnexpectedStatusCodeError occurs when a response from the HubSpot API has non-200 status code.
type UnexpectedStatusCodeError struct {
	StatusCode int
}

// Error returns a formated error message for the UnexpectedStatusCodeError.
func (e *UnexpectedStatusCodeError) Error() string {
	return fmt.Sprintf("unexpected status code %d", e.StatusCode)
}

// UnsupportedResourceError occurs when an unsupported resource is provided.
type UnsupportedResourceError struct {
	Resource string
}

// Error returns a formated error message for the UnsupportedResourceError.
func (e *UnsupportedResourceError) Error() string {
	return fmt.Sprintf("unsupported resource %q", e.Resource)
}
