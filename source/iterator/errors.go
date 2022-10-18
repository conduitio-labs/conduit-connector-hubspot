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

package iterator

import "errors"

var (
	// ErrEmptyPosition occurs when a provided SDK's position is empty.
	// It's used as a sentinel error within the [ParsePosition] method.
	ErrEmptyPosition = errors.New("position is empty")
	// ErrItemIDIsNotAString shouldn't happen cause HubSpot API v3 returns items with string identifiers
	// but it exists as a last resort.
	ErrItemIDIsNotAString = errors.New("item's id is not a string")
	// ErrNoInitializedIterator occurs when the Combined iterator has no any initialized underlying iterators.
	ErrNoInitializedIterator = errors.New("no initialized iterator")
)
