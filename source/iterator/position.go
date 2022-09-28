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

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Position is an iterator's position.
// It consists of the last processed element's id.
type Position struct {
	LastID int `json:"lastId"`
}

// MarshalSDKPosition marshals the underlying position into a [sdk.Position] as JSON bytes.
func (p *Position) MarshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// ParsePosition converts an [sdk.Position] into a [Position].
func ParsePosition(sdkPosition sdk.Position) (*Position, error) {
	var position Position

	if len(sdkPosition) == 0 {
		return nil, ErrEmptyPosition
	}

	if err := json.Unmarshal(sdkPosition, &position); err != nil {
		return nil, fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
	}

	return &position, nil
}
