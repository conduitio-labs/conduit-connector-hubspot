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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// PositionMode defines a position mode.
type PositionMode string

// The available position modes are listed below.
const (
	// SnapshotPositionMode is a snapshot position mode.
	SnapshotPositionMode PositionMode = "snapshot"
	// CDCPositionMode is a CDC position mode.
	CDCPositionMode PositionMode = "cdc"
)

// Position is an iterator's position.
// It consists of the [PositionMode], the last processed item's id, and a timestamp.
type Position struct {
	Mode PositionMode `json:"mode"`
	// ItemID is used if the position's mode is [SnapshotPositionMode].
	ItemID string `json:"itemId,omitempty"`
	// InitialTimestamp is an initial timestamp of a snapshot.
	InitialTimestamp *time.Time `json:"initialTimestamp,omitempty"`
	// Timestamp is used if the position's mode is [CDCPositionMode], or for [SnapshotPositionMode] if it was interrupted.
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

// MarshalSDKPosition marshals the underlying position into a [opencdc.Position] as JSON bytes.
func (p *Position) MarshalSDKPosition() (opencdc.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return opencdc.Position(positionBytes), nil
}

// ParsePosition converts an [opencdc.Position] into a [Position].
func ParsePosition(sdkPosition opencdc.Position) (*Position, error) {
	var position Position

	if len(sdkPosition) == 0 {
		return nil, ErrEmptyPosition
	}

	if err := json.Unmarshal(sdkPosition, &position); err != nil {
		return nil, fmt.Errorf("unmarshal opencdc.Position into Position: %w", err)
	}

	return &position, nil
}
