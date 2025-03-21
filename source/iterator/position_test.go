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
	"reflect"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

func TestPosition_MarshalSDKPosition(t *testing.T) {
	t.Parallel()

	type fields struct {
		Mode   PositionMode
		LastID string
	}

	tests := []struct {
		name    string
		fields  fields
		want    opencdc.Position
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				Mode:   CDCPositionMode,
				LastID: "1",
			},
			want:    opencdc.Position([]byte(`{"mode":"cdc","itemId":"1"}`)),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Position{
				Mode:   tt.fields.Mode,
				ItemID: tt.fields.LastID,
			}

			got, err := p.MarshalSDKPosition()
			if (err != nil) != tt.wantErr {
				t.Errorf("Position.MarshalSDKPosition() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Position.MarshalSDKPosition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePosition(t *testing.T) {
	t.Parallel()

	type args struct {
		sdkPosition opencdc.Position
	}

	tests := []struct {
		name    string
		args    args
		want    *Position
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				sdkPosition: opencdc.Position([]byte(`{"mode":"cdc","itemId": "1"}`)),
			},
			want: &Position{
				Mode:   CDCPositionMode,
				ItemID: "1",
			},
			wantErr: false,
		},
		{
			name: "fail_empty_position",
			args: args{
				sdkPosition: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "fail_invalid_json_position",
			args: args{
				sdkPosition: opencdc.Position([]byte(`invalid_json`)),
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParsePosition(tt.args.sdkPosition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePosition() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParsePosition() = %v, want %v", got, tt.want)
			}
		})
	}
}
