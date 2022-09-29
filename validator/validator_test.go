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

package validator

import "testing"

func TestValidateStruct(t *testing.T) {
	t.Parallel()

	type args struct {
		data any
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				data: struct {
					AccessToken string `validate:"required"`
				}{
					AccessToken: "secret",
				},
			},
			wantErr: false,
		},
		{
			name: "success_pointer",
			args: args{
				data: &struct {
					AccessToken string `validate:"required"`
				}{},
			},
			wantErr: true,
		},
		{
			name: "success_custom_key",
			args: args{
				data: struct {
					AccessToken string `key:"accessToken" validate:"required"`
				}{},
			},
			wantErr: true,
		},
		{
			name: "fail_required",
			args: args{
				data: struct {
					AccessToken string `key:"accessToken" validate:"required"`
					Second      string `key:"second" validate:"required"`
				}{},
			},
			wantErr: true,
		},
		{
			name: "fail_lte",
			args: args{
				data: struct {
					BufferSize int `key:"bufferSize" validate:"lte=5"`
				}{
					BufferSize: 76,
				},
			},
			wantErr: true,
		},
		{
			name: "fail_gte",
			args: args{
				data: struct {
					BufferSize int `key:"bufferSize" validate:"gte=100"`
				}{
					BufferSize: 76,
				},
			},
			wantErr: true,
		},
		{
			name: "fail_lte_and_gte",
			args: args{
				data: struct {
					BufferSize int `key:"bufferSize" validate:"lte=102,gte=100"`
				}{
					BufferSize: 103,
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateStruct(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("ValidateStruct() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
