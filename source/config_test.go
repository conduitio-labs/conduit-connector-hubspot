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

package source

import (
	"reflect"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-hubspot/config"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}

	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "success_required_and_default_values",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken: "access_token",
					config.KeyResource:    "crm.contacts",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "crm.contacts",
					MaxRetries:  config.DefaultMaxRetries,
				},
				PollingPeriod: defaultPollingPeriod,
				BufferSize:    defaultBufferSize,
			},
			wantErr: false,
		},
		{
			name: "success_required_and_custom_values",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:  "access_token",
					config.KeyResource:     "crm.contacts",
					config.KeyMaxRetries:   "10",
					ConfigKeyPollingPeriod: "10s",
					ConfigKeyBufferSize:    "100",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "crm.contacts",
					MaxRetries:  10,
				},
				PollingPeriod: time.Second * 10,
				BufferSize:    100,
			},
			wantErr: false,
		},
		{
			name: "success_zero_polling_period",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:  "access_token",
					config.KeyResource:     "crm.contacts",
					ConfigKeyPollingPeriod: "0s",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "crm.contacts",
					MaxRetries:  config.DefaultMaxRetries,
				},
				PollingPeriod: defaultPollingPeriod,
				BufferSize:    defaultBufferSize,
			},
			wantErr: false,
		},
		{
			name: "success_extra_properties",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:    "access_token",
					config.KeyResource:       "crm.contacts",
					ConfigKeyExtraProperties: "name,email",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "crm.contacts",
					MaxRetries:  config.DefaultMaxRetries,
				},
				PollingPeriod:   defaultPollingPeriod,
				BufferSize:      defaultBufferSize,
				ExtraProperties: []string{"name", "email"},
			},
			wantErr: false,
		},
		{
			name: "success_extra_properties_with_redundant_spaces_and_commas",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:    "access_token",
					config.KeyResource:       "crm.contacts",
					ConfigKeyExtraProperties: "name,email , ,, , ,,createdAt, , updatedAt",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "crm.contacts",
					MaxRetries:  config.DefaultMaxRetries,
				},
				PollingPeriod:   defaultPollingPeriod,
				BufferSize:      defaultBufferSize,
				ExtraProperties: []string{"name", "email", "createdAt", "updatedAt"},
			},
			wantErr: false,
		},
		{
			name: "fail_missing_required_common_config_value",
			args: args{
				cfg: map[string]string{
					config.KeyResource:     "crm.contacts",
					ConfigKeyPollingPeriod: "10s",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_polling_period",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:  "access_token",
					config.KeyResource:     "crm.contacts",
					ConfigKeyPollingPeriod: "ten seconds",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_buffer_size_not_a_number",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken: "access_token",
					config.KeyResource:    "crm.contacts",
					ConfigKeyBufferSize:   "ten",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_buffer_size_lte",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken: "access_token",
					config.KeyResource:    "crm.contacts",
					ConfigKeyBufferSize:   "0",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_buffer_size_gte",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken: "access_token",
					config.KeyResource:    "crm.contacts",
					ConfigKeyBufferSize:   "101",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail_invalid_polling_period_gt",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:  "access_token",
					config.KeyResource:     "crm.contacts",
					ConfigKeyPollingPeriod: "-1s",
					ConfigKeyBufferSize:    "100",
				},
			},
			want:    Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseConfig(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseConfig() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
