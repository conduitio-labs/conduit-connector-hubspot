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
					config.KeyResource:    "contacts",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "contacts",
				},
				PollingPeriod: defaultPollingPeriod,
			},
			wantErr: false,
		},
		{
			name: "success_required_and_custom_values",
			args: args{
				cfg: map[string]string{
					config.KeyAccessToken:  "access_token",
					config.KeyResource:     "contacts",
					ConfigKeyPollingPeriod: "10s",
				},
			},
			want: Config{
				Config: config.Config{
					AccessToken: "access_token",
					Resource:    "contacts",
				},
				PollingPeriod: time.Second * 10,
			},
			wantErr: false,
		},
		{
			name: "fail_missing_required_common_config_value",
			args: args{
				cfg: map[string]string{
					config.KeyResource:     "contacts",
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
					config.KeyResource:     "contacts",
					ConfigKeyPollingPeriod: "ten seconds",
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
