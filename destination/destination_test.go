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

package destination

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-hubspot/destination/mock"
	"github.com/conduitio-labs/conduit-connector-hubspot/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestDestination_Write_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := sdk.Record{
		Position:  sdk.Position("1.0"),
		Operation: sdk.OperationCreate,
		Key: sdk.StructuredData{
			"id": 1,
		},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"id":   1,
				"name": "Void",
			},
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Write(ctx, record).Return(nil)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []sdk.Record{record})
	is.NoErr(err)
	is.Equal(written, 1)
}

func TestDestination_Write_Fail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := sdk.Record{
		Position:  sdk.Position("1.0"),
		Operation: sdk.OperationCreate,
		Key: sdk.StructuredData{
			"id": 1,
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Write(ctx, record).Return(writer.ErrEmptyPayload)

	d := Destination{
		writer: w,
	}

	written, err := d.Write(ctx, []sdk.Record{record})
	is.Equal(err != nil, true)
	is.Equal(written, 0)
}
