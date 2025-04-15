// Copyright © 2025 Meroxa, Inc.
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
	"context"
	"testing"

	config "github.com/conduitio-labs/conduit-connector-dropbox/config"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestSource_Read_success(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	source := &Source{}
	defer func() {
		err := source.Teardown(ctx)
		is.NoErr(err)
	}()

	source.config = Config{
		Config: config.Config{
			Token: "test-token",
		},
		FileChunkSizeBytes: 3145728,
	}
	source.client = dropbox.NewMockClient()

	err := source.Open(ctx, nil)
	is.NoErr(err)

	var allRecords []opencdc.Record
	for len(allRecords) < 3 {
		records, err := source.ReadN(ctx, 3-len(allRecords))
		is.NoErr(err)

		allRecords = append(allRecords, records...)
	}

	is.Equal(len(allRecords), 3)

	cancel()
}

func TestSource_Read_failed_context_canceled(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	source := &Source{}
	defer func() {
		err := source.Teardown(ctx)
		is.NoErr(err)
	}()

	source.config = Config{
		Config: config.Config{
			Token: "test-token",
		},
		FileChunkSizeBytes: 3145728,
	}
	source.client = dropbox.NewMockClient()

	err := source.Open(ctx, nil)
	is.NoErr(err)

	cancel()
	records, err := source.ReadN(ctx, 2)
	is.Equal(err, ctx.Err())
	is.Equal(records, nil)
}
