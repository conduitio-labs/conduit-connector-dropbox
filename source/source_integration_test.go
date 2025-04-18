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
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-dropbox/config"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestSource_Read(t *testing.T) {
	t.Run("successful read operations", func(t *testing.T) {
		is := is.New(t)
		ctx, cancel := context.WithCancel(context.Background())

		mockClient := dropbox.NewMockClient()
		src := &Source{
			client: mockClient,
			config: Config{
				Config: config.Config{
					Token: "test-token",
				},
				FileChunkSizeBytes: 1024,
			},
		}
		defer func() {
			err := src.Teardown(ctx)
			is.NoErr(err)
		}()

		err := src.Open(ctx, nil)
		is.NoErr(err)

		var allRecords []opencdc.Record
		for len(allRecords) < 3 {
			records, err := src.ReadN(ctx, 3-len(allRecords))
			is.NoErr(err)
			allRecords = append(allRecords, records...)
		}

		is.Equal(len(allRecords), 3)

		for _, record := range allRecords {
			is.True(len(record.Payload.After.Bytes()) > 0)
			is.True(record.Metadata["file_path"] != "")
		}
		cancel()
	})

	t.Run("context cancellation", func(t *testing.T) {
		is := is.New(t)
		ctx, cancel := context.WithCancel(context.Background())

		src := &Source{
			client: dropbox.NewMockClient(),
			config: Config{
				Config: config.Config{
					Token: "test-token",
				},
				FileChunkSizeBytes: 1024,
			},
		}
		defer func() {
			err := src.Teardown(ctx)
			is.NoErr(err)
		}()

		err := src.Open(ctx, nil)
		is.NoErr(err)

		cancel()
		_, err = src.ReadN(ctx, 1)
		is.Equal(err, ctx.Err())
	})

	t.Run("chunked file processing", func(t *testing.T) {
		is := is.New(t)
		ctx, cancel := context.WithCancel(context.Background())

		mockClient := dropbox.NewMockClient()
		largeFileContent := make([]byte, 5000) // 5KB file
		mockClient.SetFiles(map[string][]byte{"/docs/large.txt": largeFileContent})

		src := &Source{
			client: mockClient,
			config: Config{
				Config: config.Config{
					Token: "test-token",
				},
				FileChunkSizeBytes: 1000,
			},
		}
		defer func() {
			err := src.Teardown(ctx)
			is.NoErr(err)
		}()

		err := src.Open(ctx, nil)
		is.NoErr(err)

		// Should get 5 chunks (5KB file / 1KB chunks)
		var chunks []opencdc.Record
		for len(chunks) < 5 {
			records, err := src.ReadN(ctx, 5-len(chunks))
			is.NoErr(err)
			chunks = append(chunks, records...)
		}

		is.Equal(len(chunks), 5)
		for i, chunk := range chunks {
			is.Equal(chunk.Metadata["is_chunked"], "true")
			is.Equal(chunk.Metadata["chunk_index"], fmt.Sprintf("%d", i+1))
			is.Equal(chunk.Metadata["total_chunks"], "5")
		}
		cancel()
	})
}
