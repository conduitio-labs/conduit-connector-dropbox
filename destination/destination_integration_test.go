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

package destination

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-dropbox/config"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	dest := &Destination{}
	defer func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	}()

	// Configure with mock client
	dest.config = Config{
		Config: config.Config{
			Token: "test-token",
		},
	}
	dest.client = dropbox.NewMockClient()

	err := dest.Open(ctx)
	is.NoErr(err)

	t.Run("successful file upload", func(t *testing.T) {
		is := is.New(t)
		records := []opencdc.Record{
			{
				Operation: opencdc.OperationCreate,
				Payload: opencdc.Change{
					After: opencdc.RawData([]byte("test content")),
				},
				Metadata: map[string]string{
					"file_path": "/test/file.txt",
					"hash":      "mock-hash",
				},
			},
		}

		n, err := dest.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, 1)
	})

	t.Run("successful chunked file upload", func(t *testing.T) {
		is := is.New(t)
		records := []opencdc.Record{
			// First chunk
			{
				Operation: opencdc.OperationCreate,
				Payload: opencdc.Change{
					After: opencdc.RawData([]byte("chunk1")),
				},
				Metadata: map[string]string{
					"is_chunked":   "true",
					"chunk_index":  "1",
					"total_chunks": "2",
					"filename":     "chunked.txt",
					"file_path":    "/test/chunked.txt",
					"hash":         "chunked-hash",
					"file_size":    "12",
					"file_id":      "chunked-file",
				},
			},
			// Second chunk
			{
				Operation: opencdc.OperationCreate,
				Payload: opencdc.Change{
					After: opencdc.RawData([]byte("chunk2")),
				},
				Metadata: map[string]string{
					"is_chunked":   "true",
					"chunk_index":  "2",
					"total_chunks": "2",
					"filename":     "chunked.txt",
					"file_path":    "/test/chunked.txt",
					"hash":         "chunked-hash",
					"file_size":    "12",
					"file_id":      "chunked-file",
				},
			},
		}

		n, err := dest.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, 2)
	})

	t.Run("successful file deletion", func(t *testing.T) {
		is := is.New(t)
		records := []opencdc.Record{
			{
				Operation: opencdc.OperationDelete,
				Metadata: map[string]string{
					"file_path": "/test/file.txt",
				},
			},
		}

		n, err := dest.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, 1)
	})
}
