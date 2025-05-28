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

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	con := &Destination{}

	con.config = Config{
		Config: config.Config{
			Token: "test-token",
		},
	}

	err := con.Open(ctx)
	is.NoErr(err)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestDestination_WriteSuccess(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	m := dropbox.NewMockFoldersClient(t)
	dest := &Destination{
		client: m,
		config: Config{
			Config: config.Config{
				Token: "test-token",
			},
		},
	}
	defer func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	}()

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
					opencdc.MetadataCollection: "/test",
					opencdc.MetadataFileName:   "file.txt",
					opencdc.MetadataFileHash:   "mock-hash",
					opencdc.MetadataFileSize:   "12",
				},
			},
		}

		m.On("UploadFile", ctx, "/test/file.txt", []byte("test content")).
			Return(&dropbox.UploadFileResponse{
				ContentHash: "mock-hash",
				Size:        12,
			}, nil)

		n, err := dest.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, 1)

		m.AssertExpectations(t)
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
					opencdc.MetadataFileChunked:    "true",
					opencdc.MetadataFileChunkIndex: "1",
					opencdc.MetadataFileChunkCount: "2",
					opencdc.MetadataFileName:       "chunked.txt",
					"file_path":                    "/test/chunked.txt",
					opencdc.MetadataFileHash:       "chunked-hash",
					opencdc.MetadataFileSize:       "12",
					"file_id":                      "chunked-file",
					opencdc.MetadataCollection:     "/test",
				},
			},
			// Second chunk
			{
				Operation: opencdc.OperationCreate,
				Payload: opencdc.Change{
					After: opencdc.RawData([]byte("chunk2")),
				},
				Metadata: map[string]string{
					opencdc.MetadataFileChunked:    "true",
					opencdc.MetadataFileChunkIndex: "2",
					opencdc.MetadataFileChunkCount: "2",
					opencdc.MetadataFileName:       "chunked.txt",
					"file_path":                    "/test/chunked.txt",
					opencdc.MetadataFileHash:       "chunked-hash",
					opencdc.MetadataFileSize:       "12",
					"file_id":                      "chunked-file",
					opencdc.MetadataCollection:     "/test",
				},
			},
		}

		m.On("CreateSession", ctx, []byte("chunk1")).
			Return(&dropbox.SessionResponse{
				SessionID: "test-session",
			}, nil)

		m.On("UploadChunk", ctx, "test-session", []byte("chunk2"), uint(6)).
			Return(nil)

		m.On("CloseSession", ctx, "/test/chunked.txt", "test-session", uint(12)).
			Return(&dropbox.UploadFileResponse{
				ContentHash: "chunked-hash",
				Size:        12,
			}, nil)

		n, err := dest.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, 2)

		m.AssertExpectations(t)
	})

	t.Run("successful file deletion", func(t *testing.T) {
		is := is.New(t)
		records := []opencdc.Record{
			{
				Operation: opencdc.OperationDelete,
				Metadata: map[string]string{
					opencdc.MetadataCollection: "/test",
					opencdc.MetadataFileName:   "file.txt",
				},
			},
		}

		m.On("DeleteFile", ctx, "/test/file.txt").
			Return(nil)

		n, err := dest.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, 1)

		m.AssertExpectations(t)
	})
}
