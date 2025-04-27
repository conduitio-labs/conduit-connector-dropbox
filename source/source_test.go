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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	config "github.com/conduitio-labs/conduit-connector-dropbox/config"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"github.com/stretchr/testify/mock"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	con := &Source{}
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	con.config = Config{
		Config: config.Config{
			Token: "test-token",
		},
	}

	err := con.Open(ctx, nil)
	is.NoErr(err)
}

func TestSource_Open_Failed_VerifyPath(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	m := dropbox.NewMockFoldersClient(t)

	testPath := "/test-path"
	con := &Source{
		client: m,
		config: Config{
			Config: config.Config{
				Token: "test-token",
				Path:  testPath,
			},
		},
	}
	defer is.NoErr(con.Teardown(ctx))

	m.On("VerifyPath", mock.Anything, testPath).
		Return(fmt.Errorf("path/not_found/"))

	err := con.Open(ctx, nil)
	is.Equal(err.Error(), "error verifying remote path: path/not_found/")

	m.AssertExpectations(t)
}

func TestSource_ReadN_Success(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := dropbox.NewMockFoldersClient(t)

	// Setup test files
	testFiles := []string{"/docs/file1.txt", "/docs/file2.txt", "/docs/file3.txt"}
	entries := make([]dropbox.Entry, len(testFiles))

	for i, path := range testFiles {
		entries[i] = dropbox.Entry{
			ID:             fmt.Sprintf("id%d", i+1),
			Name:           fmt.Sprintf("file%d.txt", i+1),
			PathDisplay:    path,
			ServerModified: time.Now(),
			Size:           uint64((i + 1) * 100),
		}
		m.On("DownloadRange", mock.Anything, path, mock.Anything, mock.Anything).
			Return(io.NopCloser(bytes.NewReader([]byte("test content"))), nil)
	}

	m.On("List", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(entries, "cursor", false, nil)
	m.On("Longpoll", mock.Anything, mock.Anything, mock.Anything).
		Return(false, nil)

	src := &Source{
		client: m,
		config: Config{
			Config: config.Config{
				Token: "test-token",
			},
			FileChunkSizeBytes: 1024,
		},
	}
	defer is.NoErr(src.Teardown(ctx))

	is.NoErr(src.Open(ctx, nil))

	var allRecords []opencdc.Record
	for len(allRecords) < 3 {
		records, err := src.ReadN(ctx, 3-len(allRecords))
		is.NoErr(err)
		allRecords = append(allRecords, records...)
	}
	is.Equal(len(allRecords), 3)

	for _, r := range allRecords {
		is.True(len(r.Payload.After.Bytes()) > 0)
		is.True(r.Metadata["file_path"] != "")
	}

	m.AssertExpectations(t)
}

func TestSource_ReadN_ContextCancelled(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())

	src := &Source{
		client: dropbox.NewMockFoldersClient(t),
		config: Config{
			Config: config.Config{
				Token: "test-token",
			},
			FileChunkSizeBytes: 1024,
		},
	}
	defer is.NoErr(src.Teardown(ctx))

	err := src.Open(ctx, nil)
	is.NoErr(err)

	cancel()
	_, err = src.ReadN(ctx, 1)
	is.Equal(err, ctx.Err())
}

func TestSource_ReadN_ChunkedFile(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := dropbox.NewMockFoldersClient(t)

	testFile := dropbox.Entry{
		ID:             "large-file-id",
		Name:           "large.txt",
		PathDisplay:    "/docs/large.txt",
		ServerModified: time.Now(),
		Size:           5000, // 5KB file
	}

	m.On("List", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]dropbox.Entry{testFile}, "cursor", false, nil)
	m.On("Longpoll", mock.Anything, mock.Anything, mock.Anything).
		Return(false, nil)

	// Mock DownloadRange to return chunks
	for i := 0; i < 5; i++ {
		start := i * 1000
		end := start + 1000
		if end > 5000 {
			end = 5000
		}
		chunk := make([]byte, end-start)

		m.On("DownloadRange", mock.Anything, testFile.PathDisplay, uint64(start), uint64(end-start)).
			Return(io.NopCloser(bytes.NewReader(chunk)), nil)
	}

	src := &Source{
		client: m,
		config: Config{
			Config: config.Config{
				Token: "test-token",
			},
			FileChunkSizeBytes: 1000, // 1KB chunks
		},
	}
	defer is.NoErr(src.Teardown(ctx))

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
		is.Equal(chunk.Metadata["file_path"], testFile.PathDisplay)
	}

	m.AssertExpectations(t)
}
