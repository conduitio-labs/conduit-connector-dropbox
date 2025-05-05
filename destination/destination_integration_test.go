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
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	config "github.com/conduitio-labs/conduit-connector-dropbox/config"
	dropbox "github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/testutil"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

const (
	testFolderPath = "/conduit-connector-dropbox-test"
	envToken       = "DROPBOX_TOKEN"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	token := os.Getenv(envToken)
	if token == "" {
		t.Skipf("%s environment variable not set, skipping integration test, ", envToken)
	}

	client, err := dropbox.NewHTTPClient(token, 30*time.Second)
	is.NoErr(err)

	// Cleanup before starting
	testutil.CleanupTestFiles(ctx, t, client, testFolderPath)

	dest := &Destination{
		config: Config{
			Config: config.Config{
				Token: token,
				Path:  testFolderPath,
			},
		},
		client: client,
	}
	defer func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	}()

	err = dest.Open(ctx)
	is.NoErr(err)

	t.Run("UploadSingleFile", func(t *testing.T) {
		is := is.New(t)

		fileContent := []byte("test file content")
		record := opencdc.Record{
			Operation: opencdc.OperationCreate,
			Payload: opencdc.Change{
				After: opencdc.RawData(fileContent),
			},
			Metadata: map[string]string{
				opencdc.MetadataCollection: "/uploads",
				"filename":                 "single_file.txt",
				"hash":                     "d8a411e8f8643821bed189e627ff57151918aa554c00c10b31c693ab2dded273",
			},
		}

		n, err := dest.Write(ctx, []opencdc.Record{record})
		is.NoErr(err)
		is.Equal(n, 1)

		// Verify file was uploaded
		reader, err := client.DownloadRange(ctx, testFolderPath+"/uploads/single_file.txt", 0, 0)
		is.NoErr(err)
		defer reader.Close()

		data, err := io.ReadAll(reader)
		is.NoErr(err)
		is.Equal(data, fileContent)
	})

	t.Run("UploadChunkedFile", func(t *testing.T) {
		is := is.New(t)

		// Create large file content (3 chunks with 1KB chunks)
		chunkSize := 1024
		fileContent := bytes.Repeat([]byte("a"), 3*chunkSize)
		totalChunks := 3

		for i := 1; i <= totalChunks; i++ {
			start := (i - 1) * chunkSize
			end := i * chunkSize
			chunk := fileContent[start:end]

			record := opencdc.Record{
				Operation: opencdc.OperationCreate,
				Payload: opencdc.Change{
					After: opencdc.RawData(chunk),
				},
				Metadata: map[string]string{
					opencdc.MetadataCollection: "/uploads",
					"filename":                 "chunked_file.txt",
					"hash":                     "2fc8ddec4e71269c71f2fb08b062172bfa4699b979c7a16d2a8dcc0dab488214",
					"is_chunked":               "true",
					"chunk_index":              strconv.Itoa(i),
					"total_chunks":             strconv.Itoa(totalChunks),
					"file_size":                strconv.Itoa(len(fileContent)),
				},
			}

			n, err := dest.Write(ctx, []opencdc.Record{record})
			is.NoErr(err)
			is.Equal(n, 1)
		}

		// Verify complete file was uploaded
		reader, err := client.DownloadRange(ctx, testFolderPath+"/uploads/chunked_file.txt", 0, 0)
		is.NoErr(err)
		defer reader.Close()

		data, err := io.ReadAll(reader)
		is.NoErr(err)
		is.Equal(data, fileContent)
	})

	t.Run("DeleteFile", func(t *testing.T) {
		is := is.New(t)

		// Upload a file to delete
		fileContent := []byte("file to delete")
		filePath := testFolderPath + "/uploads/to_delete.txt"
		_, err := client.UploadFile(ctx, filePath, fileContent)
		is.NoErr(err)

		// Verify file exists
		_, err = client.DownloadRange(ctx, filePath, 0, 0)
		is.NoErr(err)

		// Delete the file
		record := opencdc.Record{
			Operation: opencdc.OperationDelete,
			Metadata: map[string]string{
				opencdc.MetadataCollection: testFolderPath + "/uploads",
				"filename":                 "to_delete.txt",
			},
		}

		n, err := dest.Write(ctx, []opencdc.Record{record})
		is.NoErr(err)
		is.Equal(n, 1)

		// Verify file was deleted
		_, err = client.DownloadRange(ctx, filePath, 0, 0)
		is.True(err != nil) // Expect error since file should be gone
	})

	t.Run("InvalidChunkMetadata", func(t *testing.T) {
		is := is.New(t)

		record := opencdc.Record{
			Operation: opencdc.OperationCreate,
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte("chunk")),
			},
			Metadata: map[string]string{
				"is_chunked":  "true",
				"chunk_index": "1",
				// Missing total_chunks and other required fields
			},
		}

		_, err := dest.Write(ctx, []opencdc.Record{record})
		is.True(err != nil)
	})
}
