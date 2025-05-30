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
	"os"
	"testing"
	"time"

	config "github.com/conduitio-labs/conduit-connector-dropbox/config"
	dropbox "github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/testutil"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

const (
	baseTestPath = "/conduit-connector-dropbox-test"
	envToken     = "DROPBOX_TOKEN"
)

func TestSource_ReadN(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	token := os.Getenv(envToken)
	if token == "" {
		t.Skipf("%s environment variable not set, skipping integration test, ", envToken)
	}

	client, err := dropbox.NewHTTPClient(token, 30*time.Second)
	is.NoErr(err)

	testFolderPath := fmt.Sprintf("%s/integration_test/%d", baseTestPath, time.Now().UnixNano())

	// Create test files
	file1Content := []byte("test file 1 content")
	file2Content := bytes.Repeat([]byte("a"), 1500) // 1.5KB file to test chunking
	_, err = client.UploadFile(ctx, testFolderPath+"/file1.txt", file1Content)
	is.NoErr(err)
	_, err = client.UploadFile(ctx, testFolderPath+"/file2.txt", file2Content)
	is.NoErr(err)

	batchSize := 5
	source := &Source{
		config: Config{
			DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
				SourceWithBatch: sdk.SourceWithBatch{
					BatchSize: &batchSize,
				},
			},
			Config: config.Config{
				Token: token,
				Path:  testFolderPath,
			},
			FileChunkSizeBytes: 1024,
		},
	}
	defer func() {
		err = testutil.CleanupTestFolder(ctx, client, testFolderPath)
		if err != nil {
			t.Logf("warning: %v", err)
		}

		err := source.Teardown(ctx)
		is.NoErr(err)
	}()

	err = source.Open(ctx, nil)
	is.NoErr(err)

	// Read first file
	records, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(records), 1)
	is.Equal(records[0].Payload.After.Bytes(), file1Content)
	is.Equal(records[0].Metadata["file_path"], testFolderPath+"/file1.txt")

	// Read second file (should be chunked)
	var chunks []opencdc.Record
	for len(chunks) < 2 {
		records, err := source.ReadN(ctx, 2-len(chunks))
		is.NoErr(err)
		chunks = append(chunks, records...)
	}

	is.Equal(len(chunks), 2) // Expecting 2 chunks for 1.5KB file with 1KB chunks

	// First chunk
	is.Equal(len(chunks[0].Payload.After.Bytes()), 1024)
	is.Equal(chunks[0].Metadata[opencdc.MetadataFileChunked], "true")
	is.Equal(chunks[0].Metadata[opencdc.MetadataFileChunkIndex], "1")
	is.Equal(chunks[0].Metadata[opencdc.MetadataFileChunkCount], "2")
	is.Equal(chunks[0].Metadata["file_path"], testFolderPath+"/file2.txt")

	// Second chunk
	is.Equal(len(chunks[1].Payload.After.Bytes()), 476) // 1500 - 1024
	is.Equal(chunks[1].Metadata[opencdc.MetadataFileChunked], "true")
	is.Equal(chunks[1].Metadata[opencdc.MetadataFileChunkIndex], "2")
	is.Equal(chunks[1].Metadata[opencdc.MetadataFileChunkCount], "2")
	is.Equal(chunks[1].Metadata["file_path"], testFolderPath+"/file2.txt")

	// Combine chunks to verify content matches original
	combined := append(chunks[0].Payload.After.Bytes(), chunks[1].Payload.After.Bytes()...)
	is.Equal(combined, file2Content)
}
