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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	client dropbox.Client
	// sessions stores the active sessions for chunk uploads
	session *Session
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening Dropbox destination")
	var err error
	d.client, err = dropbox.NewHTTPClient(d.config.Token, 0)
	if err != nil {
		return fmt.Errorf("error creating dropbox client: %w", err)
	}
	d.session = NewSession()
	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		switch {
		case record.Operation == opencdc.OperationDelete:
			filepath, ok := record.Metadata["file_path"]
			if !ok {
				return i, ErrMissingFilePath
			}
			err := d.client.DeleteFile(ctx, filepath)
			if err != nil {
				return i, fmt.Errorf("error deleting file: %w", err)
			}

		default:
			chunked, ok := record.Metadata["is_chunked"]
			if ok && chunked == "true" {
				err := d.uploadChunkedRecord(ctx, record)
				if err != nil {
					return i, err
				}
				continue
			}

			err := d.uploadFile(ctx, record)
			if err != nil {
				return i, err
			}
		}
	}
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Dropbox destination")
	return nil
}

func (d *Destination) uploadFile(ctx context.Context, r opencdc.Record) error {
	var filepath string
	if d.config.Path == "" {
		var ok bool
		filepath, ok = r.Metadata["file_path"]
		if !ok {
			return ErrMissingUploadDirectory
		}
	}

	response, err := d.client.UploadFile(ctx, filepath, r.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("error uploading file: %w", err)
	}
	hash, ok := r.Metadata["hash"]
	if !ok || response.ContentHash != hash {
		return ErrInvalidHash
	}
	return nil
}

func (d *Destination) uploadChunkedRecord(ctx context.Context, r opencdc.Record) error {
	metaData, err := d.extractMetadata(r)
	if err != nil {
		return err
	}

	if metaData.index == 1 {
		response, err := d.client.CreateSession(ctx, r.Payload.After.Bytes())
		if err != nil {
			return fmt.Errorf("error creating upload session: %w", err)
		}
		d.session.startSession(metaData.fileID, response.SessionID, uint(len(r.Payload.After.Bytes())))
		return nil
	}

	sess, ok := d.session.getSession(metaData.fileID)
	if !ok {
		return ErrInvalidSession
	}
	err = d.client.UploadChunk(ctx, sess.sessionID, r.Payload.After.Bytes(), sess.offset)
	if err != nil {
		return fmt.Errorf("error uploading chunk: %w", err)
	}
	err = d.session.updateSession(metaData.fileID, uint(len(r.Payload.After.Bytes())))
	if err != nil {
		return err
	}

	if metaData.index == metaData.totalChunks {
		sess, _ := d.session.getSession(metaData.fileID)
		response, err := d.client.CloseSession(ctx, metaData.filepath, sess.sessionID, sess.offset)
		if err != nil {
			return fmt.Errorf("error closing upload session: %w", err)
		}
		if response.ContentHash != metaData.hash || response.Size != metaData.filesize {
			return fmt.Errorf("corrupt file upload: %w", err)
		}
		d.session.closeSession(metaData.fileID)
	}

	return nil
}

type metadata struct {
	index       int64
	totalChunks int64
	hash        string
	fileID      string
	filename    string
	filepath    string
	filesize    uint64
}

func (d *Destination) extractMetadata(record opencdc.Record) (metadata, error) {
	meta := metadata{}
	var ok bool
	chunked, ok := record.Metadata["is_chunked"]
	if ok && chunked == "true" {
		chunkIndex, ok := record.Metadata["chunk_index"]
		if !ok {
			return metadata{}, NewInvalidChunkError("chunk_index not found")
		}
		var err error
		meta.index, err = strconv.ParseInt(chunkIndex, 10, 64)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse chunk_index: %w", err)
		}
		total, ok := record.Metadata["total_chunks"]
		if !ok {
			return metadata{}, NewInvalidChunkError("total_chunk not found")
		}
		meta.totalChunks, err = strconv.ParseInt(total, 10, 64)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse total_chunks: %w", err)
		}
	}

	meta.filename, ok = record.Metadata["filename"]
	if !ok {
		structuredKey, err := d.structurizeData(record.Key)
		if err != nil {
			meta.filename = string(record.Key.Bytes())
		} else {
			name, ok := structuredKey["filename"].(string)
			if !ok {
				return metadata{}, NewInvalidChunkError("invalid filename")
			}
			meta.filename = name
		}
	}

	meta.hash, ok = record.Metadata["hash"]
	if !ok {
		return metadata{}, NewInvalidChunkError("hash not found")
	}
	fileSize, ok := record.Metadata["file_size"]
	if !ok {
		return metadata{}, NewInvalidChunkError("file_size not found")
	}
	var err error
	meta.filesize, err = strconv.ParseUint(fileSize, 10, 64)
	if err != nil {
		return metadata{}, fmt.Errorf("failed to parse file_size: %w", err)
	}
	meta.filepath, ok = record.Metadata["file_path"]
	if !ok {
		return metadata{}, NewInvalidChunkError("file_path not found")
	}
	meta.fileID, ok = record.Metadata["file_id"]
	if !ok {
		return metadata{}, NewInvalidChunkError("file_id not found")
	}

	return meta, nil
}

func (d *Destination) structurizeData(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return opencdc.StructuredData{}, nil
	}

	structuredData := make(opencdc.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}
