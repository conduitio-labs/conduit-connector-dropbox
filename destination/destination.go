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
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	client dropbox.FoldersClient
	// sessions stores the active sessions for chunk uploads
	session map[string]cursor
}

type cursor struct {
	sessionID string
	offset    uint
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

	if d.client == nil {
		d.client, err = dropbox.NewHTTPClient(d.config.Token, 0)
		if err != nil {
			return fmt.Errorf("error creating dropbox client: %w", err)
		}
	}
	d.session = make(map[string]cursor)
	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		switch {
		case record.Operation == opencdc.OperationDelete:
			err := d.deleteFile(ctx, record)
			if err != nil {
				return i, fmt.Errorf("failed to delete file: %w", err)
			}

		case record.Metadata["is_chunked"] == "true":
			err := d.uploadFileChunk(ctx, record)
			if err != nil {
				return i, fmt.Errorf("failed to upload file chunk: %w", err)
			}

		default:
			err := d.uploadFile(ctx, record)
			if err != nil {
				return i, fmt.Errorf("failed to upload file: %w", err)
			}
		}
	}
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Dropbox destination")
	return nil
}

func (d *Destination) deleteFile(ctx context.Context, r opencdc.Record) error {
	directory, ok := r.Metadata[opencdc.MetadataCollection]
	if !ok {
		return ErrMissingCollection
	}

	filename, ok := r.Metadata["filename"]
	if !ok {
		return ErrMissingFilename
	}

	err := d.client.DeleteFile(ctx, filepath.Join(directory, filename))
	if err != nil {
		return fmt.Errorf("error deleting file: %w", err)
	}

	return nil
}

func (d *Destination) uploadFile(ctx context.Context, r opencdc.Record) error {
	filepath, err := d.getUploadPath(r)
	if err != nil {
		return err
	}

	response, err := d.client.UploadFile(ctx, filepath, r.Payload.After.Bytes())
	if err != nil {
		return fmt.Errorf("error uploading file: %w", err)
	}
	hash := r.Metadata["hash"]
	if response.ContentHash != hash {
		return ErrInvalidHash
	}
	return nil
}

func (d *Destination) uploadFileChunk(ctx context.Context, r opencdc.Record) error {
	metaData, err := d.extractMetadata(r)
	if err != nil {
		return err
	}

	filepath, err := d.getUploadPath(r)
	if err != nil {
		return err
	}

	if metaData.index == 1 {
		response, err := d.client.CreateSession(ctx, r.Payload.After.Bytes())
		if err != nil {
			return fmt.Errorf("error creating upload session: %w", err)
		}
		// start session
		d.session[metaData.hash] = cursor{response.SessionID, uint(len(r.Payload.After.Bytes()))}
		return nil
	}

	sess, ok := d.session[metaData.hash]
	if !ok {
		return ErrInvalidSession
	}
	err = d.client.UploadChunk(ctx, sess.sessionID, r.Payload.After.Bytes(), sess.offset)
	if err != nil {
		return fmt.Errorf("error uploading chunk: %w", err)
	}

	// update session
	val, ok := d.session[metaData.hash]
	if !ok {
		return ErrInvalidSession
	}
	d.session[metaData.hash] = cursor{val.sessionID, val.offset + uint(len(r.Payload.After.Bytes()))}

	if metaData.index == metaData.totalChunks {
		sess = d.session[metaData.hash]
		response, err := d.client.CloseSession(ctx, filepath, sess.sessionID, sess.offset)
		if err != nil {
			return fmt.Errorf("error closing upload session: %w", err)
		}
		if response.ContentHash != metaData.hash || response.Size != metaData.filesize {
			return fmt.Errorf("corrupt file upload: %w", err)
		}
		// close session
		delete(d.session, metaData.hash)
	}

	return nil
}

type metadata struct {
	index       int64
	totalChunks int64
	hash        string
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

	return meta, nil
}

func (d *Destination) getUploadPath(r opencdc.Record) (string, error) {
	directory, ok := r.Metadata[opencdc.MetadataCollection]
	if !ok {
		return "", ErrMissingCollection
	}

	filename, ok := r.Metadata["filename"]
	if !ok {
		return "", ErrMissingFilename
	}

	return filepath.Join(d.config.Path, directory, filename), nil
}
