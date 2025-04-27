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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	tagFolder  = "folder"
	tagDeleted = "deleted"
)

type Worker struct {
	client    dropbox.FoldersClient
	config    Config
	recordsCh chan<- opencdc.Record
	wg        *sync.WaitGroup

	cursor            string
	currentChunkInfo  *ChunkInfo
	lastProcessedTime int64
}

// NewWorker creates a new worker instance with the given configuration.
func NewWorker(
	client dropbox.FoldersClient,
	config Config,
	position *Position,
	recordsCh chan<- opencdc.Record,
	wg *sync.WaitGroup,
) *Worker {
	return &Worker{
		client:            client,
		config:            config,
		recordsCh:         recordsCh,
		wg:                wg,
		cursor:            position.Cursor,
		currentChunkInfo:  position.ChunkInfo,
		lastProcessedTime: position.LastProcessedUnixTime,
	}
}

// Start begins processing files and handles retries on errors.
func (w *Worker) Start(ctx context.Context) {
	defer w.wg.Done()
	retries := w.config.Retries

	for {
		select {
		case <-ctx.Done():
			sdk.Logger(ctx).Debug().Msg("context canceled, worker shutting down...")
			return
		default:
		}

		err := w.process(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("context canceled, worker shutting down...")
				return
			case <-time.After(w.config.RetryDelay):
				if retries == 0 {
					sdk.Logger(ctx).Err(err).Msg("retries exhausted, worker shutting down...")
					return
				}
				retries--
				sdk.Logger(ctx).Warn().Err(err).Msgf("retrying... (%d attempts left)", retries)
				continue
			}
		}
		// Reset retries on successful operation
		retries = w.config.Retries
	}
}

// process handles the main workflow of checking and processing changes.
func (w *Worker) process(ctx context.Context) error {
	if w.cursor == "" {
		return w.initialSync(ctx)
	}

	// Using longpoll for change detection
	hasChanges, err := w.client.Longpoll(ctx, w.cursor, w.config.LongpollTimeout)
	if err != nil {
		return w.handleCursorError(ctx, "longpoll", err)
	}

	if !hasChanges {
		return nil
	}

	entries, cursor, hasMore, err := w.client.ListContinue(ctx, w.cursor)
	if err != nil {
		return w.handleCursorError(ctx, "list continue", err)
	}
	w.cursor = cursor

	for _, entry := range entries {
		if err := w.processFile(ctx, entry); err != nil {
			return fmt.Errorf("process file failed: %w", err)
		}
	}

	return w.handlePagination(ctx, hasMore)
}

// initialSync performs the first complete sync of the target path.
func (w *Worker) initialSync(ctx context.Context) error {
	entries, cursor, hasMore, err := w.client.List(ctx, w.config.Path, false, w.config.Limit)
	if err != nil {
		return fmt.Errorf("list failed: %w", err)
	}
	w.cursor = cursor

	for _, entry := range entries {
		if err := w.processFile(ctx, entry); err != nil {
			return fmt.Errorf("process file failed: %w", err)
		}
	}

	return w.handlePagination(ctx, hasMore)
}

// reSync performs a full resync when the cursor expires.
func (w *Worker) reSync(ctx context.Context) error {
	entries, cursor, hasMore, err := w.client.List(ctx, w.config.Path, false, w.config.Limit)
	if err != nil {
		return fmt.Errorf("resync list failed: %w", err)
	}
	w.cursor = cursor

	for _, entry := range entries {
		// Skip already processed files.
		if entry.ServerModified.UnixNano() < w.lastProcessedTime {
			continue
		}
		if err := w.processFile(ctx, entry); err != nil {
			return fmt.Errorf("process file failed: %w", err)
		}
	}

	return w.handlePagination(ctx, hasMore)
}

// handlePagination manages continuation of large result sets.
func (w *Worker) handlePagination(ctx context.Context, hasMore bool) error {
	var entries []dropbox.Entry
	var cursor string
	var err error

	for hasMore {
		entries, cursor, hasMore, err = w.client.ListContinue(ctx, w.cursor)
		if err != nil {
			return w.handleCursorError(ctx, "list continue", err)
		}
		w.cursor = cursor

		for _, entry := range entries {
			if err := w.processFile(ctx, entry); err != nil {
				return fmt.Errorf("process file failed: %w", err)
			}
		}
	}

	return nil
}

// processFile routes file processing based on entry type.
func (w *Worker) processFile(ctx context.Context, entry dropbox.Entry) error {
	if entry.Tag == tagFolder {
		return nil
	}

	if entry.Tag == tagDeleted {
		return w.processDeletedFile(ctx, entry)
	}

	if entry.Size > w.config.FileChunkSizeBytes {
		return w.processChunkedFile(ctx, entry)
	}
	return w.processFullFile(ctx, entry)
}

// processDeletedFile handles deletion records.
func (w *Worker) processDeletedFile(ctx context.Context, entry dropbox.Entry) error {
	w.currentChunkInfo = nil
	position, err := ToSDKPosition(w.cursor, nil, w.lastProcessedTime)
	if err != nil {
		return fmt.Errorf("marshal position: %w", err)
	}

	metadata := opencdc.Metadata{
		"filename":  entry.Name,
		"file_path": entry.PathDisplay,
	}

	record := sdk.Util.Source.NewRecordDelete(
		position,
		metadata,
		opencdc.StructuredData{"file_path": entry.PathDisplay},
		nil,
	)

	select {
	case w.recordsCh <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processChunkedFile processes large files in chunks.
func (w *Worker) processChunkedFile(ctx context.Context, entry dropbox.Entry) error {
	totalChunks := (entry.Size + w.config.FileChunkSizeBytes - 1) / w.config.FileChunkSizeBytes

	var startChunk uint64 = 1
	if w.currentChunkInfo != nil && w.currentChunkInfo.FileID == entry.ID {
		startChunk = w.currentChunkInfo.ChunkIndex + 1
	}

	for chunkIdx := startChunk; chunkIdx <= totalChunks; chunkIdx++ {
		start := (chunkIdx - 1) * w.config.FileChunkSizeBytes
		end := min(start+w.config.FileChunkSizeBytes, entry.Size)

		chunkData, err := w.downloadChunk(ctx, entry.PathDisplay, start, end-start)
		if err != nil {
			return fmt.Errorf("download chunk %d failed: %w", chunkIdx, err)
		}

		record, err := w.createChunkedRecord(entry, chunkIdx, totalChunks, chunkData)
		if err != nil {
			return fmt.Errorf("create record failed: %w", err)
		}

		select {
		case w.recordsCh <- record:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// processFullFile processes small files in one operation.
func (w *Worker) processFullFile(ctx context.Context, entry dropbox.Entry) error {
	fileData, err := w.downloadChunk(ctx, entry.PathDisplay, 0, entry.Size)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	record, err := w.createRecord(entry, fileData)
	if err != nil {
		return fmt.Errorf("create record failed: %w", err)
	}

	select {
	case w.recordsCh <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// downloadChunk downloads a specific byte range of a file.
func (w *Worker) downloadChunk(ctx context.Context, path string, start, length uint64) ([]byte, error) {
	reader, err := w.client.DownloadRange(ctx, path, start, length)
	if err != nil {
		return nil, fmt.Errorf("download file %q: %w", path, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read file %q: %w", path, err)
	}

	return data, nil
}

// createChunkedRecord creates a record for a file chunk.
func (w *Worker) createChunkedRecord(entry dropbox.Entry, chunkIdx, totalChunks uint64, data []byte) (opencdc.Record, error) {
	var chunkInfo *ChunkInfo

	if chunkIdx == totalChunks {
		w.currentChunkInfo = nil
		w.lastProcessedTime = entry.ServerModified.UnixNano()
	} else {
		chunkInfo = &ChunkInfo{
			FileID:      entry.ID,
			FilePath:    entry.PathDisplay,
			ChunkIndex:  chunkIdx,
			TotalChunks: totalChunks,
		}
		w.currentChunkInfo = chunkInfo
	}

	sdkPosition, err := ToSDKPosition(w.cursor, chunkInfo, w.lastProcessedTime)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	metadata := opencdc.Metadata{
		"filename":     entry.Name,
		"file_id":      entry.ID,
		"file_path":    entry.PathDisplay,
		"file_size":    fmt.Sprintf("%d", entry.Size),
		"hash":         entry.ContentHash,
		"chunk_index":  fmt.Sprintf("%d", chunkIdx),
		"total_chunks": fmt.Sprintf("%d", totalChunks),
		"is_chunked":   "true",
	}

	return sdk.Util.Source.NewRecordCreate(
		sdkPosition,
		metadata,
		opencdc.StructuredData{"file_path": entry.PathDisplay},
		opencdc.RawData(data),
	), nil
}

// createRecord creates a record for a complete file.
func (w *Worker) createRecord(entry dropbox.Entry, data []byte) (opencdc.Record, error) {
	w.currentChunkInfo = nil
	w.lastProcessedTime = entry.ServerModified.UnixNano()

	position, err := ToSDKPosition(w.cursor, nil, w.lastProcessedTime)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	metadata := opencdc.Metadata{
		"filename":  entry.Name,
		"file_id":   entry.ID,
		"file_path": entry.PathDisplay,
		"file_size": fmt.Sprintf("%d", entry.Size),
		"hash":      entry.ContentHash,
	}

	return sdk.Util.Source.NewRecordCreate(
		position,
		metadata,
		opencdc.StructuredData{"file_path": entry.PathDisplay},
		opencdc.RawData(data),
	), nil
}

// handleCursorError handles cursor reset/expiry error.
func (w *Worker) handleCursorError(ctx context.Context, op string, err error) error {
	if errors.Is(err, dropbox.ErrExpiredCursor) {
		sdk.Logger(ctx).Warn().Msgf("cursor expired during %s, falling back to re-sync", op)
		return w.reSync(ctx)
	}
	return fmt.Errorf("%s failed: %w", op, err)
}
