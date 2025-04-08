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
	"io"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Worker struct {
	ctx                context.Context
	client             dropbox.Client
	path               string
	longpollTimeout    int
	fileChunkSizeBytes uint64
	position           *Position
	recordsCh          chan<- opencdc.Record
	currentCursor      string
	retries            int
	pollingPeriod      time.Duration
	wg                 *sync.WaitGroup
}

func NewWorker(
	ctx context.Context,
	client dropbox.Client,
	path string,
	longpollTimeout int,
	fileChunkSizeBytes uint64,
	position *Position,
	recordsCh chan<- opencdc.Record,
	retries int,
	pollingPeriod time.Duration,
	wg *sync.WaitGroup,
) *Worker {
	return &Worker{
		ctx:                ctx,
		client:             client,
		path:               path,
		longpollTimeout:    longpollTimeout,
		fileChunkSizeBytes: fileChunkSizeBytes,
		position:           position,
		recordsCh:          recordsCh,
		retries:            retries,
		pollingPeriod:      pollingPeriod,
		wg:                 wg,
	}
}

func (w *Worker) Start() {
	defer w.wg.Done()
	retries := w.retries

	for {
		err := w.processChanges()
		if err != nil {
			select {
			case <-w.ctx.Done():
				sdk.Logger(w.ctx).Debug().Msg("worker shutting down...")
				return
			case <-time.After(w.pollingPeriod):
				if retries == 0 {
					sdk.Logger(w.ctx).Err(err).Msg("retries exhausted, worker shutting down...")
					return
				}
				retries--
				sdk.Logger(w.ctx).Warn().Err(err).Msgf("retrying... (%d attempts left)", retries)
				continue
			}
		}
		// Reset retries on successful operation
		retries = w.retries
	}
}

func (w *Worker) processChanges() error {
	// Initialize cursor if missing
	if w.currentCursor == "" {
		return w.initialSync()
	}

	// Use longpoll for change detection
	hasChanges, newCursor, err := w.client.ListFolderLongpoll(w.ctx, w.currentCursor, w.longpollTimeout)
	if err != nil {
		return fmt.Errorf("longpoll failed: %w", err)
	}

	if !hasChanges {
		return nil
	}

	// Get the actual changes
	changes, newCursor, hasMore, err := w.client.ListFolderContinue(w.ctx, w.currentCursor)
	if err != nil {
		return fmt.Errorf("list continue failed: %w", err)
	}

	w.currentCursor = newCursor
	w.position.Cursor = newCursor

	// Process changes
	for _, change := range changes {
		if err := w.processChange(change); err != nil {
			return fmt.Errorf("process change failed: %w", err)
		}
	}

	// Handle pagination
	for hasMore {
		changes, newCursor, hasMore, err = w.client.ListFolderContinue(w.ctx, w.currentCursor)
		if err != nil {
			return fmt.Errorf("pagination failed: %w", err)
		}

		w.currentCursor = newCursor
		w.position.Cursor = newCursor

		for _, change := range changes {
			if err := w.processChange(change); err != nil {
				return fmt.Errorf("process change failed: %w", err)
			}
		}
	}

	return nil
}

func (w *Worker) initialSync() error {
	entries, cursor, err := w.client.ListFolder(w.ctx, w.path, false)
	if err != nil {
		return fmt.Errorf("initial list failed: %w", err)
	}

	w.currentCursor = cursor
	w.position.Cursor = cursor

	for _, entry := range entries {
		if entry.Tag == "file" {
			if err := w.processFile(entry); err != nil {
				return fmt.Errorf("process file failed: %w", err)
			}
		}
	}
	return nil
}

func (w *Worker) processChange(change dropbox.Change) error {
	if change.Type == "removed" {
		return nil // Skip deleted files for now
	}
	return w.processFile(change.Entry)
}

func (w *Worker) processFile(entry dropbox.Entry) error {
	if entry.Size > w.fileChunkSizeBytes {
		return w.processChunkedFile(entry)
	}
	return w.processFullFile(entry)
}

func (w *Worker) processChunkedFile(entry dropbox.Entry) error {
	totalChunks := (entry.Size + w.fileChunkSizeBytes - 1) / w.fileChunkSizeBytes

	// Continue from last chunk if resuming
	startChunk := 0
	if w.position.ChunkInfo != nil && w.position.ChunkInfo.FileID == entry.ID {
		startChunk = w.position.ChunkInfo.ChunkIndex + 1
	}

	for chunkIdx := startChunk; chunkIdx < int(totalChunks); chunkIdx++ {
		start := uint64(chunkIdx) * w.fileChunkSizeBytes
		end := start + w.fileChunkSizeBytes
		if end > entry.Size {
			end = entry.Size
		}

		chunkData, err := w.downloadChunk(entry.PathDisplay, start, end-start)
		if err != nil {
			return fmt.Errorf("download chunk failed: %w", err)
		}

		record, err := w.createChunkRecord(entry, chunkIdx, int(totalChunks), chunkData)
		if err != nil {
			return fmt.Errorf("create record failed: %w", err)
		}

		select {
		case w.recordsCh <- record:
			// Update position after successful send
			w.position.updateFile(entry.ID, entry.PathDisplay)
			w.position.updateChunk(entry.ID, entry.PathDisplay, chunkIdx, int(totalChunks))
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	return nil
}

func (w *Worker) processFullFile(entry dropbox.Entry) error {
	fileData, err := w.downloadChunk(entry.PathDisplay, 0, entry.Size)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	record, err := w.createFullFileRecord(entry, fileData)
	if err != nil {
		return fmt.Errorf("create record failed: %w", err)
	}

	select {
	case w.recordsCh <- record:
		w.position.updateFile(entry.ID, entry.PathDisplay)
	case <-w.ctx.Done():
		return w.ctx.Err()
	}

	return nil
}

func (w *Worker) downloadChunk(path string, start, length uint64) ([]byte, error) {
	reader, err := w.client.DownloadRange(w.ctx, path, start, length)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (w *Worker) createChunkRecord(entry dropbox.Entry, chunkIdx, totalChunks int, data []byte) (opencdc.Record, error) {
	position, err := w.position.marshal()
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
		position,
		metadata,
		opencdc.StructuredData{"file_id": entry.ID},
		opencdc.RawData(data),
	), nil
}

func (w *Worker) createFullFileRecord(entry dropbox.Entry, data []byte) (opencdc.Record, error) {
	position, err := w.position.marshal()
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
		opencdc.StructuredData{"file_id": entry.ID},
		opencdc.RawData(data),
	), nil
}
