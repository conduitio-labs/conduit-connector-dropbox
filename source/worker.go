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
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Worker struct {
	client          dropbox.Client
	path            string
	position        *Position
	ch              chan<- opencdc.Record
	longpollTimeout int
	pollingPeriod   time.Duration
	maxRetries      int
	wg              *sync.WaitGroup
}

func NewWorker(
	client dropbox.Client,
	path string,
	position *Position,
	ch chan<- opencdc.Record,
	longpollTimeout int,
	pollingPeriod time.Duration,
	maxRetries int,
	wg *sync.WaitGroup,
) *Worker {
	return &Worker{
		client:          client,
		path:            path,
		position:        position,
		ch:              ch,
		longpollTimeout: longpollTimeout,
		pollingPeriod:   pollingPeriod,
		maxRetries:      maxRetries,
		wg:              wg,
	}
}

func (w *Worker) Start(ctx context.Context) {
	defer w.wg.Done()

	retries := w.maxRetries

	for {
		err := w.process(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("worker shutting down...")
				return
			case <-time.After(w.pollingPeriod):
				if retries == 0 {
					sdk.Logger(ctx).Err(err).Msg("retries exhausted, worker shutting down...")
					return
				}
				retries--
			}
		} else {
			retries = w.maxRetries
		}
	}
}

func (w *Worker) process(ctx context.Context) error {
	if w.position.Cursor != "" {
		return w.processChanges(ctx)
	}
	return w.processInitial(ctx)
}

func (w *Worker) processInitial(ctx context.Context) error {
	entries, cursor, err := w.client.ListFolder(ctx, w.path, false)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := w.handleEntry(entry); err != nil {
			return err
		}
	}
	w.position.updateCursor(cursor)
	return nil
}

func (w *Worker) processChanges(ctx context.Context) error {
	changes, cursor, hasMore, err := w.client.ListFolderContinue(ctx, w.position.Cursor)
	if err != nil {
		return err
	}
	for _, change := range changes {
		if err := w.handleEntry(change.Entry); err != nil {
			return err
		}
	}
	w.position.updateCursor(cursor)
	if !hasMore {
		time.Sleep(w.pollingPeriod)
	}
	return nil
}

func (w *Worker) handleEntry(entry dropbox.Entry) error {
	w.position.updateCursor(entry.ID)

	sdkPosition, err := w.position.marshal()
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"path": entry.PathDisplay,
		"type": entry.Tag,
	}

	key := opencdc.StructuredData{
		"id":   entry.ID,
		"path": entry.PathDisplay,
	}

	payload := opencdc.StructuredData{
		"id":              entry.ID,
		"name":            entry.Name,
		"path_display":    entry.PathDisplay,
		"client_modified": entry.ClientModified,
		"server_modified": entry.ServerModified,
		"size":            entry.Size,
		"content_hash":    entry.ContentHash,
	}

	record := sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, payload)
	w.ch <- record
	return nil
}
