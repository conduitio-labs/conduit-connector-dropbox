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

package dropbox

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio-labs/conduit-connector-dropbox/source"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	ErrSourceClosed = fmt.Errorf("error source not opened for reading")
	ErrReadingData  = fmt.Errorf("error reading data")
)

type Source struct {
	sdk.UnimplementedSource

	config   SourceConfig
	position *source.Position
	client   dropbox.Client
	ch       chan opencdc.Record
	wg       *sync.WaitGroup
}

type SourceConfig struct {
	sdk.DefaultSourceMiddleware
	Config

	// Interval to poll for changes when longpolling fails.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"10s"`
	// Number of seconds to wait for changes during longpolling.
	LongpollTimeout int `json:"longpollTimeout" default:"30"`
	// Maximum size of a file chunk in bytes to split large files, default is 3MB.
	FileChunkSizeBytes uint64 `json:"fileChunkSizeBytes" default:"3145728"`
	// The maximum number of retries of failed operations.
	Retries int `json:"retries" default:"0"`
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening Dropbox source")

	var err error
	s.position, err = source.ParseSDKPosition(position)
	if err != nil {
		return err
	}

	s.client, err = dropbox.NewDropboxClient(s.config.Token, s.config.LongpollTimeout)
	if err != nil {
		return fmt.Errorf("error creating dropbox client: %w", err)
	}

	s.ch = make(chan opencdc.Record, 1)
	s.wg = &sync.WaitGroup{}

	// Start worker
	s.wg.Add(1)
	go func() {
		source.NewWorker(
			ctx,
			s.client,
			s.config.Path,
			s.config.LongpollTimeout,
			s.config.FileChunkSizeBytes,
			s.position,
			s.ch,
			s.config.Retries,
			s.config.PollingPeriod,
			s.wg,
		).Start()
	}()

	return nil
}

func (s *Source) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if s.ch == nil {
		return nil, ErrSourceClosed
	}

	records := make([]opencdc.Record, 0, n)
	for len(records) < n {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case record, ok := <-s.ch:
			if ok {
				fmt.Println("got record --------- ", record.Metadata)
			}
			if !ok {
				if len(records) == 0 {
					return nil, ErrReadingData
				}
				return records, nil
			}
			records = append(records, record)
		}
	}

	return records, nil
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Trace().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Dropbox source")
	if s.wg != nil {
		s.wg.Wait()
	}
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
	return nil
}
