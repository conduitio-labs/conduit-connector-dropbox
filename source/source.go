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
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-dropbox/config"
	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	ErrSourceClosed = fmt.Errorf("error source not opened for reading")
	ErrReadingData  = fmt.Errorf("error reading data")
)

type Source struct {
	sdk.UnimplementedSource

	config   Config
	position *Position
	client   dropbox.Client
	ch       chan opencdc.Record
	wg       *sync.WaitGroup
}

type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config

	// Timeout (in seconds) for Dropbox longpolling requests.
	LongpollTimeout int `json:"longpollTimeout" default:"30"`
	// Size of a file chunk in bytes to split large files, maximum is 4MB.
	FileChunkSizeBytes uint64 `json:"fileChunkSizeBytes" default:"3145728"`
	// Maximum number of retry attempts.
	Retries int `json:"retries" default:"0"`
	// Delay between retry attempts.
	RetryDelay time.Duration `json:"retryDelay" default:"10s"`
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
	s.position, err = ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("error parsing sdk position: %w", err)
	}

	if s.client == nil {
		s.client, err = dropbox.NewHTTPClient(s.config.Token, s.config.LongpollTimeout)
		if err != nil {
			return fmt.Errorf("error creating http client for dropbox: %w", err)
		}
	}

	s.ch = make(chan opencdc.Record, 1)
	s.wg = &sync.WaitGroup{}

	// Start worker
	s.wg.Add(1)
	go func() {
		NewWorker(
			s.client,
			s.config,
			s.position,
			s.ch,
			s.wg,
		).Start(ctx)
	}()

	return nil
}

func (s *Source) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if s.ch == nil {
		return nil, ErrSourceClosed
	}

	records := make([]opencdc.Record, 0, n)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r, ok := <-s.ch:
		if !ok {
			return nil, ErrReadingData
		}
		records = append(records, r)
	}

	for len(records) < n {
		select {
		case r, ok := <-s.ch:
			if !ok {
				break
			}
			records = append(records, r)
		default:
			return records, nil
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
