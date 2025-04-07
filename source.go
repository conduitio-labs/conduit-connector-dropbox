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
	cancel   context.CancelFunc
}

type SourceConfig struct {
	sdk.DefaultSourceMiddleware
	Config

	Path            string        `json:"path" default:"/"`
	PollingPeriod   time.Duration `json:"pollingPeriod" default:"10s"`
	LongpollTimeout int           `json:"longpollTimeout" default:"30"`
	MaxRetries      int           `json:"maxRetries" default:"5"`
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

	s.client, err = dropbox.NewDropboxClient(s.config.Token)
	if err != nil {
		return fmt.Errorf("error creating dropbox client: %w", err)
	}

	s.ch = make(chan opencdc.Record, 1)
	s.wg = &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go source.NewWorker(s.client, s.config.Path, s.position, s.ch, s.config.LongpollTimeout, s.config.PollingPeriod, s.config.MaxRetries, s.wg).Start(ctx)

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
	if s.cancel != nil {
		s.cancel()
	}
	if s.wg != nil {
		s.wg.Wait()
	}
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
	return nil
}
