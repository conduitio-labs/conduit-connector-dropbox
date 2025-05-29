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
	"time"

	"github.com/conduitio-labs/conduit-connector-dropbox/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var ErrInvalidBatchSize = fmt.Errorf("batch size is out of allowed range [1-2000]")

type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config

	// Timeout for Dropbox longpolling requests.
	LongpollTimeout time.Duration `json:"longpollTimeout" default:"30s"`
	// Size of a file chunk in bytes to split large files, maximum is 4MB.
	FileChunkSizeBytes uint64 `json:"fileChunkSizeBytes" default:"4194304"`
	// Maximum number of retry attempts.
	Retries int `json:"retries" default:"0"`
	// Delay between retry attempts.
	RetryDelay time.Duration `json:"retryDelay" default:"10s"`
}

// Validate checks if the configuration values are within allowed Dropbox limits.
func (c *Config) Validate(_ context.Context) error {
	// c.BatchSize must be 1-2000 per Dropbox list_folder API requirements.
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder
	if c.BatchSize == nil || *c.BatchSize < 1 || *c.BatchSize > 2000 {
		return ErrInvalidBatchSize
	}

	return nil
}
