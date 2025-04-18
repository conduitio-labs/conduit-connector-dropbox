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
	"io"
)

type Client interface {
	List(ctx context.Context, path string, recursive bool) ([]Entry, string, bool, error)
	ListContinue(ctx context.Context, cursor string) ([]Entry, string, bool, error)
	Longpoll(ctx context.Context, cursor string, timeoutSec int) (bool, error)
	DownloadRange(ctx context.Context, path string, start, length uint64) (io.ReadCloser, error)
}
