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
	"time"
)

type FoldersClient interface {
	// List returns metadata for all files/folders in path
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder
	List(ctx context.Context, path string, recursive bool, limit int) ([]Entry, string, bool, error)

	// ListContinue retrieves additional results from a previous List call
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-continue
	ListContinue(ctx context.Context, cursor string) ([]Entry, string, bool, error)

	// Longpoll checks for changes to folder contents (blocks until timeout or changes)
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-longpoll
	Longpoll(ctx context.Context, cursor string, timeout time.Duration) (bool, error)

	// DownloadRange retrieves specific bytes from a file (supports partial downloads)
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-download
	DownloadRange(ctx context.Context, path string, start, length uint64) (io.ReadCloser, error)

	// VerifyPath validates if path exists and is accessible
	// Docs: https://www.dropbox.com/developers/documentation/http/documentation#files-get_metadata
	VerifyPath(ctx context.Context, path string) (bool, error)
}
