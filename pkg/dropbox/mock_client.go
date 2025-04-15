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
	"io"
	"strings"
	"time"
)

var ErrFileNotFound = fmt.Errorf("file not found")

type MockClient struct {
	Files        map[string][]byte
	Entries      []Entry
	Cursor       string
	LongpollChan chan bool
}

func NewMockClient() *MockClient {
	files := map[string][]byte{
		"/docs/file1.txt": []byte("Hello from Dropbox mock - file1"),
		"/docs/file2.txt": []byte("Another test file from Dropbox mock - file2"),
		"/docs/file3.txt": []byte("Another test file from Dropbox mock - file3"),
	}

	entries := []Entry{
		{
			Tag:            "file",
			ID:             "id:file1",
			Name:           "file1.txt",
			PathDisplay:    "/docs/file1.txt",
			ServerModified: time.Now().Add(-2 * time.Hour),
			Size:           uint64(len(files["/docs/file1.txt"])),
		},
		{
			Tag:            "file",
			ID:             "id:file2",
			Name:           "file2.txt",
			PathDisplay:    "/docs/file2.txt",
			ServerModified: time.Now().Add(-1 * time.Hour),
			Size:           uint64(len(files["/docs/file2.txt"])),
		},
		{
			Tag:            "file",
			ID:             "id:file3",
			Name:           "file3.txt",
			PathDisplay:    "/docs/file3.txt",
			ServerModified: time.Now().Add(-1 * time.Hour),
			Size:           uint64(len(files["/docs/file3.txt"])),
		},
	}

	return &MockClient{
		Files:        files,
		Entries:      entries,
		Cursor:       "mock-cursor-1",
		LongpollChan: make(chan bool, 1),
	}
}

func (m *MockClient) List(_ context.Context, _ string, _ bool) ([]Entry, string, bool, error) {
	return m.Entries[:2], m.Cursor, true, nil
}

func (m *MockClient) ListContinue(_ context.Context, cursor string) ([]Entry, string, bool, error) {
	if cursor != m.Cursor {
		return nil, "", false, ErrExpiredCursor
	}

	return m.Entries[2:], m.Cursor, false, nil
}

func (m *MockClient) Longpoll(ctx context.Context, _ string, timeoutSec int) (bool, error) {
	select {
	case change := <-m.LongpollChan:
		return change, nil
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (m *MockClient) DownloadRange(_ context.Context, path string, start, length uint64) (io.ReadCloser, error) {
	content, ok := m.Files[path]
	if !ok {
		return nil, ErrFileNotFound
	}

	end := start + length
	contentLen := uint64(len(content))
	if end > contentLen || length == 0 {
		end = contentLen
	}

	return io.NopCloser(strings.NewReader(string(content[start:end]))), nil
}
