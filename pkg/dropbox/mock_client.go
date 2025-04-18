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
	"sync"
	"time"
)

var (
	ErrFileNotFound    = fmt.Errorf("file not found")
	ErrSessionNotFound = fmt.Errorf("session not found")
	ErrInvalidOffset   = fmt.Errorf("invalid offset")
)

type MockClient struct {
	files   map[string][]byte
	entries []Entry
	cursor  string

	// For tracking upload sessions
	sessions   map[string]*mockSession
	sessionsMu sync.Mutex
}

type mockSession struct {
	sessionID string
	content   []byte
	offset    uint
}

func NewMockClient() *MockClient {
	mock := &MockClient{
		cursor:   "mock-cursor-1",
		sessions: make(map[string]*mockSession),
	}

	mock.SetFiles(map[string][]byte{
		"/docs/file1.txt": []byte("Hello from Dropbox mock - file1"),
		"/docs/file2.txt": []byte("Another test file from Dropbox mock - file2"),
		"/docs/file3.txt": []byte("Another test file from Dropbox mock - file3"),
	})

	return mock
}

// SetFiles sets the mock files and automatically creates corresponding entries.
func (m *MockClient) SetFiles(files map[string][]byte) {
	m.files = files

	m.entries = make([]Entry, 0, len(files))
	for path, content := range files {
		m.entries = append(m.entries, Entry{
			Tag:            "file",
			ID:             "id:" + path,
			Name:           path[strings.LastIndex(path, "/")+1:],
			PathDisplay:    path,
			ServerModified: time.Now(),
			Size:           uint64(len(content)),
		})
	}
}

func (m *MockClient) List(_ context.Context, _ string, _ bool) ([]Entry, string, bool, error) {
	if len(m.entries) <= 1 {
		return m.entries, m.cursor, false, nil
	}

	return m.entries[:1], m.cursor, true, nil
}

func (m *MockClient) ListContinue(_ context.Context, cursor string) ([]Entry, string, bool, error) {
	if cursor != m.cursor {
		return nil, "", false, ErrExpiredCursor
	}

	if len(m.entries) <= 1 {
		return nil, m.cursor, false, nil
	}

	return m.entries[1:], m.cursor, false, nil
}

func (m *MockClient) Longpoll(ctx context.Context, _ string, timeoutSec int) (bool, error) {
	select {
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (m *MockClient) DownloadRange(_ context.Context, path string, start, length uint64) (io.ReadCloser, error) {
	content, ok := m.files[path]
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

func (m *MockClient) UploadFile(_ context.Context, filepath string, content []byte) (*UploadFileResponse, error) {
	m.files[filepath] = content

	return &UploadFileResponse{
		ID:          "id:" + filepath,
		ContentHash: "mock-hash",
		Name:        filepath[strings.LastIndex(filepath, "/")+1:],
		PathDisplay: filepath,
		Size:        uint64(len(content)),
	}, nil
}

func (m *MockClient) CreateSession(_ context.Context, content []byte) (*SessionResponse, error) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	sessionID := fmt.Sprintf("mock-session-%d", len(m.sessions)+1)
	m.sessions[sessionID] = &mockSession{
		sessionID: sessionID,
		content:   content,
		offset:    uint(len(content)),
	}

	return &SessionResponse{
		SessionID: sessionID,
	}, nil
}

func (m *MockClient) UploadChunk(_ context.Context, sessionID string, content []byte, offset uint) error {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return ErrSessionNotFound
	}

	if offset != session.offset {
		return ErrInvalidOffset
	}

	session.content = append(session.content, content...)
	session.offset += uint(len(content))
	return nil
}

func (m *MockClient) CloseSession(_ context.Context, filepath, sessionID string, offset uint) (*UploadFileResponse, error) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}

	if offset != session.offset {
		return nil, ErrInvalidOffset
	}

	m.files[filepath] = session.content
	delete(m.sessions, sessionID)

	return &UploadFileResponse{
		ID:          "id:" + filepath,
		ContentHash: "chunked-hash",
		Name:        filepath[strings.LastIndex(filepath, "/")+1:],
		PathDisplay: filepath,
		Size:        uint64(len(session.content)),
	}, nil
}

func (m *MockClient) DeleteFile(_ context.Context, filepath string) error {
	if _, ok := m.files[filepath]; !ok {
		return ErrFileNotFound
	}
	delete(m.files, filepath)

	return nil
}
