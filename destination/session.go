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

package destination

import (
	"fmt"
	"sync"
)

var ErrInvalidSession = fmt.Errorf("invalid session")

type cursor struct {
	sessionID string
	offset    uint
}

type Session struct {
	mu       sync.Mutex
	sessions map[string]cursor
}

func NewSession() *Session {
	return &Session{
		sessions: make(map[string]cursor),
	}
}

func (s *Session) startSession(fileID, sessionID string, offset uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[fileID] = cursor{sessionID, offset}
}

func (s *Session) updateSession(fileID string, offset uint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.sessions[fileID]
	if !ok {
		return ErrInvalidSession
	}
	s.sessions[fileID] = cursor{val.sessionID, val.offset + offset}
	return nil
}

func (s *Session) closeSession(fileID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, fileID)
}

func (s *Session) getSession(fileID string) (cursor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.sessions[fileID]
	return val, ok
}
