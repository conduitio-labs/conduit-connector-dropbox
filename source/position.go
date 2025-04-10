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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

type ChunkInfo struct {
	FileID      string `json:"file_id"`
	FilePath    string `json:"file_path"`
	ChunkIndex  int    `json:"chunk_index"`
	TotalChunks int    `json:"total_chunks"`
}

type Position struct {
	mu          sync.Mutex
	Cursor      string     `json:"cursor"`
	ChunkInfo   *ChunkInfo `json:"chunk_info,omitempty"`
	LastUpdated time.Time  `json:"last_updated"`
}

func NewPosition() *Position {
	return &Position{}
}

func ParseSDKPosition(position opencdc.Position) (*Position, error) {
	var pos Position
	if position == nil {
		return NewPosition(), nil
	}

	if err := json.Unmarshal(position, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal opencdc.Position into Position: %w", err)
	}
	return &pos, nil
}

func (p *Position) marshal() (opencdc.Position, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}
	return positionBytes, nil
}

func (p *Position) updateFile(fileID, filePath string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ChunkInfo = nil
	p.LastUpdated = time.Now()
}

func (p *Position) updateChunk(fileID, filePath string, chunkIdx, totalChunks int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ChunkInfo == nil || p.ChunkInfo.FileID != fileID {
		p.ChunkInfo = &ChunkInfo{
			FileID:      fileID,
			FilePath:    filePath,
			ChunkIndex:  chunkIdx,
			TotalChunks: totalChunks,
		}
	} else {
		p.ChunkInfo.ChunkIndex = chunkIdx
	}
	p.LastUpdated = time.Now()
}
