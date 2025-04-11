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

import "time"

type Entry struct {
	Tag            string    `json:".tag"`
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	PathDisplay    string    `json:"path_display"`
	ServerModified time.Time `json:"server_modified"`
	Size           uint64    `json:"size,omitempty"`
	ContentHash    string    `json:"content_hash,omitempty"`
}
