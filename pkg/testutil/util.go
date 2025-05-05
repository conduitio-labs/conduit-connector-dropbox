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

package testutil

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
)

func CleanupTestFiles(ctx context.Context, t *testing.T, client dropbox.FoldersClient, path string) {
	t.Helper()

	entries, _, _, err := client.List(ctx, path, false, 100)
	if err != nil {
		t.Logf("warning: failed to list test folder for cleanup: %v", err)
		return
	}

	for _, entry := range entries {
		err := client.DeleteFile(ctx, entry.PathDisplay)
		if err != nil {
			t.Logf("warning: failed to delete test file %s: %v", entry.PathDisplay, err)
		}
	}
}
