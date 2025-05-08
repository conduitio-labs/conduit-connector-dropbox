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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-dropbox/pkg/dropbox"
)

func CleanupTestFiles(ctx context.Context, client dropbox.FoldersClient, path string) error {
	entries, _, _, err := client.List(ctx, path, false, 100)
	if err != nil {
		return fmt.Errorf("failed to list test folder for cleanup: %w", err)
	}

	for _, entry := range entries {
		err := client.DeleteFile(ctx, entry.PathDisplay)
		if err != nil {
			return fmt.Errorf("failed to delete test file %s: %w", entry.PathDisplay, err)
		}
	}

	return nil
}
