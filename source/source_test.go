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
	"testing"

	config "github.com/conduitio-labs/conduit-connector-dropbox/config"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	con := &Source{}
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	con.config = Config{
		Config: config.Config{
			Token: "test-token",
		},
	}

	err := con.Open(ctx, nil)
	is.NoErr(err)
}
