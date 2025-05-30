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

//go:generate conn-sdk-cli specgen

package dropbox

import (
	_ "embed"

	"github.com/conduitio-labs/conduit-connector-dropbox/destination"
	"github.com/conduitio-labs/conduit-connector-dropbox/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:embed connector.yaml
var specs string

var version = "(devel)"

var Connector = sdk.Connector{
	NewSpecification: sdk.YAMLSpecification(specs, version),
	NewSource:        source.NewSource,
	NewDestination:   destination.NewDestination,
}
