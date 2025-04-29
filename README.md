# Conduit Connector for <!-- readmegen:name -->Dropbox<!-- /readmegen:name -->

[Conduit](https://conduit.io) connector for <!-- readmegen:name -->Dropbox<!-- /readmegen:name -->.

<!-- readmegen:description -->
## Source

The Dropbox Source Connector reads files from a configured Dropbox path and converts 
them into `opencdc.Record` that can be processed by Conduit. Files larger than 
`fileChunkSizeBytes` (maximum value 4MB) are split into smaller chunks, and each 
chunk is emitted as a separate record.

### Snapshot (Initial Sync)

When the connector starts without a saved position, it triggers a snapshot listing 
all files in the configured Dropbox path. A cursor and the last processed timestamp 
are saved at the end of snapshotting.

### Change Data Capture

After the snapshot, the connector uses Dropbox's Longpoll API to wait for changes 
(file creation, modification, deletion). Upon detecting changes, it fetches updated 
entries using the saved cursor. If Dropbox reports expired cursor the connector 
falls back to a fresh scan skipping already-processed files based on the 
`lastProcessedUnixTime` field.

Each record have following metadata fields to support downstream file reassembly:

* `filename`: Filename of the file with extension.
* `file_id`: Unique Dropbox file ID.
* `file_path`: Full path of the file in Dropbox (e.g., `/Photos/Vacation/image.jpg`).
* `opencdc.collection`: Path to the parent directory containing the file (e.g., `/Photos/Vacation`).
* `file_size`: Integer size of the file.
* `chunk_index`: Index of the current chunk (starting from 1) – only present for chunks.
* `total_chunks`: Total number of chunks – only present for chunked files.
* `hash`: A hash of the file content. This field can be used to verify data integrity.

## Destination

A destination connector pushes data from upstream resources to an external
resource via Conduit.
<!-- /readmegen:description -->

## Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "dropbox"
        settings:
          # Token is used to authenticate API access.
          # Type: string
          # Required: yes
          token: ""
          # Size of a file chunk in bytes to split large files, maximum is 4MB.
          # Type: int
          # Required: no
          fileChunkSizeBytes: "3145728"
          # Maximum number of entries to fetch in a single list_folder request.
          # Type: int
          # Required: no
          limit: "1000"
          # Timeout for Dropbox longpolling requests.
          # Type: duration
          # Required: no
          longpollTimeout: "30s"
          # Path of the Dropbox directory to read/write files. Empty path
          # implies root directory.
          # Type: string
          # Required: no
          path: ""
          # Maximum number of retry attempts.
          # Type: int
          # Required: no
          retries: "0"
          # Delay between retry attempts.
          # Type: duration
          # Required: no
          retryDelay: "10s"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

## Destination Configuration Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "dropbox"
        settings:
          # Token is used to authenticate API access.
          # Type: string
          # Required: yes
          token: ""
          # DestinationConfigParam must be either yes or no (defaults to yes).
          # Type: string
          # Required: no
          destinationConfigParam: "yes"
          # Path of the Dropbox directory to read/write files. Empty path
          # implies root directory.
          # Type: string
          # Required: no
          path: ""
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## Development

- To install the required tools, run `make install-tools`.
- To generate code (mocks, re-generate `connector.yaml`, update the README,
  etc.), run `make generate`.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the
required resource locally.

## How to release?

The release is done in two steps:

- Bump the version in [connector.yaml](/connector.yaml). This can be done
  with [bump_version.sh](/scripts/bump_version.sh) script, e.g.
  `scripts/bump_version.sh 2.3.4` (`2.3.4` is the new version and needs to be a
  valid semantic version). This will also automatically create a PR for the
  change.
- Tag the connector, which will kick off a release. This can be done
  with [tag.sh](/scripts/tag.sh).

## Known Issues & Limitations

- Known issue A
- Limitation A

## Planned work

- [ ] Item A
- [ ] Item B
