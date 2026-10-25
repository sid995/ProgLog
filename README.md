# proglog

An append-only record log with a small HTTP demo, disk-backed storage, authenticated gRPC operations, and Serf peer discovery. Method-level documentation lives alongside the Go code; the RPC contract is documented in `api/v1/log.proto`.

## Code map

| Path | Responsibility |
| --- | --- |
| `main.go` | Starts the HTTP demo on `:8080`. |
| `internal/server/http.go`, `internal/server/log.go` | JSON API backed by an in-memory slice. This state is separate from the persistent log. |
| `internal/log/` | Persistent segmented log, protobuf serialization, record framing, and offset lookup. |
| `internal/server/server.go` | gRPC handlers, streaming, authentication interceptors, logging, and OpenCensus telemetry. |
| `api/v1/log.proto`, `api/v1/error.go` | RPC schema and missing-offset status conversion. The `.pb.go` files are generated bindings. |
| `internal/auth/` | Casbin authorization for subject/object/action tuples. |
| `internal/config/` | TLS configuration and credential/ACL path resolution. |
| `internal/discovery/` | Serf gossip membership and application callbacks. Discovery is not wired into the executable. |
| `test/` | Development certificate requests, signing profiles, and ACL fixtures. |

## Persistent record flow

`Log.Append` serializes writes under a mutex and delegates to the active segment. The segment replaces the supplied record's offset, encodes it as protobuf, appends it to the store, then adds an index entry. It advances the next offset only after the index write succeeds. Once an append reaches a configured size threshold, the log opens a new segment beginning at the following offset.

Each segment uses two files named after its absolute base offset:

- `<base>.store`: repeated `[8-byte big-endian payload length][protobuf Record]` entries. Appends are buffered; reads and close flush that buffer.
- `<base>.index`: repeated `[4-byte big-endian relative offset][8-byte big-endian store position]` entries. Relative offsets save four bytes per entry. The index is preallocated and memory-mapped with shared writes.

`Log.Read` locates the segment containing an absolute offset. The segment subtracts its base offset, uses the resulting index slot to find the store position, then reads and decodes the record. A missing absolute offset becomes `ErrOffsetOutOfRange`, which exposes gRPC `NotFound` with localized details.

On a clean close the index is synchronized and truncated to its used length. Reopening uses that length and its final entry to recover the next offset. This assumes complete file pairs and a clean shutdown; there is no crash recovery scan. Append acknowledgement does not imply an fsync to durable media. An index write failure can leave store bytes without an index entry.

`Reader` exposes raw framed store bytes across the current segment list. `Truncate(lowest)` deletes whole segments ending at or below the cutoff; it retains a segment that contains records on both sides of the cutoff. It does not remove individual records within a segment.

## API behavior and identity

The executable starts only the HTTP demo. `POST /` accepts `{"record":{"value":"aGVsbG8="}}`; `GET /` accepts a JSON body such as `{"offset":0}`. Byte values use JSON base64 encoding. HTTP has no persistence or authentication.

To use gRPC, an embedding program supplies a `CommitLog`, an `Authorizer`, transport options, and a listener to `NewGRPCServer`. Both unary and streaming operations use the same authorization checks. A verified client certificate's common name is the policy subject. All log requests use the literal object `*` and action `produce` or `consume`. The test policy permits `root` for both actions and denies `nobody` by absence of a matching rule.

`ProduceStream` acknowledges each request before receiving the next. `ConsumeStream` sends successive offsets, advancing only after a successful send. Missing offsets are retried immediately, including at the log tail, so an idle consuming stream busy-polls. Cancellation ends that consuming loop. The producer returns receive errors directly, including client EOF.

TLS configuration loads a certificate only when both certificate and key paths are provided. A configured CA supplies client trust roots in client mode; in server mode it requires and verifies client certificates. The authentication interceptor assumes authenticated transports are TLS connections with verified certificate chains.

Serf uses its own gossip bind address. A member's `rpc_addr` tag advertises the application address delivered to `Handler.Join`. Remote departures and failures invoke `Handler.Leave`; callback errors are logged without retry. Joining seed peers happens after Serf starts, so a seed-join error does not shut down the already-created instance.

## Development and tests

The module's Go version is declared in `go.mod`. Run the HTTP demo with `go run .`. Run storage tests with `go test ./internal/log`.

The gRPC tests need certificates and ACL files. `internal/config` resolves their paths once at package initialization, using `CONFIG_DIR` when set or `$HOME/.proglog` otherwise. With the CFSSL tools installed, `make init`, `make gencert`, and `make test` prepare the default directory and run the suite with the race detector. The Makefile uses `$HOME/.proglog`; a custom `CONFIG_DIR` must be populated separately. Certificate fixtures cover localhost and `127.0.0.1`; generated root and nobody certificates exercise permission checks.

`make compile` regenerates protobuf bindings and requires `protoc` plus the Go protobuf and gRPC generators. Edit schema documentation in `log.proto` rather than generated bindings. `go test ./internal/server -debug` enables development logging and writes telemetry to temporary files.

Storage tests cover framing, positional reads, index boundaries, segment limits, clean reopen, raw readers, and segment retention. Server tests exercise TLS identities, unary and streaming requests, missing offsets, and authorization failures.

## Current implementation limits

These describe existing behavior rather than guarantees:

- Startup assumes the log directory contains only matching segment file pairs. It ignores filename parse errors and currently suppresses segment-opening errors.
- `Reset` deletes the directory and calls setup without recreating it or clearing segment state; it is not a working reset operation.
- `Remove` and `Reset` require caller coordination with concurrent use. `Reader` captures a segment list, not an immutable snapshot; readers must not race with removal or closing those files.
- Truncating all segments can leave stale active-segment state and an empty segment list. Offset queries expect at least one segment. An empty log with a nonzero initial offset reports that initial offset minus one as its highest offset.
- gRPC construction changes process-wide trace sampling and registers global metric views. The provided executable does not start gRPC, replication, or discovery.
