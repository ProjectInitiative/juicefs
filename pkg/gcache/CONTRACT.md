# gcache ↔ JuiceFS Contract

Single source of truth for the facts `pkg/gcache` depends on. If an upstream
sync changes any of these, the contract tests fail and only the bridge/wiring
needs a touch-up. Keep updated when contracts change.

Owner: ProjectInitiative fork. Last verified against upstream `f654a80c`.

## 1. Block key format

Produced by `pkg/chunk/cached_store.go` `rSlice.key()`:

* hash-prefix mode: `<id>_` + hex(murmur3 of `<id>_<indx>_<size>`) (via `utils.Hash`)
* plain mode: `<id>_<indx>_<size>`

Keys are simultaneously object-storage keys. gcache treats them as opaque
strings — placement hashes the string as-is.

## 2. Full-block Get signature

`cachedStore.load()` fetches objects ONLY in this shape:

```go
storage.Get(ctx, key, 0, -1, object.WithRequestID(&id), object.WithStorageClass(&sc))
```

`off == 0 && limit == -1` ⇒ full object ⇒ the only shape the client-side
decorator intercepts. Partial reads (`off>0` or `limit>0`) pass through
untouched (seekable fast path / range warmup semantics preserved).
`AttrGetter` options must be forwarded to the underlying Get on fall-through.

## 3. Compression round-trip (critical)

* `load()` receives **volume-format** bytes from `Get()` and decompresses
  itself via `store.compressor` (`pkg/compress`, selected by string:
  "zstd" / "lz4" / "none").
* Local cache files store **decompressed** bytes (`diskCache`).
* Therefore the peer wire ALWAYS carries volume-format bytes: the server
  reads decompressed bytes from its cache and re-compresses on serve when
  the volume uses compression (per-frame `algo` flag: WireRaw/WireCompressed).
* The client decorator never decompresses (it cannot know the decompressed
  size; `load()` does that internally) — it returns wire bytes as-is.
* For AI/training volumes created with `--compress none` (the common case)
  server re-compression cost is zero.

## 4. ServerSource interface (server side, via bridge)

The peer server needs exactly this from a mount's `*cachedStore` (adapters in
`pkg/chunk/gcache_bridge.go`; structural typing, no imports either way):

```go
type ServerSource interface {
    LoadCached(key string) (io.ReadCloser, error)          // os.ErrNotExist if absent
    FillFromStorage(ctx context.Context, key string) (io.ReadCloser, error)
    CompressAlgo() string                                   // "none"/"lz4"/"zstd"
    CompressBound(n int) int
    CompressPayload(dst, src []byte) (int, error)
}
```

Bridge maps: `bcache.load(key)` → LoadCached; load+cache variant of
`cachedStore.load()` → FillFromStorage; `store.compressor` passthrough for
the rest. NOTE: `ReadCloser` here is `chunk.ReadCloser` (pkg/chunk/cache_manager.go:255).

## 5. Membership registry integration point

* Registration happens after `metaCli.NewSession(true)` in `cmd/mount.go`
  (requires an open meta connection).
* Member uuid = JuiceFS session uuid (fresh per mount process).
* Redis impl: per-group HASH `<prefix>gcache/<group>`, field = uuid, value =
  JSON `{uuid,addr,weight,version,ts}`; `EXPIRE` refreshed per heartbeat
  (10s interval, 90s TTL); `HGETALL` to list; ts-staleness backstop.
* tkv impl: keys `<prefix>gcache/<group>/<uuid>`; range scan to list;
  ts-based pruning.
* SQL impl: `gcache_members(group, uuid, addr, weight, ts)`; heartbeat
  upsert; DELETE stale at list time.

## 6. Wire protocol (v1)

```
20-byte header, little-endian:
  magic 3 bytes 'G','C',1 | type u8 | algo u8 | xid u32 | klen u16 | plen u64
  type: 0x01 BLOCK_REQ, 0x02 BLOCK_RESP, 0x03 ERROR
  requests:   algo=0, klen>0, key follows, plen=0
  responses:  klen=0, payload follows; algo=WireRaw|WireCompressed
  ERROR:      plen=^uint64(0), klen>0, error string follows
```

Client may pipeline requests per connection; responses carry matching xid.

## 7. Failure semantics (enterprise parity)

* `--remote-timeout` per attempt (default 65s), one retry on transient errors.
* 31 consecutive failures → evict peer from consumer's local view:
  `remove peer %s after %d failures in a row`.
* Successful op re-adds: `add peer %s back after %s`.
* Any peer read failure ⇒ fall through to underlying object storage Get.

## 8. Metrics (Prometheus, enterprise-parity names)

* `juicefs_remotecache_gets`, `juicefs_remotecache_bytes` (counters)
* `juicefs_remotecache_errors` (counter)
* `juicefs_remotecache_durations` (histogram, seconds)
* `juicefs_peer_failures` (gauge vec by peer addr)

## 9. Flags added by this fork (cmd/mount.go)

`--cache-group` (strings slice), `--group-weight`, `--group-listen`,
`--group-advertise`, `--group-heartbeat`, `--remote-timeout`, `--no-sharing`,
`--fill-group-cache`. Names mirror the enterprise command reference.

## 10. Contract tests

`pkg/gcache/contract_test.go` verifies: ObjectStorage interface shape (§2),
key format sample hash round-trip (§1), compressor string selection (§3),
ServerSource satisfiable by a test adapter (§4). Run before/after upstream
sync: `go test ./pkg/gcache/ -run TestContract`.
