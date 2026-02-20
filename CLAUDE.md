# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
perl Makefile.PL && make          # build (auto-detects OpenSSL, liblz4)
make test                          # run all tests (needs ClickHouse running)
prove -lv t/02_query.t             # run a single test
make manifest && make dist         # build distribution tarball
```

Tests require a running ClickHouse instance. Set env vars:
```bash
TEST_CLICKHOUSE_HOST=127.0.0.1     # default
TEST_CLICKHOUSE_PORT=18123          # HTTP port
TEST_CLICKHOUSE_NATIVE_PORT=19000   # native protocol port
```

Quick local ClickHouse for testing: write config XML to `/tmp/ch-config.xml`, run `clickhouse-server --config-file=/tmp/ch-config.xml --daemon`.

## Architecture

Async ClickHouse client implemented entirely in XS (no external C library). Supports two protocols:

- **HTTP** (port 8123): Stateless, serialized request queue (one in-flight at a time, queued via send_queue). TabSeparated format with gzip compression.
- **Native** (port 9000): Binary column-oriented protocol with handshake. Optional LZ4 compression with CityHash checksums. Client revision 54459.

### Core files

- `ClickHouse.xs` — struct definitions, magic constants, freelists, watcher helpers, callback machinery, error/cleanup paths, OpenSSL init, all XSUBs + BOOT (~2700 lines)
- `xs/codecs.c` — gzip + LZ4 (with CityHash checksum) + date helpers + TabSeparated parser
- `xs/proto_http.c` — HTTP request building + response parsing
- `xs/proto_native_build.c` — native protocol packet builders (hello, query, ping, etc.)
- `xs/proto_native_parse.c` — native protocol response parser (Hello/Data/Exception/Progress/...)
- `xs/types.c` — JSON typed-paths + native column decoders + opt-in formatting + native column encoders (~3000 lines)
- `xs/io.c` — async TCP connect, TLS handshake, I/O callbacks, keepalive, reconnect with backoff, pipeline orchestrator
- `lib/EV/ClickHouse.pm` — thin Perl wrapper: constructor arg parsing, method aliases (`q`=`query`, `reconnect`=`reset`, `disconnect`=`finish`); Pool/Streamer/Iterator hot paths now route through XS helpers
- `cityhash.h` — CityHash for LZ4 checksum validation
- `ngx_queue.h` — intrusive linked list (from nginx) for callback/send queues

The `xs/*.c` files are textually `#include`d into `ClickHouse.xs` (single translation unit), so `static` helpers stay file-local-to-the-TU and the build system needs no extra OBJECT entries. To rebuild, just `make` as usual.

### State machine

Connect → (TLS handshake) → (Native handshake / ServerHello) → Connected → queue queries → io_cb writes → io_cb reads → parse response → deliver callback → pipeline_advance

### Key struct: `ev_clickhouse_s`

Central connection object holding: fd, EV watchers (rio/wio/timer), send/recv buffers, callback queue (ngx_queue), send queue, protocol state, TLS context.

### XS conventions

- `#ifdef` inside CODE blocks must be indented (not column 1) to avoid confusing xsubpp
- Freelist pools for queue entries to reduce malloc churn
- `callback_depth` counter guards against reentrancy
- `PL_dirty` check in DESTROY for graceful global destruction
- Buffer growth uses doubling strategy via `nbuf_grow()`

### Native protocol details

- VarUInt length-prefixed packets; exception code is Int32 (not VarUInt)
- `decode_column()` handles 30+ types: Int/UInt variants, String, FixedString, Nullable, LowCardinality, UUID, Date/DateTime, Enum, Array, Tuple, Map, Nothing, IPv4/IPv6
- LowCardinality uses UInt64 LE for version/counts (not VarUInt), flags in bits 8-10
- UUID: 16 bytes as two LE UInt64 halves, reversed to standard byte order
- Nothing type: 1 byte ('0'=0x30) per row, not 0 bytes
- Must receive ServerHello before setting connected=1; set connected=1 before firing on_connect

### Test structure

- `t/00_load.t` — module load only, no ClickHouse needed
- `t/01-04` — HTTP protocol tests (connect, query, insert, misc)
- `t/05-07` — native protocol tests (basic, types, compression)
- `t/08` — per-query settings (both protocols)
- `t/09` — arrayref INSERT (both protocols)
- `t/10` — raw query mode (HTTP only)
- `t/11_new_features.t` — server_timezone, column_names, decode_datetime/decimal/enum, named_rows, on_disconnect, on_data, keepalive, query_timeout, cancel, error message format
- `t/12_advanced.t` — Decimal128 scaling, DateTime with TZ, DateTime64, SimpleAggregateFunction, Nested, drain (deferred + immediate), finish
- `t/13_params_uri.t` — parameterized queries, URI parsing, on_trace, keepalive smoke, Int256/UInt256
- `t/14_new_accessors.t` — column_types, last_error_code, profile_rows_before_limit, last_totals, LowCardinality multi-block, reconnect_delay option
- `t/15_streaming.t` — on_data per-block delivery
- `t/16_totals_extremes.t` — WITH TOTALS, last_totals, last_extremes
- `t/17_progress.t` — on_progress callback fires
- `t/18_cancel.t` — cancel mid-query (native + HTTP)
- `t/19_timeouts.t` — connect_timeout, query_timeout
- `t/20_reconnect.t` — auto_reconnect, pre-connect queue
- `t/21_edge_types.t` — empty, NULL, FixedString, large blob, IPv6, Map
- `t/22_named_rows.t` — named_rows + decode_datetime/decimal/enum
- `t/23_more_coverage.t` — HTTP/native ping, decode_decimal precision, per-query settings proof, 10k-row INSERT
- `t/24_review_gaps.t` — profile_rows/bytes (HTTP+native), UUID, Date32, Enum16, reset(), cancel no-op, on_data croak on HTTP, IPv6 URI literal, native param escape regression, zero-row schema, insert query_timeout, last_totals/extremes cleared between queries, db => alias, loop => parameter, server_info undef on HTTP, HTTP last_query_id, profile_*/column_names cleared after DDL, last_error_code reset, params shared between query and insert, native connect_timeout covering ServerHello stall
- `t/25_features.t` — max_reconnect_attempts cap, HTTP keepalive PING, progress_period coalescing, for_table schema helper, insert_streamer streaming INSERT, cancel during on_data, on_disconnect not firing on connect-phase failure, connect_timeout + auto_reconnect interaction
- `t/26_json.t` — JSON / Object('json') round-trip with flat + nested values, Bool/Float64 leaves, JSON typed-paths
- `t/27_pool_failover_qlc.t` — Pool basics + drain, multi-host failover, query_log_comment, async_insert, async DNS via EV::cares, Pool insert + iterate
- `t/28_pass2_coverage.t` — on_query_complete (success/error), HTTP keepalive on_query_complete suppression, query_log_comment in INSERT and on HTTP, DNS failure with pre-queued query, Pool skip_pending broadcast, Iterator timeout, reset() from on_error / queued-cb / query-timeout cb / cancel cb, finish-during-DNS no zombie reconnect, Streamer high_water
- `xt/` — author tests: pod_syntax, pod_coverage, kwalitee, changes,
  opt-in asan, opt-in compat-matrix (Docker multi-version), fault-injection
  (in-process TCP proxy), property-based round-trip, Test::LeakTrace,
  opt-in valgrind, stress, opt-in TLS
- Tests use `with_ch(cb => sub { ... })` helper: creates connection, runs callback on connect, calls `EV::break` to exit loop, cleans up. All tests check reachability upfront and `skip_all` if ClickHouse is unavailable.
- HTTP queries require explicit `format TabSeparated` in SQL; native protocol auto-returns typed data
- Native parameter values are escaped per ClickHouse Field::dump format (single quote and backslash get a leading `\`); see `nbuf_quoted_param`. Use `prove -bv` (with blib) so freshly built XS is loaded — `prove -lv` picks up the installed `.so`.

### Build dependencies

- Required: EV (>= 4.11, via EV::MakeMaker), zlib
- Optional: OpenSSL (TLS), liblz4 (native compression) — auto-detected via pkg-config with fallback to header probe
