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
- **Native** (port 9000): Binary column-oriented protocol with handshake. Optional LZ4 compression with CityHash checksums. Client revision 54429.

### Core files

- `ClickHouse.xs` — all C logic (~5900 lines): connection state machine, HTTP/native parsers, column type decoders, buffer management, EV watcher callbacks
- `lib/EV/ClickHouse.pm` — thin Perl wrapper: constructor arg parsing, method aliases (`q`=`query`, `reconnect`=`reset`, `disconnect`=`finish`)
- `cityhash.h` — CityHash for LZ4 checksum validation
- `ngx_queue.h` — intrusive linked list (from nginx) for callback/send queues

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
- Tests use `with_ch(cb => sub { ... })` helper: creates connection, runs callback on connect, calls `EV::break` to exit loop, cleans up. All tests check reachability upfront and `skip_all` if ClickHouse is unavailable.
- HTTP queries require explicit `format TabSeparated` in SQL; native protocol auto-returns typed data

### Build dependencies

- Required: EV (>= 4.11, via EV::MakeMaker), zlib
- Optional: OpenSSL (TLS), liblz4 (native compression) — auto-detected via pkg-config with fallback to header probe
