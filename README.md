# NAME

EV::ClickHouse - Async ClickHouse client using EV

# SYNOPSIS

    use EV;
    use EV::ClickHouse;

    # Discrete parameters
    my $ch = EV::ClickHouse->new(
        host       => '127.0.0.1',
        port       => 9000,
        protocol   => 'native',     # or 'http'
        user       => 'default',
        password   => '',
        database   => 'default',
        settings   => { max_threads => 4 },  # connection-level defaults
        on_connect => sub { print "connected\n" },
        on_error   => sub { warn "error: $_[0]\n" },
    );

    # Or via URI: clickhouse[+native]://user:pass@host:port/db?key=val
    my $ch = EV::ClickHouse->new(
        uri        => 'clickhouse+native://default:@127.0.0.1:9000/default',
        on_connect => sub { ... },
    );

    # select
    $ch->query("select number from system.numbers limit 3", sub {
        my ($rows, $err) = @_;
        die $err if $err;
        print "row: @$_\n" for @$rows;     # row: 0 / row: 1 / row: 2
    });

    # Per-query settings + parameterized values (no string interpolation)
    $ch->query(
        "select {x:UInt32} + {y:UInt32} as sum",
        { params => { x => 40, y => 2 }, max_execution_time => 30 },
        sub { my ($rows, $err) = @_; print $rows->[0][0], "\n" },  # 42
    );

    # insert - arrayref of rows (no TSV escaping needed)
    $ch->insert("my_table", [
        [1, "hello\tworld"],   # embedded tab is fine
        [2, undef],            # null
        [3, [10, 20]],         # Array column
    ], sub { my (undef, $err) = @_; warn "insert: $err" if $err });

    # insert - pre-formatted TSV string
    $ch->insert("my_table", "1\tfoo\n2\tbar\n", sub { ... });

    # Raw HTTP response body (HTTP only)
    $ch->query("select * from t format CSV", { raw => 1 }, sub {
        my ($body, $err) = @_;
        print $body;
    });

    EV::run;

# DESCRIPTION

EV::ClickHouse is an asynchronous ClickHouse client that integrates with
the [EV](https://metacpan.org/pod/EV) event loop. It speaks both the ClickHouse HTTP protocol
(port 8123) and the native TCP protocol (port 9000) directly in XS, with
no external ClickHouse client library linked. zlib is required; OpenSSL
(for TLS) and liblz4 (for native compression) are optional and detected
at build time.

## Features

- HTTP and native TCP protocols, with the same Perl API
- gzip compression (HTTP) and LZ4 compression with CityHash
checksums (native)
- TLS/SSL via OpenSSL, with optional `tls_skip_verify` for
self-signed certs and `tls_ca_file` for additional roots
- Connection URIs (`clickhouse[+native]://user:pass@host:port/db`),
including bracketed IPv6 literals
- Per-query and connection-level ClickHouse settings; parameterized
queries via `params`
- Auto-reconnect with exponential backoff; queued (unsent) queries
are preserved across reconnects
- Keepalive pings for idle native connections; graceful drain;
query cancellation and skip\_pending
- Streaming results via `on_data` per-block callback (native);
on\_progress for native progress packets
- Raw HTTP response mode for CSV / JSONEachRow / Parquet / etc.
- 35+ ClickHouse types including Int/UInt 8..256, Float32/64,
BFloat16, Decimal32/64/128/256, UUID, IPv4/IPv6, Nullable, Array,
Tuple, Map, LowCardinality (with cross-block dictionaries),
SimpleAggregateFunction, Nested, Geo (Point/Ring/LineString/Polygon
and the Multi variants), and JSON / Object('json') with auto-flattened
hashref leaves (Int64/Float64/Bool/String + Array variants).
- Opt-in decode of Date/DateTime, Decimal, and Enum columns; named-rows
(hashref) mode

# CONSTRUCTOR

## new

    my $ch = EV::ClickHouse->new(%args);

The connection is initiated immediately; `new` returns before it
completes. Queries issued before `on_connect` fires are queued and
dispatched once the connection is ready.

**Connection parameters:**

- uri => $uri\_string

    Single-string connection target:
    `clickhouse[+native]://user:pass@host:port/database?key=value`.

    The `+native` suffix selects the native protocol; otherwise HTTP is used.
    Hostnames, IPv4 addresses, and bracketed IPv6 literals are all accepted
    (e.g. `clickhouse://[::1]:9000/db`). Query-string values are merged into
    the constructor arguments. Discrete `host`, `port`, etc. arguments
    override the URI.

- host => $hostname

    Server hostname. Default: `127.0.0.1`.

    **Note:** DNS resolution is blocking unless [EV::cares](https://metacpan.org/pod/EV%3A%3Acares) is installed.
    With [EV::cares](https://metacpan.org/pod/EV%3A%3Acares) available, hostnames are resolved off-loop at
    construct time (the constructor returns immediately, queries queue
    until the resolved address is connected). Falls back to blocking
    `getaddrinfo` otherwise.

- hosts => \[$h1, $h2, ...\]

    Multi-host failover list. Each entry is `host`, `host:port`, or a
    bracketed-IPv6 literal. On a connect-phase failure (refused, timeout,
    ServerHello stall), the client advances to the next host in round-robin
    order; pair with `auto_reconnect => 1` for automatic recovery.
    The single `host` argument is honoured as a fallback when
    `hosts` isn't given.

- port => $port

    Server port. Default: `8123` (HTTP), `9000` (native).

- protocol => 'http' | 'native'

    Protocol to use. Default: `http`.

- user => $username

    Username. Default: `default`.

- password => $password

    Password. Default: empty.

- database => $dbname

    Default database. Default: `default`. The shorter alias `db` is also
    accepted.

- tls => 0 | 1

    Enable TLS. Default: `0`. Requires the module to be built with OpenSSL
    (otherwise the constructor croaks).

- tls\_ca\_file => $path

    Additional CA certificate file for TLS verification, used alongside the
    system trust store.

- tls\_cert\_file => $path, tls\_key\_file => $path

    PEM-encoded client certificate and matching private key for mutual TLS
    (mTLS). Both must be set together. The client certificate is sent
    during the TLS handshake; the server's trust chain decides whether to
    accept it. Required by managed ClickHouse offerings (Aiven, Altinity
    Cloud) that enforce cert-based auth. The private key must match the
    public key in the certificate; the constructor errors out at handshake
    time with `"TLS client cert / private key mismatch"` otherwise.

- tls\_skip\_verify => 0 | 1

    Skip TLS certificate verification. Default: `0`. Useful in development
    with self-signed certs; do not use in production.

- loop => $ev\_loop

    EV event loop object. Default: `EV::default_loop`.

**Callbacks:**

- on\_connect => sub { }

    Called once the connection is fully established (after the native
    ServerHello, or after the TCP/TLS handshake for HTTP).

- on\_error => sub { my ($message) = @\_ }

    Called on connection-level errors (DNS failure, socket error, TLS failure,
    read/write errors, etc.). Default: `sub { die @_ }`. Per-query errors
    are delivered to the query's own callback as the second argument; they
    do not invoke `on_error`.

    When a connection drops mid-flight, `on_error` fires first with the
    underlying cause, and `on_disconnect` fires immediately after as the
    state machine tears the socket down. If `auto_reconnect` is set, the
    reconnect attempt happens after `on_disconnect` returns.

    It is safe to call `reset` (or `reconnect`) from inside `on_error` -
    the freshly-armed socket survives the outer teardown that would
    otherwise close it. Use this for custom recovery logic (e.g. switching
    to a backup host on specific errors).

- on\_progress => sub { my ($rows, $bytes, $total\_rows, $written\_rows, $written\_bytes) = @\_ }

    Called on native protocol progress packets. Not fired for HTTP.

- on\_disconnect => sub { }

    Called when an established connection closes (by `finish`, server
    disconnect, or mid-flight error). Only fires if `on_connect` had
    previously fired - it does **not** fire for connect-phase failures
    (refused, timeout, ServerHello stall) since no connection was ever
    established. Fires after internal state has been reset, so it is safe
    to queue new queries or call `reset` from inside the handler.

- on\_trace => sub { my ($message) = @\_ }

    Debug trace callback. Called with internal state-machine messages
    (connect, dispatch, disconnect). Useful for diagnosing protocol issues.

- on\_failover => sub { my ($old\_host, $old\_port, $new\_host, $new\_port, $msg) = @\_ }

    Multi-host only. Fires after the failover wrapper rotates to the next
    host in the `hosts => [...]` list, with the old and new (host, port)
    pair plus the triggering error message. Use it for metrics ("which host
    am I on?") or to log host transitions. Fires before the user's `on_error`.

**Options:**

- compress => 0 | 1

    Enable compression: gzip on HTTP (request and response), LZ4 with CityHash
    checksums on the native protocol. Default: `0`. Native compression
    requires liblz4 at build time.

- session\_id => $id

    HTTP session id for stateful operations (temporary tables, SET, etc.).
    Native protocol has stateful sessions intrinsically; this option is HTTP-only.

- connect\_timeout => $seconds

    TCP/TLS connection timeout. `0` (default) means no timeout. Floating
    point allowed.

- query\_timeout => $seconds

    Default per-query timeout applied to every query and insert. The query
    callback receives a `timeout` error if exceeded. Override per-call via
    the `query_timeout` key in the settings hashref.

- max\_query\_size => $bytes

    Client-side guard: croak before sending any query whose SQL text exceeds
    this many bytes. `0` (default) disables the check. Useful as a
    last-resort defense against accidentally sending unbounded strings.

- max\_recv\_buffer => $bytes

    Defensive ceiling on the response. The cap applies to the raw recv
    buffer (every protocol), the chunked-decoded body (HTTP), and the
    gzip-decompressed body (HTTP), so the same upper bound applies to the
    user-visible payload regardless of transport encoding. On overflow the
    query callback receives an appropriate error ("recv buffer overflow",
    "chunked response too large", or "gzip body exceeds max\_recv\_buffer")
    and the connection is torn down so no subsequent query can slip past
    the cap on the same socket. `0` (default) keeps the historical
    no-cap behaviour (still bounded internally by a hard 128 MB ceiling
    on compressed paths). Recommended in production when the schema is
    constrained and you want a hard upper bound (e.g.
    `128 * 1024 * 1024` for 128 MB).

- http\_basic\_auth => 0 | 1

    HTTP only. When set, send credentials as
    `Authorization: Basic base64(user:password)` instead of the default
    `X-ClickHouse-User` / `X-ClickHouse-Key` header pair. Use this when
    the connection passes through an HTTP gateway (nginx, Envoy, ...) that
    strips the X-ClickHouse-\* headers but forwards Basic auth verbatim.
    Default: `0`.

- auto\_reconnect => 0 | 1

    Reconnect automatically on connection loss. Default: `0`. When enabled,
    queued (unsent) queries are preserved across reconnects; in-flight queries
    receive an error.

    The reconnect path covers TCP/TLS connect failures, `connect_timeout`
    or `query_timeout` expiry, and any clean server-side EOF (idle or
    mid-request). Mid-query I/O errors (ECONNRESET / EPIPE) and a malformed
    native ServerHello are **not** retried - they typically indicate a
    misconfigured peer or client-side bug that retry would only loop on.
    Combine with `reconnect_max_attempts` for an explicit ceiling.

- settings => \\%hash

    ClickHouse settings applied to every query and insert. Per-call settings
    (see ["query"](#query), ["insert"](#insert)) override these.

        settings => { async_insert => 1, max_threads => 4 }

- keepalive => $seconds

    Send a keepalive request every N seconds while the connection is idle:
    a native CLIENT\_PING on the native protocol or a `GET /ping` on HTTP
    (some load balancers / NATs drop idle HTTP connections after a few
    seconds; TCP-level keepalive is too coarse). Default: `0` (disabled).

- reconnect\_delay => $seconds

    Initial delay for the `auto_reconnect` exponential backoff. Each failed
    attempt doubles the delay, capped at `reconnect_max_delay`. Default:
    `0` (immediate retry, no backoff).

- reconnect\_max\_delay => $seconds

    Backoff ceiling. Default: `0`, meaning no explicit cap; the implementation
    still bounds the backoff exponent at 20 doublings, so with
    `reconnect_delay = 0.5` the worst case is roughly 6 days. Setting an
    explicit ceiling is recommended in production.

- reconnect\_jitter => $fraction

    Multiplicative jitter applied to each backoff delay: the actual sleep
    is uniformly random in `[delay, delay * (1 + jitter)]`. `0` (default)
    disables. Set to `0.1`-`0.5` when many clients reconnect against a
    shared cluster - without jitter, every replica restart causes a
    synchronised reconnect storm at the same backoff intervals. Jitter is
    applied _after_ `reconnect_max_delay` clamping, then re-clamped, so
    the ceiling is never exceeded.

- reconnect\_max\_attempts => $N

    Cap the total number of reconnect attempts before giving up. Once the
    cap is reached, `on_error` fires with the message
    `"max reconnect attempts exceeded"` and no further attempts are made
    (the user can manually call `reset` later). Default: `0` (unlimited
    retries; be careful with permanent failures like wrong host).

- progress\_period => $seconds

    Coalesce `on_progress` packets so the callback fires at most once per
    N seconds, with the per-field counters accumulated over the interval.
    Useful for big SELECTs where the server can emit hundreds of progress
    packets per second. Default: `0` (fire on every packet).

- query\_log\_comment => 1 | $string

    Prepend a SQL block comment to every query for `system.query_log`
    traceability. `1` auto-generates `ev_ch user=$ENV{USER} pid=$$`;
    a string is taken literally. Omit (or pass a falsy value) to disable.
    Embedded `*/` sequences are escaped to keep the comment well-formed.

**Decode options (native protocol only):**

These shape how column values are returned. All are opt-in and default
to `0`, which returns raw numeric forms for stable round-tripping.

- decode\_datetime => 0 | 1

    Return `Date`, `Date32`, `DateTime`, and `DateTime64` as formatted
    strings (e.g. `"2024-01-15"`, `"2024-01-15 10:30:00"`) instead of raw
    integers. Uses UTC; columns with an explicit timezone
    (`DateTime('America/New_York')`) are converted to that zone.

- decode\_decimal => 0 | 1

    Return `Decimal32`/`Decimal64`/`Decimal128` as scaled floating-point
    numbers instead of unscaled integers. Note: at large precisions, double
    loses bits, so leave disabled if you need exact arithmetic.

- decode\_enum => 0 | 1

    Return `Enum8`/`Enum16` as string labels instead of numeric codes.

- named\_rows => 0 | 1

    Return each row as a hashref keyed by column name instead of an arrayref.

        my $ch = EV::ClickHouse->new(named_rows => 1, ...);
        $ch->query("select 1 as n", sub {
            my ($rows, $err) = @_;
            print $rows->[0]{n};  # 1
        });

# METHODS

## query

    $ch->query($sql, sub { my ($rows, $err) = @_ });
    $ch->query($sql, \%settings, sub { my ($rows, $err) = @_ });

Executes a SQL statement. The callback receives:

- `($arrayref_of_arrayrefs)` for select with at least one row
- `(undef)` for DDL/DML on success and for select with zero rows
(both protocols). When in doubt, treat `undef` and `[]` equivalently
with `my @rows = @{$rows // []};`.
- `(undef, $error_message)` on error (server exception or
connection error)

The optional `\%settings` hashref passes per-query ClickHouse settings
(`max_execution_time`, `max_threads`, `async_insert`, etc.), overriding
connection-level defaults.

The following keys are intercepted by the client and not sent verbatim
to the server:

- `params =` \\%hash>

    Parameterized values for `{name:Type}` placeholders in the SQL. Encoding
    and quoting is the server's job, so values do not need escaping:

        $ch->query(
            "select * from t where id = {id:UInt64} and name = {n:String}",
            { params => { id => 42, n => "O'Brien" } },
            sub { ... },
        );

    Works on both protocols (HTTP uses URL-encoded `param_*` query string;
    native uses dedicated wire fields).

- `query_id =` $string>

    Set the protocol-level query identifier. Retrievable later via
    ["last\_query\_id"](#last_query_id).

- `raw =` 1>

    HTTP only. The callback receives the raw response body as a scalar string
    instead of parsed rows. Use with an explicit `format` clause:

        $ch->query("select * from t format CSV", { raw => 1 }, sub {
            my ($body, $err) = @_;
        });

    Croaks if used with the native protocol.

- `query_timeout =` $seconds>

    Per-query timeout, overriding the connection-level `query_timeout`.

- `on_data =` sub { my ($rows) = @\_; ... }>

    Native protocol only. A code ref called for each data block as it arrives,
    for streaming large result sets. Rows are delivered incrementally and
    **not** accumulated, so the final callback receives `(undef)` rather than
    all rows. The final callback always fires on completion or error, even if
    no data block was emitted (empty result, server-side error before the
    first block).

        $ch->query("select * from big_table",
            { on_data => sub { my ($rows) = @_; process_batch($rows) } },
            sub { my (undef, $err) = @_; warn $err if $err },
        );

**Native protocol type notes:** values come back as typed Perl scalars.
By default `Date`/`DateTime` are integers (days since epoch / Unix
timestamps); enable `decode_datetime` for strings. `Enum` values are
numeric codes; `decode_enum` returns labels. `Decimal` values are
unscaled integers; `decode_decimal` scales them to floats.
`SimpleAggregateFunction` is transparently decoded as its inner type.
`Nested` columns become arrays of tuples. `LowCardinality` works
correctly across multi-block results with shared dictionaries.

## insert

    $ch->insert($table, $data, sub { my (undef, $err) = @_ });
    $ch->insert($table, $data, \%settings, sub { my (undef, $err) = @_ });

`$data` may be either:

- A pre-formatted TabSeparated string (tabs separate columns,
newlines separate rows, with the standard ClickHouse escapes).
- An arrayref of arrayrefs (rows of column values).

When using arrayrefs, no TSV escaping is needed: `undef` maps to null
and strings may contain tabs and newlines freely.

Nested arrayrefs (Array/Tuple columns) and hashrefs (Map columns) are
supported **only on the native protocol**, where the encoder has the
column type from the server's sample block. On HTTP the same call
croaks rather than silently produce malformed TSV; use the native
protocol or pre-serialise nested types into ClickHouse TSV literal form.

    # Native: nested types encode directly.
    $ch->insert("my_table", [
        [1, "hello\tworld"],   # embedded tab
        [2, undef],            # null
        [3, [10, 20]],         # Array column   (native only)
        [4, { a => 1, b => 2 }],  # Map column  (native only)
    ], sub { ... });

The optional `\%settings` hashref works exactly as in ["query"](#query),
including `query_id`, `query_timeout`, and `params`. Two extra
flags are recognised here:

- `idempotent => 1 | $token`

    Auto-mints (or uses the supplied) `insert_deduplication_token`, so a
    reconnect-driven retry of the same INSERT doesn't double-write. Falsy
    values are a no-op.

- `async_insert => 1`

    Enables ClickHouse server-side INSERT batching by setting
    `async_insert=1, wait_for_async_insert=0`. Both sub-settings can be
    overridden by passing them explicitly.

## ping

    $ch->ping(sub { my ($result, $err) = @_ });

Send a no-op round trip to verify the connection is alive. On success
`$result` is true, `$err` is `undef`. On error: `(undef, $error)`.

## is\_healthy

    $ch->is_healthy(sub { my ($ok, $err) = @_ });
    $ch->is_healthy(sub { ... }, $timeout_seconds);

Bounded health probe: wraps ["ping"](#ping) with a deadline (default 5s). The
callback receives `(1, undef)` on a successful round trip, or
`(0, $msg)` on ping error or timeout. Failure does **not** tear down the
connection; recovery (`reset`, host rotation, etc.) is the caller's
choice. Useful for L4 load-balancer probes and self-monitoring loops.

## is\_retryable\_error

    EV::ClickHouse->is_retryable_error($code)   # class method
    $ch->is_retryable_error($code)              # also works on instance

Returns true if the given ClickHouse error code (as reported by
["last\_error\_code"](#last_error_code) or the per-query `$err` argument's prefix) is a
common transient failure that warrants automatic retry: timeouts,
network errors, memory pressure, replica catch-up, keeper exceptions,
etc. Authoritative-looking source list curated against ClickHouse's
`src/Common/ErrorCodes.cpp`; expect the set to grow conservatively.

    $ch->query($sql, sub {
        my ($r, $err) = @_;
        if ($err && EV::ClickHouse->is_retryable_error($ch->last_error_code)) {
            schedule_retry($sql);
        }
    });

## server\_supports

    $ch->server_supports($feature_name)

Returns true if the live native server's protocol revision is high
enough to support the given feature. Feature names map to documented
protocol-revision thresholds so user code can branch cleanly on
capability instead of hard-coding revision numbers. Supported names:

    block_info           51903   block_info packet in DATA blocks
    server_display_name  54372   ServerHello carries display name
    version_patch        54401   ServerHello carries patch version
    progress_writes      54420   Progress packets include write counters
    server_timezone      54423   Server timezone string in ServerHello
    addendum             54458   Native ClientHello addendum block

HTTP connections have no protocol revision (`server_revision` is `0`),
so `server_supports` returns false on HTTP for any feature. Unknown
feature names also return false. Use `server_revision` directly if you
need the raw integer.

## for\_table

    $ch->for_table('events', sub {
        my ($info, $err) = @_;
        die $err if $err;
        for my $col (@{ $info->{columns} }) {
            printf "%-20s %s\n", $col->{name}, $col->{type};
        }
    });

Schema introspection: issues `describe table $name` and delivers
`{ columns => [{name=>..., type=>...}, ...] }` to the
callback. Useful for generic insert pipelines that need column types
without hard-coding them. `$name` may be `table` or `db.table`;
non-identifier characters are rejected up-front.

## iterate

    my $it = $ch->iterate("select number from numbers(1_000_000)");
    while (my $batch = $it->next($timeout)) {
        process($_) for @$batch;
    }
    die $it->error if $it->error;

**Native protocol only** - relies on the per-block `on_data` hook and
will croak if invoked on an HTTP connection.

Synchronous-feeling pull iterator over a streaming select. Internally
wraps the native `on_data` per-block callback and drives the EV loop
from inside `->next` until the next block arrives, the query
completes, or the optional timeout (seconds) expires. Useful for
procedural ETL / export code that doesn't fit a callback shape.

`->error`, `->is_done`, and `->cancel` are also
available on the returned iterator object.

## on\_query\_complete

    $ch->on_query_complete(sub {
        my ($query_id, $rows, $bytes, $error_code, $duration_s, $err) = @_;
        log_metric(...);
    });

Optional connection-level hook that fires after every query (success
or error). Arguments: query\_id (or undef), profile\_rows, profile\_bytes,
last\_error\_code, wall-clock duration in seconds, error message (or
undef). Useful for statsd/Prometheus-style instrumentation. Also
accepted as a constructor argument.

A per-query override may be passed in the `\%settings` hashref of
["query"](#query) or ["insert"](#insert). When set, it **replaces** (does not augment)
the connection-level handler for that single call, so per-query
instrumentation doesn't double-count against global metrics:

    $ch->query(
        $sql,
        { on_query_complete => sub {
              my ($qid, $rows, $bytes, $code, $dur, $err) = @_;
              record_slow_query($qid, $dur);
        } },
        $cb,
    );

## insert\_streamer

    my $s = $ch->insert_streamer('events',
        batch_size     => 5_000,
        settings       => { query_id => 'ingest-1' },     # optional
        on_batch_error => sub { warn "batch err: $_[0]" }, # per-failure
    );
    while (my $row = next_event()) {
        $s->push_row($row);
    }
    $s->finish(sub {
        my (undef, $err) = @_;
        die "ingest failed: $err" if $err;
    });

Buffered streaming insert for ETL workloads. Rows are buffered until
`batch_size` is reached, then dispatched as a single `insert()`.
Dispatches are serialised; push\_row keeps buffering while a batch is
in flight (the native protocol cannot pipeline INSERTs). `finish`
flushes the remaining buffer and fires its callback once all batches
complete; if any batch failed the first error is delivered as
`$err`. The streamer also offers `buffered_count` and `in_flight`
accessors for backpressure logic.

`$streamer->reset` discards any rows still in the local buffer
and clears the sticky error so the streamer can be reused after a
permanent error (e.g. a schema fix). Does **not** touch the underlying
`$ch` - any batch already on the wire still completes normally.

`high_water` + `on_high_water` trigger a one-shot notification when
the buffered row count crosses the watermark, intended as a hint to
slow the producer. Set `high_water` below `batch_size`; if
`high_water >= batch_size`, the buffer drains via `batch_size`
flushes before the watermark is observed and `on_high_water` never
fires. The notification re-arms only after the buffer drops below
`high_water`.

**Named-row mode:** pass `columns => [@col_names]` at construction
to accept hashref rows instead of positional arrayrefs. The streamer
reorders each pushed hash into the declared column order, so producer
code does not have to know where each column lives in the table.

    my $s = $ch->insert_streamer('events',
        columns    => [qw(ts user_id action payload)],
        batch_size => 5_000,
    );
    $s->push_row({ user_id => 7, action => 'click', ts => time });
    $s->push_row([ 1735, 7, 'view', '...' ]);   # arrayref still works

Hash keys missing from a row become `undef`; extra keys are ignored.
Mixing arrayref and hashref pushes is allowed.

## EV::ClickHouse::Pool

    my $pool = EV::ClickHouse::Pool->new(
        host => 'ch', port => 9000, protocol => 'native',
        size => 8,                # other %args pass through to ::new
    );
    $pool->query($sql, $cb);
    $pool->insert($table, $data, $cb);
    $pool->drain(sub { ... });    # all connections drained
    $pool->finish;

Built-in connection pool. Each member is an independent
`EV::ClickHouse` with its own `auto_reconnect`, send queue, and
in-flight callback queue, so a hung query on one connection doesn't
block the others. Dispatch picks the least-busy connection; ties are
broken round-robin.

The Pool exposes per-pick dispatch via `query`, `insert`, `ping`,
`for_table`, `iterate`, `insert_streamer`; aggregate stats via
`size`, `pending_count`, `conns` (the underlying connection list);
and broadcast lifecycle methods `drain`, `finish`, `cancel`,
`skip_pending`, `reset` (each affects every member because the state
they touch is owned per connection, not per query). The broadcast
`cancel`, `skip_pending`, and `reset` methods wrap each per-member
call in `eval` so a member that croaks doesn't abort the broadcast;
per-member errors are silently discarded (the surviving members still
receive the call). Iterate `conns` yourself if you need per-member
error handling.

Queries that need server-side state (temporary tables, session
variables) must use a single connection, not a Pool, since successive
calls may land on different members.

**Circuit breaker:** pass `circuit_threshold => N` at construction
to enable per-member fail-fast. After N consecutive query/insert/ping
errors on a given member, that member is excluded from `_pick` for
`circuit_cooldown` seconds (default 30). A successful callback resets
the per-member fail counter. If every member is dead at pick time the
breaker is bypassed so the next attempt still has a chance to recover.
Inspect with `$pool->circuit_state` which returns one
`{ fails => N, dead_until => $epoch, alive => 0|1 }`
hashref per member.

**Graceful shutdown:** `$pool->shutdown($grace_seconds, $cb)`
drains every member, then calls `finish` on each. If `$grace_seconds`
elapses before every member drains, members still in flight are
force-finished and `$cb` receives the string
`"Pool::shutdown timed out after Ns"`. On a clean shutdown `$cb`
receives undef. `$grace_seconds` may be 0 (or undef) to wait
indefinitely. The callback fires exactly once.

    $SIG{TERM} = sub { $pool->shutdown(10, sub { EV::break }) };

## finish

    $ch->finish;

Close the connection. Pending queries receive an error callback. Aliased
as `disconnect`.

## reset

    $ch->reset;

Disconnect and immediately reconnect using the original parameters.
Aliased as `reconnect`.

## drain

    $ch->drain(sub { ... });

Register a callback to fire once all pending queries (queued + in-flight)
have completed. If nothing is pending, the callback fires synchronously.
The classic graceful-shutdown pattern:

    $ch->query("select 1", sub { ... });
    $ch->query("select 2", sub { ... });
    $ch->drain(sub {
        $ch->finish;
        EV::break;
    });

## cancel

    $ch->cancel;

Cancel the currently in-flight query. Native protocol sends CLIENT\_CANCEL
and waits for the server's EndOfStream/Exception; HTTP closes the connection
(use `auto_reconnect` or call ["reset"](#reset) to recover). The query's callback
receives an error.

## skip\_pending

    $ch->skip_pending;

Drop every pending operation: each queued and in-flight callback is invoked
with `(undef, $error_message)`. If a request was on the wire, the connection
is torn down; call ["reset"](#reset) (or rely on `auto_reconnect`) before issuing
new queries.

# ACCESSORS

All per-query accessors (`column_names`, `column_types`, `last_query_id`,
`last_error_code`, `last_totals`, `last_extremes`, `profile_rows`,
`profile_bytes`, `profile_rows_before_limit`) are reset at the moment a
new query is dispatched (queued or sent), _not_ when its callback fires.
It is always safe to read them inside the query's own callback. Reading
them after dispatching a subsequent query but before its callback fires
returns the initial state (0 or `undef`), never the previous query's
data. Connection-level accessors (`is_connected`, `server_info`,
`server_version`, `server_timezone`, `pending_count`) are unaffected.

- is\_connected

    True if the connection is established.

- current\_host

    The host the connection is presently pointed at as a string. After a
    multi-host failover rotation, this reflects the new target rather than
    the originally-supplied one.

- current\_port

    The port the connection is presently pointed at as an integer.

- server\_revision

    The native protocol revision the server reports in its ServerHello,
    as a positive integer (e.g. `54459`). `0` before the handshake
    completes and for HTTP connections (which have no native handshake).
    Use ["server\_supports"](#server_supports) for named-capability checks; this raw integer
    is the escape hatch when you need to compare against a specific
    revision number from the ClickHouse source.

- pending\_count

    Number of pending operations (queued + in-flight).

- server\_info

    Full server identification string (e.g. `"ClickHouse 24.1.0 (revision 54459)"`),
    populated from the native ServerHello. `undef` for HTTP connections.

- server\_version

    Server version (e.g. `"24.1.0"`). Native only; `undef` for HTTP.

- server\_timezone

    Server timezone (e.g. `"UTC"`, `"Europe/Moscow"`). Native only; `undef`
    for HTTP.

- column\_names

    Arrayref of column names from the most recent native query result, or
    `undef` if no query has run. Native protocol only - HTTP responses
    do not carry column metadata.

        $ch->query("select 1 as foo, 2 as bar", sub {
            my $names = $ch->column_names;  # ['foo', 'bar']
        });

- column\_types

    Arrayref of ClickHouse type strings from the most recent native query
    (e.g. `['UInt32', 'String', 'Nullable(DateTime)']`). Native protocol
    only - `undef` on HTTP.

- last\_query\_id

    `query_id` of the most recently dispatched query, or `undef`. Set via
    `{ query_id => 'my-id' }` in the settings hash of ["query"](#query)/["insert"](#insert).

- last\_error\_code

    ClickHouse error code (integer) of the most recent server-side exception,
    or `0` if no error. The **top-level** code is reported even when the
    exception is a chain. Useful for distinguishing retryable errors (e.g.
    `202` = `TOO_MANY_SIMULTANEOUS_QUERIES`) from permanent ones (`60` =
    `UNKNOWN_TABLE`, `516` = `AUTHENTICATION_FAILED`).

- last\_totals

    Arrayref of totals rows from the last query that used `with totals`,
    or `undef`. Native only.

- last\_extremes

    Arrayref of extremes rows from the last native query, or `undef`.

- profile\_rows\_before\_limit

    Rows that would have been returned without `limit`. Useful for pagination
    UIs. Native only.

- profile\_rows

    Total rows processed by the last query. Populated from the native
    ProfileInfo packet on the native protocol, or from `X-ClickHouse-Summary`
    (`read_rows`) on HTTP.

- profile\_bytes

    Total bytes processed by the last query. Populated from the native
    ProfileInfo packet on the native protocol, or from `X-ClickHouse-Summary`
    (`read_bytes`) on HTTP.

# ALIASES

    q          -> query
    reconnect  -> reset
    disconnect -> finish

# REQUIREMENTS

- Perl 5.12 or newer
- [EV](https://metacpan.org/pod/EV) 4.11 or newer (event loop)
- zlib (required)
- OpenSSL (optional, for TLS; auto-detected at build time)
- liblz4 (optional, for native protocol compression; auto-detected)

# TROUBLESHOOTING

- AUTHENTICATION\_FAILED on the first query

    The native handshake authenticates lazily; the first query is what surfaces
    a bad `user`/`password`. Check the server's `users.xml` and the URI form
    `clickhouse://user:pass@host:port/db`.

- DateTime returns a number, not a string

    `DateTime`/`Date` decode to raw integers (Unix epoch / days since epoch)
    by default for stable round-tripping. Pass `decode_datetime => 1` to get
    ISO-formatted strings.

- ClickHouse error `UNKNOWN_DATABASE` on connect

    The `database` argument is sent as the default; the server must already
    have that database. Use `database => 'default'` while bootstrapping.

- Insert silently dropped (counts don't match)

    Likely `insert_deduplication_token` dedupe; either you're reusing a token
    across distinct batches, or the table is `ReplicatedMergeTree` with the
    default dedupe window. See `eg/idempotent_insert.pl`.

- Hangs on connect when host is a hostname

    Without [EV::cares](https://metacpan.org/pod/EV%3A%3Acares), DNS resolution falls back to blocking
    `getaddrinfo`. Install [EV::cares](https://metacpan.org/pod/EV%3A%3Acares) for non-blocking lookup; otherwise
    use an IP literal or a local caching resolver (nscd / systemd-resolved).

- `connect_timeout` doesn't fire

    It does across TCP connect, TLS handshake, and native ServerHello. If
    the timer doesn't fire, the underlying issue is usually a synchronous
    DNS stall (see above) which happens before `start_connect` arms the
    timer; install [EV::cares](https://metacpan.org/pod/EV%3A%3Acares) to push DNS off the loop.

- Per-query `query_timeout` is ignored

    Set it inside the `\%settings` hashref, not as a top-level argument:
    `$ch->query($sql, { query_timeout => 5 }, $cb)`.

- Which host am I currently pointed at after failover?

    `$ch->current_host` and `$ch->current_port` reflect the
    live target after a multi-host rotation. Use `on_failover =>
    sub { ... }` to get notified at the moment of each rotation.

- How do I retry only on transient errors?

    `EV::ClickHouse->is_retryable_error($code)` returns true for the
    common transient codes (timeouts, network errors, replica catch-up,
    keeper exceptions, ...). Inspect `$ch->last_error_code` from
    inside your query callback and schedule a retry only when the predicate
    fires - permanent errors (auth failures, missing tables) won't qualify.

    Sample skeleton:

        $ch->query($sql, sub {
            my ($r, $err) = @_;
            if ($err && EV::ClickHouse->is_retryable_error($ch->last_error_code)) {
                schedule_retry($sql);
            } elsif ($err) { warn "permanent: $err" }
        });

- Idempotent insert silently drops some rows

    `idempotent => 1` auto-mints
    `insert_deduplication_token`; if your producer issues the SAME logical
    batch twice (e.g. retry after a transient network blip) only the first
    write lands, by design. To force two distinct logical batches through,
    either pass an explicit `idempotent => $token` per batch or
    omit the option for fresh inserts. See `eg/idempotent_insert.pl`.

- `on_data` vs `iterate` - which should I pick?

    `on_data => sub { }` in the per-query settings is the
    lowest-overhead streaming path: each native data block is delivered as
    soon as the parser has it, no per-row allocation overhead beyond the
    batch arrayref. `iterate` is a synchronous-feeling pull wrapper around
    the same machinery - useful when the surrounding code is procedural
    (ETL scripts, exporters) and a callback shape doesn't fit. Both are
    native-only.

- Connection in front of nginx / reverse proxy strips X-ClickHouse-\* headers

    Pass `http_basic_auth => 1` to send the credentials as
    `Authorization: Basic ...` instead. Most HTTP gateways forward
    Authorization verbatim while filtering proprietary headers.

# TUNING

- Native vs HTTP

    Native (port 9000) is typically 2-5x faster for insert and select-of-many-rows
    because rows ship as binary columns instead of TSV text. Use HTTP only when
    the network path requires HTTPS-only or when you need `raw => 1` CSV /
    JSONEachRow / Parquet bodies.

- `compress => 1`

    Enables LZ4 (native) or gzip (HTTP). LZ4 cost is small and saves ~50-70%
    on text-heavy columns. Gzip is heavier; turn on only if you're bandwidth-bound.

- `insert_streamer` batch\_size

    Default 10\_000 is a good baseline. Smaller (1k-2k) reduces memory pressure
    on the producer; larger (50k-100k) reduces server-side merge cost on
    MergeTree. Match to your row width: ~1 MB per batch is a sweet spot.

- `keepalive`

    Enable on long-lived idle connections (HTTP behind a load balancer or
    NAT, or a native connection that may sit minutes between queries). 15-30s
    is typical.

- `reconnect_max_attempts`

    Always set in production. Default is unlimited; a permanent failure
    (wrong host, wrong port, dead server) will spin `on_error` forever
    otherwise.

- `progress_period`

    Coalesce on\_progress packets to one fire per N seconds. Big SELECTs can
    emit hundreds per second; throttle to 1-5s for monitoring dashboards.

- Pull-iterator vs `on_data`

    `on_data` has lower per-block overhead. `iterate` trades that for a
    synchronous-feeling API; use it when the surrounding code is procedural.

- `EV::ClickHouse::Pool`

    A Pool fans concurrent queries across N independent connections, so a
    slow query on one doesn't head-of-line-block the others. Use it for
    read-mostly fan-out; do not use it for queries that depend on
    session-level state (temporary tables, `set`) since each query may
    land on a different connection.

# ARCHITECTURE

The client is a single state machine driven by an [EV](https://metacpan.org/pod/EV) event loop. Each
connection holds: a TCP fd (non-blocking), a send buffer, a receive
buffer, a callback queue (next-in-line per protocol), and a pending
send queue (buffered before connect).

State transitions:

    Connect TCP --> [TLS handshake] --> [Native ServerHello]
        --> Connected --> { dispatch from send_queue;
                            parse response; deliver via cb_queue }

The connect\_timeout timer covers all three pre-Connected stages.
auto\_reconnect re-runs the chain via `schedule_reconnect`.

Two key invariants:

- Native protocol is strictly request/response. Only one query is
in-flight per connection at a time. `insert_streamer` serialises
batches against this constraint.
- `callback_depth` guards against `self` being freed mid-callback.
Every callback dispatch increments it; `check_destroyed` defers the
final `Safefree` until depth returns to zero.

For deeper detail (state-machine table, queue semantics) see `CLAUDE.md`
in the source distribution.

# TYPES

Per-column wire format and Perl-side gotchas. All numeric types
round-trip stable raw values by default; opt into string forms via
`decode_datetime`, `decode_decimal`, `decode_enum`.

- Integers

    Int8..Int64 / UInt8..UInt64: native Perl IV/UV. Int128/UInt128/Int256/UInt256
    return decimal string representations on platforms with `__int128` (Int128/UInt128)
    or always for the 256-bit forms.

- Floats

    Float32/Float64 round-trip exactly within IEEE-754 limits. `NaN`/`+Inf`/
    `-Inf` are preserved.

- BFloat16

    Top 16 bits of a Float32. Encoded by truncation; decoded by zero-extension.
    Suitable for ML feature columns; not for accounting.

- Decimal32/64/128

    Decoded as IV (raw integer) or NV (scaled to N decimal digits if
    `decode_decimal => 1`). Decimal128 over very long precision may lose
    trailing digits in the NV form; pass `decode_decimal => 0` and divide
    yourself with [Math::BigInt](https://metacpan.org/pod/Math%3A%3ABigInt) for exact arithmetic.

- Decimal256

    Returns raw 32 LE bytes. Decode with [Math::BigInt](https://metacpan.org/pod/Math%3A%3ABigInt) (see
    `eg/decimal_bigmath.pl`).

- Date / Date32 / DateTime / DateTime64

    Default: integer (days since epoch / Unix seconds). With `decode_datetime`:
    `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD HH:MM:SS.ffffff`.
    DateTime carries a timezone string; the formatted output uses it.

- Bool

    Decoded as 0/1. Encoded from any truthy/falsy SV. ClickHouse stores
    internally as UInt8 0/1.

- String / FixedString

    Bytes-in, bytes-out. No UTF-8 transformation.

- UUID

    Canonical hex form `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. Encode
    accepts the same.

- IPv4 / IPv6

    Dotted-quad / canonical IPv6 strings.

- Enum8 / Enum16

    Default: integer code. With `decode_enum => 1`: label string.

- Nullable(T)

    `undef` in Perl maps to null; otherwise the inner type's encoding.

- Array(T)

    Perl arrayref of inner-type values.

- Tuple(T1, T2, ...)

    Perl arrayref ordered as the type declaration. Named tuples
    (`Tuple(a Int32, b String)`) are still arrayref-positional;
    parse the name from `column_types` if you need it.

- Map(K, V)

    Perl hashref. Keys are stringified.

- LowCardinality(T)

    Transparent: encodes/decodes as the inner type. Cross-block dictionaries
    are managed internally.

- SimpleAggregateFunction / AggregateFunction

    Decoded as the inner declared type (correct for sum/min/max/avg-ish
    functions). For complex states (quantile, uniqExact, ...) wrap the select
    with `finalizeAggregation(col)` server-side.

- Geo (Point/Ring/LineString/MultiLineString/Polygon/MultiPolygon)

    Decoded as the underlying nested arrayref/tuple shape.

- JSON / Object('json')

    Decoded as a Perl hashref with dotted-path leaves auto-unflattened to
    nested hashes. Encode accepts arbitrarily-nested hashrefs; supported
    leaf kinds are Int64, Float64, Bool (recognised JSON::PP::Boolean
    classes or `SvIsBOOL`), String, and Array(&lt;those>).

- Variant / Dynamic

    Recognised by the type parser so schema blocks containing these types
    do not abort, but the wire format itself is not decoded - reading a
    column of either type yields raw bytes. Wrap with `toString(col)` or
    cast to a concrete supported type server-side if you need a usable
    value.

- Interval (Second/Minute/Hour/Day/Week/Month/Quarter/Year)

    Decoded as Int64 (the unit count). The unit is implicit from the column
    type.

# COOKBOOK

The `eg/` directory in the source distribution carries runnable
patterns for the common production shapes. Each one is self-contained
and reads top-to-bottom.

- `eg/etl_pipeline.pl`

    Producer + Pool + ["insert\_streamer"](#insert_streamer) with `high_water` backpressure
    and `idempotent` tokens. The reliable-ingest baseline.

- `eg/health_probe.pl`

    Periodic ["is\_healthy"](#is_healthy) probe with bounded timeout, transition logging,
    and automatic ["reset"](#reset) on failure. Drop-in for self-monitoring.

- `eg/circuit_breaker.pl`

    Pool with `circuit_threshold` + `circuit_cooldown` shielding the
    rotation from a sticky bad member. Demonstrates `circuit_state`
    introspection.

- `eg/csv_export.pl`

    Streams a multi-million-row select to a CSV file via the per-block
    `on_data` hook (no full-result buffering). Mirrors the equivalent
    ["iterate"](#iterate) form in a comment.

- `eg/migration_runner.pl`

    Apply numbered SQL migration files in order, recording successes in a
    `_migrations` table and using `idempotent` on the registry insert
    so a partial apply doesn't leave the registry out of sync.

- `eg/failover.pl` + `eg/pool.pl`

    Multi-host failover and built-in connection pool - the reliability
    primitives the cookbook recipes layer on top of.

- `eg/async_dns.pl`

    Constructor returns immediately even for hostnames; queries queue
    behind [EV::cares](https://metacpan.org/pod/EV%3A%3Acares) resolution.

- `eg/idempotent_insert.pl`

    Auto-minted insert deduplication tokens that survive a reconnect-
    driven retry without double-writing.

# SEE ALSO

[EV](https://metacpan.org/pod/EV), [https://clickhouse.com/docs](https://clickhouse.com/docs)

# AUTHOR

vividsnow

# LICENSE

This library is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.
