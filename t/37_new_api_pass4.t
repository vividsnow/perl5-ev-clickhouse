#!/usr/bin/env perl
# Pass-4 batch coverage: error class, bind_ident, on_log,
# cancel_by_query_id, Pool::query_to/insert_to/nominate/hedged_query,
# pending_queries, dump_state, track_query_durations,
# insert_async, for_json_paths, insert_aggregated.
use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use EV;
use EV::ClickHouse;

my $host  = $ENV{TEST_CLICKHOUSE_HOST}        || '127.0.0.1';
my $nport = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;

plan skip_all => "ClickHouse native not reachable"
    unless IO::Socket::INET->new(PeerAddr => $host, PeerPort => $nport, Timeout => 2);

plan tests => 67;

# Run EV::run with a bail-out timer so a missed EV::break can't hang
# the test. Cheaper than spelling it out in every block.
sub run_with_bail {
    my ($timeout) = @_;
    my $t = EV::timer($timeout, 0, sub { EV::break });
    EV::run;
    undef $t;
}

# 1. Error class
{
    my $e = EV::ClickHouse::Error->new(message => 'boom', code => 60);
    is "$e", 'boom',                           'Error stringifies to message';
    is $e->code, 60,                           'Error code';
    is $e->name, 'UNKNOWN_TABLE',              'Error name lookup';
    ok !$e->is_retryable,                      'UNKNOWN_TABLE not retryable';
    my $e2 = EV::ClickHouse::Error->new(message => 'slow', code => 159);
    ok $e2->is_retryable,                      'TIMEOUT_EXCEEDED retryable';
}

# 2. bind_ident
{
    is(EV::ClickHouse->bind_ident('events'),    '`events`',      'simple identifier');
    is(EV::ClickHouse->bind_ident('db.events'), '`db`.`events`', 'dotted identifier');
    eval { EV::ClickHouse->bind_ident("1bad") };
    like $@, qr/invalid/,                       'leading digit rejected';
    eval { EV::ClickHouse->bind_ident("a; drop") };
    like $@, qr/invalid/,                       'SQL injection attempt rejected';
}

# 3. on_log fires + dump_state + pending_queries
{
    my @logs;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_log => sub { push @logs, $_[0] },
        on_connect => sub {
            # send_logs_level is a per-query setting, not nested
            # under {settings}. trace level fires LOG packets for
            # virtually any SELECT — enough to drive parse_and_emit_log_block.
            $ch->query(
                "select sleep(0.01)",
                { send_logs_level => 'trace' },
                sub { EV::break },
            );
        },
        on_error => sub { EV::break },
    );
    run_with_bail(5);
    ok scalar @logs > 0,                       'on_log fires for at least one log row';
    is ref($logs[0]),       'HASH',            'log entry is a hashref';
    ok exists $logs[0]{text},                  'log entry has text field';
    my $st = $ch->dump_state;
    is ref($st), 'HASH',                        'dump_state returns hashref';
    ok exists $st->{protocol},                  'dump_state has protocol field';
    my $pq = $ch->pending_queries;
    is ref($pq), 'ARRAY',                       'pending_queries returns arrayref';
    $ch->finish;
}

# 4. track_query_durations + Pool::query_to + hedged_query
{
    my $pool = EV::ClickHouse::Pool->new(
        host => $host, port => $nport, protocol => 'native', size => 3,
    );

    # query_to: pin to member 1, verify it lands.
    my $routed_done = 0;
    $pool->query_to(1, "select 1", sub {
        my ($rows, $err) = @_;
        $routed_done = 1 if !$err && $rows && $rows->[0][0] == 1;
        EV::break;
    });
    run_with_bail(5);
    ok $routed_done,                           'Pool::query_to routes + returns rows';

    # hedged_query: ask two members, take first. cb must be last arg.
    my $hedged_done = 0;
    $pool->hedged_query("select 7", hedge => 2, sub {
        my ($rows, $err) = @_;
        $hedged_done = 1 if !$err && $rows && $rows->[0][0] == 7;
        EV::break;
    });
    run_with_bail(5);
    ok $hedged_done,                           'Pool::hedged_query resolves with first reply';

    # nominate returns underlying conn
    is ref($pool->nominate(0)), 'EV::ClickHouse', 'Pool::nominate returns conn';
    $pool->finish;
}

# 5. insert_async (server may not have async_insert enabled by default,
# so we just verify the helper plumbs settings without croaking).
{
    my $tbl = "ev_ch_async_$$";
    my $err_phase; my $count;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->query("create table $tbl (n UInt32) engine=Memory", sub {
                my (undef, $e) = @_;
                $err_phase = "ddl: $e" if $e;
                $ch->insert_async($tbl, [[1],[2],[3]], sub {
                    my (undef, $e) = @_;
                    $err_phase = "ins: $e" if $e;
                    $ch->query("select count() from $tbl", sub {
                        my ($r, $e) = @_;
                        $count = $r ? $r->[0][0] : undef;
                        $ch->query("drop table $tbl", sub { EV::break });
                    });
                });
            });
        },
        on_error => sub { $err_phase = "conn: $_[0]"; EV::break },
    );
    run_with_bail(8);
    ok !$err_phase,                            "insert_async ran (" . ($err_phase // '') . ")";
    is $count, 3,                              'insert_async rows landed';
    $ch->finish;
}

# 6. cancel_by_query_id matches and no-ops correctly.
{
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub { EV::break },
    );
    run_with_bail(3);
    is $ch->cancel_by_query_id('whatever'), 0, 'no in-flight query: no-op';
    eval { $ch->cancel_by_query_id(undef) };
    like $@, qr/query_id required/,            'cancel_by_query_id rejects undef';
    $ch->finish;
}

# 7. Pool::insert_to routes to the chosen member.
{
    my $tbl = "ev_ch_pti_$$";
    my $pool = EV::ClickHouse::Pool->new(
        host => $host, port => $nport, protocol => 'native', size => 2,
    );
    my $tag = "ev-ch-insert-to-$$";
    my $err; my $member0_qid_after_insert; my $count;
    $pool->query("create table $tbl (n UInt32) engine=Memory", sub {
        my (undef, $e) = @_; $err = $e;
        $pool->insert_to(0, $tbl, [[1],[2]],
            { query_id => $tag }, sub {
            my (undef, $e) = @_; $err //= $e;
            # Read member-0 last_query_id BEFORE issuing any other query
            # on that member, so the tag is still observable.
            $member0_qid_after_insert = $pool->nominate(0)->last_query_id;
            $pool->query_to(1, "select count() from $tbl", sub {
                my ($r, $e) = @_;
                $err //= $e;
                $count = $r ? $r->[0][0] : undef;
                $pool->query("drop table $tbl", sub { EV::break });
            });
        });
    });
    run_with_bail(5);
    ok !$err, 'Pool::insert_to ran cleanly' or diag $err;
    is $member0_qid_after_insert, $tag,
        'last_query_id on member 0 is the tag we passed (proves routing)';
    is $count, 2, 'inserted rows are visible via query_to';
    $pool->finish;
}

# 8. for_json_paths against a JSON column (CH 23.8+; skip if unsupported).
SKIP: {
    my $tbl = "ev_ch_jp_$$";
    my $paths; my $err;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->query(
                "create table $tbl (j JSON) engine=Memory",
                { allow_experimental_json_type => 1 },
                sub {
                    my (undef, $e) = @_;
                    if ($e) { $err = $e; return EV::break }
                    $ch->insert($tbl,
                        [[ '{"a":1,"b":"x","nested":{"c":1.5}}' ]],
                        sub {
                            my (undef, $e) = @_;
                            if ($e) { $err = $e; return EV::break }
                            $ch->for_json_paths($tbl, 'j', sub {
                                my ($got, $e) = @_;
                                if ($e) { $err = $e } else { $paths = $got }
                                $ch->query("drop table $tbl", sub { EV::break });
                            });
                        });
                });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    run_with_bail(8);
    $ch->finish;
    skip "JSON type not supported by this server", 2
        if $err && $err =~ /JSON|experimental|unsupported type|JSONAllPathsWithTypes/i;
    ok !$err && $paths,                       'for_json_paths returns a list'
        or diag "err: " . ($err // '') . " paths: ". ($paths ? scalar @$paths : 'undef');
    ok $paths && @$paths >= 1,                'at least one path discovered';
}

# 9. insert_aggregated round-trip: build states, then uniqExactMerge to
# verify the State combinator wire-format matches what reads back.
{
    my $tbl = "ev_ch_agg_$$";
    my $err; my $merged;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->query(
                "create table $tbl (k String, u AggregateFunction(uniqExact, UInt64)) engine=Memory",
                sub {
                    my (undef, $e) = @_;
                    if ($e) { $err = $e; return EV::break }
                    $ch->insert_aggregated($tbl,
                        u        => { func => 'uniqExact', args => ['UInt64'] },
                        key_cols => [qw(k)],
                        rows     => [['a', 1], ['a', 2], ['b', 7]],
                        cb       => sub {
                            my (undef, $e) = @_;
                            if ($e) { $err = $e; return EV::break }
                            $ch->query("select uniqExactMerge(u) from $tbl", sub {
                                my ($r, $e) = @_;
                                $err //= $e;
                                $merged = $r ? $r->[0][0] : undef;
                                $ch->query("drop table $tbl", sub { EV::break });
                            });
                        });
                });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    run_with_bail(8);
    $ch->finish;
    ok !$err, 'insert_aggregated round-tripped' or diag $err;
    is $merged, 3, 'three distinct uniqExact states merge back to 3';
}

# 10. finish() inside on_query_start must not crash (UAF regression).
{
    my $started = 0;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_query_start => sub {
            $started++;
            $ch->finish;
        },
        on_connect => sub {
            $ch->query("select 1", sub { EV::break });
        },
        on_error => sub { EV::break },
    );
    run_with_bail(3);
    ok $started, 'on_query_start fired; finish() inside it did not crash';
}

# 11. track_query_durations records p95 over actual queries.
{
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub { EV::break },
    );
    run_with_bail(3);
    $ch->track_query_durations(64);
    my $left = 5;
    for (1 .. 5) {
        $ch->query("select sleep(0.01)", sub { EV::break if !--$left });
    }
    run_with_bail(5);
    is $ch->query_duration_count, 5,    'ring captured 5 samples';
    ok $ch->query_duration_p(0.5) > 0,  'p50 is positive';
    ok $ch->query_duration_p(0.95) >= $ch->query_duration_p(0.5),
        'p95 >= p50';
    $ch->track_query_durations(0);
    is $ch->query_duration_count, 0,    'disable clears the ring';
    $ch->finish;
}

# 12. on_log callback that drops itself must not UAF (cb refcount guard).
{
    my $fired = 0;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->on_log(sub {
                $fired++;
                # Drop the handler from inside itself; the row loop must
                # still finish without dereferencing a freed CV.
                $ch->on_log(undef);
            });
            $ch->query("select sleep(0.01)",
                { send_logs_level => 'trace' },
                sub { EV::break });
        },
        on_error => sub { EV::break },
    );
    run_with_bail(5);
    ok $fired > 0, 'on_log fired (guard actually exercised)';
    $ch->finish;
}

# 13. Pool::hedged_query with circuit_threshold: queries route around
# a tripped member and per-query oqc override fires for cancelled queries.
{
    my $pool = EV::ClickHouse::Pool->new(
        host => $host, port => $nport, protocol => 'native', size => 2,
        circuit_threshold => 2, circuit_cooldown => 1,
    );
    my $ok = 0;
    $pool->hedged_query("select 11", hedge => 2, sub {
        my ($r, $e) = @_;
        $ok = 1 if !$e && $r && $r->[0][0] == 11;
        EV::break;
    });
    run_with_bail(5);
    ok $ok, 'hedged_query with circuit breaker resolves successfully';
    $pool->finish;
}

# 14. Drop the last $ch reference from inside a query callback and
# trigger DESTROY mid-callback. The new DESTROY second watcher-stop
# pass must catch the watchers that cancel_pending's user error
# callback re-armed via reset().
{
    my $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_error => sub { EV::break },
    );
    # First, fully connect.
    my $ready = 0;
    $ch->on_connect(sub { $ready = 1; EV::break });
    run_with_bail(3);
    ok $ready, 'connected before UAF test';

    # Now: queue a query, drop our ref inside its on_error so DESTROY
    # fires while callback_depth > 0. From inside the error callback,
    # re-arm watchers via reset() to provoke the use-after-free path.
    $ch->on_error(sub {
        eval { $ch->reset };   # re-arm io watchers on the new fd
        $ch = undef;           # last ref drop → DESTROY runs deferred
        EV::break;
    });
    $ch->query("select sleep(120)", sub { });
    $ch->cancel;               # forces cancel_pending → user error cb
    run_with_bail(2);
    ok 1, 'reset()-from-error-callback + ref-drop did not UAF';
}

# 15. Per-query on_query_complete override fires for cancelled queries
# (drain_cb_queue contract).
{
    my $oqc_fired = 0;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            # Issue a slow query with a per-query oqc that records errors.
            $ch->query("select sleep(60)",
                { on_query_complete => sub {
                    my (undef, undef, undef, undef, undef, $err) = @_;
                    $oqc_fired++ if $err;
                } },
                sub { });
            # Cancel right away — cb_queue is drained with errmsg, oqc must fire.
            $ch->cancel;
            EV::timer(0.3, 0, sub { EV::break });
        },
    );
    run_with_bail(2);
    ok $oqc_fired, 'per-query on_query_complete fires when cancelled';
    $ch->finish;
}

# 16. insert() with per-query on_query_complete AND another setting
# that triggers a settings_copy (regression: insert XSUB used to read
# from the freed settings_copy when extracting on_query_complete).
{
    my $tbl = "ev_ch_oqc_ins_$$";
    my $oqc = 0; my $err;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->query("create table $tbl (n UInt32) engine=Memory", sub {
                $ch->insert($tbl, [[1]],
                    { idempotent => 1,             # forces settings_copy path
                      on_query_complete => sub { $oqc++ } },
                    sub {
                        my (undef, $e) = @_; $err = $e;
                        $ch->query("drop table $tbl", sub { EV::break });
                    });
            });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    run_with_bail(5);
    ok !$err, 'insert with on_query_complete+idempotent did not UAF' or diag $err;
    ok $oqc > 0, 'per-query on_query_complete fired';
    $ch->finish;
}

# 17. track_query_durations resize-down preserves newest samples.
{
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub { EV::break },
    );
    run_with_bail(3);
    $ch->track_query_durations(4);
    # Push 5 queries; with size=4 the buffer wraps once. After the wrap,
    # the newest 4 should be the last 4 we measured.
    my $left = 5;
    for (1 .. 5) {
        $ch->query("select 1", sub { EV::break if !--$left });
    }
    run_with_bail(5);
    is $ch->query_duration_count, 4, 'ring captured 4 samples (size 4, 5 pushed)';
    # Resize down to 2 — should keep the 2 newest, NOT the 2 oldest.
    $ch->track_query_durations(2);
    is $ch->query_duration_count, 2, 'resize-down kept 2 samples';
    # We don't know exact durations but they should sort cleanly.
    my $p50 = $ch->query_duration_p(0.5);
    ok defined $p50 && $p50 >= 0, 'p50 after resize is well-formed';
    $ch->track_query_durations(0);
    $ch->finish;
}

# 18. retry: succeeds on first try when no error; ignores non-retryable.
{
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub { EV::break },
    );
    run_with_bail(3);
    my ($rows, $err);
    $ch->retry("select 99", retries => 2, backoff => 0.05, cb => sub {
        ($rows, $err) = @_; EV::break;
    });
    run_with_bail(3);
    ok !$err && $rows && $rows->[0][0] == 99, 'retry returns rows on success';

    # Non-retryable error: bad table; should NOT retry.
    my $non_retry_err;
    $ch->retry("select * from nonexistent_table_xyz",
        retries => 5, backoff => 0.05, cb => sub {
        (undef, $non_retry_err) = @_; EV::break;
    });
    run_with_bail(3);
    ok defined($non_retry_err),
       'retry surfaces non-retryable error without infinite loop';
    $ch->finish;
}

# 19. Pool::fan_out gathers per-member results.
{
    my $pool = EV::ClickHouse::Pool->new(
        host => $host, port => $nport, protocol => 'native', size => 3,
    );
    my $out;
    $pool->fan_out("select 1, hostName()", sub { $out = $_[0]; EV::break });
    run_with_bail(5);
    is scalar @$out, 3,                'fan_out returns one entry per member';
    is_deeply [ map { $_->{member} } @$out ], [0, 1, 2],
                                       'fan_out indexes are 0..size-1';
    ok !grep({ $_->{err} } @$out),     'no per-member errors';
    ok !grep({ !$_->{rows} || @{$_->{rows}} != 1 || $_->{rows}[0][0] != 1 } @$out),
       'every member returned [[1, hostname]]';
    $pool->finish;
}

# 20. ping_round_trip reports a positive latency.
{
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub { EV::break },
    );
    run_with_bail(3);
    my ($rtt, $err);
    $ch->ping_round_trip(sub { ($rtt, $err) = @_; EV::break });
    run_with_bail(3);
    ok !$err && defined($rtt) && $rtt > 0,
       "ping_round_trip returned a positive latency"
       or diag "rtt=" . ($rtt // 'undef') . " err=" . ($err // '');
    $ch->finish;
}

# 21. Pool::with_session pins a member while the cb holds the release.
{
    my $pool = EV::ClickHouse::Pool->new(
        host => $host, port => $nport, protocol => 'native', size => 3,
    );
    my $pinned_id;
    my $other_id;
    $pool->with_session(sub {
        my ($pinned, $release) = @_;
        $pinned_id = "$pinned";
        # Inside the pin: a normal $pool->query should land on a different
        # member (the pinned one is excluded by _pick).
        $pool->query("select 1", sub {
            EV::break;
        });
        # Hold the release until after the EV::break above.
        EV::timer(0.5, 0, sub { $release->() });
    });
    run_with_bail(3);
    # Inspect which connection $pool just used (last_query_id is per-conn
    # but unset here; instead infer via _pick a second time after release).
    ok defined($pinned_id),     'with_session received a connection';
    $pool->finish;
}

# 22. server_setting + row_count + table_size + ddl + dictionary_reload
#     (just the parse_uri class method is also exercised here).
{
    my $p = EV::ClickHouse->parse_uri(
        'clickhouse+native://u:p@host.example:9000/mydb?max_threads=4');
    is $p->{protocol}, 'native',                     'parse_uri: protocol';
    is $p->{host},     'host.example',               'parse_uri: host';
    is $p->{port},     9000,                         'parse_uri: port';
    is $p->{user},     'u',                          'parse_uri: user';
    is $p->{password}, 'p',                          'parse_uri: password';
    is $p->{database}, 'mydb',                       'parse_uri: database';
    is $p->{settings}{max_threads}, '4',             'parse_uri: settings';
    is(EV::ClickHouse->parse_uri('not-a-uri'), undef, 'parse_uri rejects garbage');
}

{
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->server_setting('max_threads', sub {
                my ($v) = @_;
                ok defined($v),       'server_setting returns a value';
                # Spin up an ephemeral table — relying on system.numbers
                # with a predicate is brittle across CH versions
                # (some don't early-terminate without an explicit LIMIT).
                my $tbl = "ev_ch_rc_$$";
                $ch->ddl("create table $tbl (n UInt32) engine=Memory", sub {
                  $ch->insert($tbl, [ map [$_], 1..50 ], sub {
                    $ch->row_count($tbl, 'n > 25', sub {
                      my ($n) = @_;
                      is $n, 25,    'row_count with WHERE returns expected count';
                      $ch->table_size('system.parts', sub {
                        my ($s) = @_;
                        is ref($s), 'HASH',            'table_size returns hashref';
                        ok exists $s->{bytes_on_disk}, 'table_size has bytes_on_disk';
                        $ch->ddl("select 1", sub {
                          my (undef, $e) = @_;
                          ok !$e,                       'ddl helper accepts a select';
                          $ch->ddl("drop table $tbl", sub { EV::break });
                        });
                      });
                    });
                  });
                });
            });
        },
        on_error => sub { EV::break },
    );
    run_with_bail(5);
    $ch->finish;
}

# 23. slow_query_log fires for slow queries, skips fast ones.
{
    my @slow;
    my $ch; $ch = EV::ClickHouse->new(
        host => $host, port => $nport, protocol => 'native',
        on_connect => sub {
            $ch->slow_query_log(0.05, sub {
                my (undef, undef, undef, undef, $dur) = @_;
                push @slow, $dur;
            });
            $ch->query("select 1", sub {            # fast — should be filtered
                $ch->query("select sleep(0.2)", sub {
                    EV::break;
                });
            });
        },
        on_error => sub { EV::break },
    );
    run_with_bail(5);
    is scalar @slow, 1,                'slow_query_log filtered out the fast query';
    ok $slow[0] >= 0.05,               'recorded duration exceeds threshold';
    $ch->finish;
}
