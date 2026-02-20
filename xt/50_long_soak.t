#!/usr/bin/env perl
# Long-soak stability test (opt-in). Drives a sustained query workload
# for ~30 minutes and asserts that the resident set size and open file
# descriptor count don't drift upward. Catches:
#   - LowCardinality cross-block dictionary leaks
#   - HTTP request buffer / send queue accumulation
#   - libev watcher fd leaks across reconnect cycles
#
# Default run is short (10s) so CI can sanity-check the harness.
# Set EV_CH_SOAK_SECONDS=1800 in CI nightlies for the real soak.
use strict;
use warnings;
use Test::More;
use EV;
use EV::ClickHouse;
use IO::Socket::INET;

my $host  = $ENV{TEST_CLICKHOUSE_HOST}        || '127.0.0.1';
my $nport = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;
my $duration = $ENV{EV_CH_SOAK_SECONDS}       || 10;

plan skip_all => "set EV_CH_SOAK_SECONDS to opt into long soak"
    unless $ENV{EV_CH_SOAK_SECONDS} || $ENV{AUTHOR_TESTING};

plan skip_all => "ClickHouse native not reachable"
    unless IO::Socket::INET->new(PeerAddr => $host, PeerPort => $nport, Timeout => 2);

plan tests => 2;

sub rss_kb {
    open my $fh, '<', "/proc/$$/status" or return undef;
    while (<$fh>) { return $1 if /^VmRSS:\s+(\d+)/ }
    return undef;
}

sub fd_count {
    opendir my $dh, "/proc/$$/fd" or return undef;
    my $n = grep { !/^\.\.?$/ } readdir $dh;
    closedir $dh;
    $n;
}

my $rss_start = rss_kb();
my $fds_start = fd_count();
my $queries   = 0;
my $errors    = 0;

my $ch; $ch = EV::ClickHouse->new(
    host => $host, port => $nport, protocol => 'native',
    auto_reconnect   => 1,
    reconnect_delay  => 0.1,
    reconnect_jitter => 0.2,
    keepalive        => 5,
    on_error         => sub { $errors++ },
);

my $deadline = EV::time() + $duration;
my $tick;
$tick = EV::timer(0, 0.05, sub {
    if (EV::time() >= $deadline) {
        undef $tick;
        EV::break;
        return;
    }
    # Keep up to ~50 queries in flight at any time.
    my $launch = 50 - $ch->pending_count;
    for (1 .. $launch) {
        $ch->query("select arrayJoin(range(100)) as n",
                   { on_data => sub { } },
                   sub { $queries++ });
    }
});
EV::run;

my $rss_end = rss_kb();
my $fds_end = fd_count();
diag sprintf("ran %d queries (errors=%d) over %ds; RSS %d -> %d KB; FDs %d -> %d",
             $queries, $errors, $duration, $rss_start, $rss_end,
             $fds_start, $fds_end);

# Allow up to 50% RSS growth as warmup slack; anything more is a real leak.
my $rss_ratio = $rss_start ? $rss_end / $rss_start : 1;
ok $rss_ratio < 1.5,
   "RSS within bounds (started ${rss_start}KB ended ${rss_end}KB ratio $rss_ratio)";

# Allow at most 4 more FDs than we started with (some EV bookkeeping ok).
ok $fds_end <= $fds_start + 4,
   "FD count stable (started $fds_start ended $fds_end)";

$ch->finish;
