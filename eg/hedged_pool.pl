#!/usr/bin/env perl
# Tail-latency mitigation with hedged_query. Hits 3 replicas in parallel
# for every SELECT and resolves with the first reply. Tracks per-member
# "win" counts so you can spot a consistently-slow replica.
#
# Realistic shape for a low-RPS dashboard backend where a stuck replica
# would otherwise spike p99 latency. Not appropriate for INSERT
# (would silently double-write under dedupe miss) or high-RPS bulk
# scans (doubles server load for marginal latency gain).
#
# Usage:
#   CH_REPLICAS=127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002 ./eg/hedged_pool.pl

use strict;
use warnings;
use EV;
use EV::ClickHouse;

my @replicas = split /,/, $ENV{CH_REPLICAS}
            || '127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002';

my $pool = EV::ClickHouse::Pool->new(
    hosts             => \@replicas,
    protocol          => 'native',
    size              => scalar @replicas,
    auto_reconnect    => 1,
    circuit_threshold => 5,
    circuit_cooldown  => 30,
);

# Track which member "wins" each hedged race.
my %wins;
$pool->with_each(sub { $wins{ $_[1] } = 0 });

my $rps = $ENV{RPS} // 5;
my $period = 1 / $rps;

my $issue = EV::timer(0, $period, sub {
    my $sent_at = EV::time;
    $pool->hedged_query(
        "select sleep(0.05), hostName()",       # 50ms baseline
        hedge => 2,
        sub {
            my ($rows, $err) = @_;
            my $latency = EV::time - $sent_at;
            if ($err) { warn "err: $err\n"; return }
            printf "%.0fms  %s\n", $latency * 1000, $rows->[0][1];
        },
    );
});

# Report breaker + win distribution every 5s.
my $report = EV::timer(5, 5, sub {
    print "--- circuit state ---\n";
    my $i = 0;
    for my $st ($pool->circuit_state) {
        printf "  member %d: fails=%d alive=%d dead_until=%.0f\n",
               $i++, $st->{fails}, $st->{alive}, $st->{dead_until};
    }
});

# Graceful drain on Ctrl-C.
my $stop = EV::signal('INT', sub {
    undef $issue; undef $report;
    print "draining…\n";
    $pool->shutdown(5, sub { print "done\n"; EV::break });
});

EV::run;
