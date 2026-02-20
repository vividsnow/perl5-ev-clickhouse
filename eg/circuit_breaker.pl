#!/usr/bin/env perl
# Pool circuit breaker: after N consecutive query/insert failures on a
# member, the Pool marks it dead for `circuit_cooldown` seconds. The
# next _pick() skips dead members; if all are dead, the breaker is
# bypassed so a recovery attempt still has a chance.
#
# Inspect state via $pool->circuit_state - returns a list of
# { fails => N, dead_until => $epoch, alive => 0|1 } per member.
use strict;
use warnings;
use EV;
use EV::ClickHouse;

my $host  = $ENV{CLICKHOUSE_HOST}        // '127.0.0.1';
my $nport = $ENV{CLICKHOUSE_NATIVE_PORT} // 9000;

# Mix one bad host with two good ones. The bad one will trip the breaker
# after `circuit_threshold` failed connect attempts and stay out of the
# rotation for `circuit_cooldown` seconds.
my $pool = EV::ClickHouse::Pool->new(
    size              => 3,
    host              => $host, port => $nport, protocol => 'native',
    auto_reconnect    => 1,
    connect_timeout   => 0.5,
    circuit_threshold => 3,
    circuit_cooldown  => 10,
);

# Force one member onto a dead port so its queries fail.
my @c = $pool->conns;
$c[0]->finish; $c[0]->_set_host($host, 1); $c[0]->reset;

# Fire bursts of selects; periodically print breaker state.
my $issued = 0;
my $w = EV::timer(0, 0.2, sub {
    for (1 .. 5) {
        $pool->query("select 1", sub {
            my (undef, $err) = @_;
            warn "query err: $err\n" if $err;
        });
    }
    $issued += 5;
    my @s = $pool->circuit_state;
    for my $i (0 .. $#s) {
        printf STDERR "[breaker] member %d  fails=%d  alive=%s\n",
                      $i, $s[$i]{fails}, ($s[$i]{alive} ? 'yes' : 'no');
    }
    EV::break if $issued >= 50;
});

EV::run;
$pool->finish;
