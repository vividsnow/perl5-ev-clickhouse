#!/usr/bin/env perl
# Minimal HTTP dashboard exposing per-replica health for a ClickHouse
# Pool. GET /health returns JSON with circuit_state, last RTT, and
# pending_count for every pool member. Wired with EV so the same loop
# drives the dashboard server, the periodic probes, and the ClickHouse
# connections.
#
# Usage:
#   CH_REPLICAS=127.0.0.1:9000,127.0.0.1:9001 ./eg/health_dashboard.pl
#   curl http://127.0.0.1:8085/health

use strict;
use warnings;
use EV;
use EV::ClickHouse;
use IO::Socket::INET;
use JSON::PP qw(encode_json);

my @replicas = split /,/, $ENV{CH_REPLICAS}
            || '127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002';
my $dash_port = $ENV{DASH_PORT} // 8085;

my $pool = EV::ClickHouse::Pool->new(
    hosts             => \@replicas,
    protocol          => 'native',
    size              => scalar @replicas,
    auto_reconnect    => 1,
    circuit_threshold => 3,
    circuit_cooldown  => 30,
);

# Per-member latest RTT (seconds), updated by the periodic probe.
my %rtt;
$pool->with_each(sub { $rtt{ $_[1] } = undef });

my $probe = EV::timer(0, 5, sub {
    for my $i (0 .. $pool->size - 1) {
        my $ch = ($pool->conns)[$i];
        $ch->ping_round_trip(sub {
            my ($s, $err) = @_;
            $rtt{$i} = $err ? undef : $s;
        });
    }
});

# Tiny HTTP server. Single-shot, no keep-alive, no streaming — just
# enough to demonstrate the JSON shape.
my $listener = IO::Socket::INET->new(
    Listen => 16, LocalAddr => '0.0.0.0', LocalPort => $dash_port,
    ReuseAddr => 1, Blocking => 0,
) or die "listen $dash_port: $!";

my $accept_io = EV::io($listener->fileno, EV::READ, sub {
    while (my $cli = $listener->accept) {
        $cli->blocking(0);
        my $buf = '';
        my $w; $w = EV::io($cli->fileno, EV::READ, sub {
            my $n = sysread($cli, $buf, 8192, length $buf);
            if (!defined $n || $n == 0) { undef $w; close $cli; return }
            return unless $buf =~ /\r\n\r\n/;
            undef $w;
            my @state = $pool->circuit_state;
            my @body;
            for my $i (0 .. $#state) {
                push @body, {
                    member        => $i,
                    host          => ($pool->conns)[$i]->current_host,
                    port          => ($pool->conns)[$i]->current_port + 0,
                    pending_count => ($pool->conns)[$i]->pending_count + 0,
                    fails         => $state[$i]{fails},
                    dead_until    => $state[$i]{dead_until},
                    alive         => $state[$i]{alive} ? \1 : \0,
                    rtt_ms        => defined $rtt{$i}
                                       ? sprintf("%.2f", $rtt{$i} * 1000) + 0
                                       : undef,
                };
            }
            my $json = encode_json({ pool => \@body });
            my $http = "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n"
                     . "Content-Length: " . length($json) . "\r\nConnection: close\r\n\r\n"
                     . $json;
            syswrite $cli, $http;
            close $cli;
        });
    }
});

warn "dashboard on :$dash_port — try: curl http://127.0.0.1:$dash_port/health\n";

my $sig = EV::signal('INT', sub {
    undef $accept_io; undef $probe;
    close $listener;
    $pool->shutdown(5, sub { EV::break });
});

EV::run;
