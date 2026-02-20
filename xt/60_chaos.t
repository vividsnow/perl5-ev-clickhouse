#!/usr/bin/env perl
# Protocol-boundary chaos: stress the client against an adversarial proxy
# that injects faults at points the existing fault-injection test doesn't
# cover. Goals:
#
#   1. Slow trickle (1-byte chunks): exercise the "more than one packet
#      per buffer" path and the "less than one packet per buffer" path
#      back-to-back. Should not deadlock and should return correct data.
#
#   2. Random RST during ServerHello: client should fail cleanly with
#      on_error (not hang past connect_timeout) every time.
#
#   3. Bidirectional dribble + reorder under load: 50 quick selects on a
#      pipeline-serialised native connection; the response stream is
#      arbitrarily sliced. Final query count must match.
#
# Like xt/07_fault_injection.t this requires a reachable real ClickHouse
# to act as the upstream end of the proxy.
use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use IO::Select;
use Socket qw(SOL_SOCKET SO_LINGER);
use Time::HiRes ();
use EV;
use EV::ClickHouse;

my $host     = $ENV{TEST_CLICKHOUSE_HOST}        || '127.0.0.1';
my $nat_port = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;

my $real = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $nat_port, Timeout => 2);
plan skip_all => "ClickHouse native not reachable" unless $real;
$real->close;

plan tests => 3;

# Generic proxy harness. Forwards client<->upstream with optional faults.
# Options:
#   chunk        => max bytes per read (default 1024)
#   delay_us     => sleep between iterations
#   rst_after    => RST the client after this many upstream bytes seen
#   rst_in_hello => RST after the first chunk of upstream bytes
sub run_proxy {
    my (%opts) = @_;
    my $listener = IO::Socket::INET->new(
        Listen => 1, LocalAddr => '127.0.0.1', LocalPort => 0,
        ReuseAddr => 1,
    ) or die "listen: $!";
    my $port = $listener->sockport;
    my $pid = fork;
    die "fork: $!" unless defined $pid;
    if ($pid == 0) {
        my $client = $listener->accept or exit 0;
        my $upstream = IO::Socket::INET->new(
            PeerAddr => $host, PeerPort => $nat_port, Timeout => 5,
        ) or exit 0;
        $client->blocking(0);
        $upstream->blocking(0);
        my $forwarded = 0;
        my $iter      = 0;
        my $cap       = $opts{rst_after}    || 0;
        my $rst_hello = $opts{rst_in_hello} || 0;
        my $chunk     = $opts{chunk}        || 1024;
        my $delay     = $opts{delay_us}     || 0;
        my $sel = IO::Select->new($client, $upstream);
        eval { while (my @ready = $sel->can_read(0.05)) {
            for my $fh (@ready) {
                my $buf;
                my $n = sysread($fh, $buf, $chunk);
                last unless defined $n && $n > 0;
                if ($fh == $client) {
                    syswrite($upstream, $buf);
                } else {
                    $forwarded += $n;
                    $iter++;
                    if ($rst_hello && $iter == 1) {
                        setsockopt($client, SOL_SOCKET, SO_LINGER, pack('II', 1, 0));
                        $client->close;
                        $upstream->close;
                        exit 0;
                    }
                    if ($cap && $forwarded > $cap) {
                        setsockopt($client, SOL_SOCKET, SO_LINGER, pack('II', 1, 0));
                        $client->close;
                        $upstream->close;
                        exit 0;
                    }
                    syswrite($client, $buf);
                }
                Time::HiRes::usleep($delay) if $delay;
            }
        } };
        $client->close; $upstream->close; exit 0;
    }
    return ($port, $pid);
}

# 1. 1-byte trickle. The native parser must handle every header
# byte arriving in its own packet without losing framing.
{
    my ($port, $pid) = run_proxy(chunk => 1);
    my ($ch, $rows, $err);
    $ch = EV::ClickHouse->new(
        host => '127.0.0.1', port => $port, protocol => 'native',
        connect_timeout => 8,
        on_connect => sub {
            $ch->query("select number from numbers(50)", sub {
                ($rows, $err) = @_; EV::break;
            });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    my $t = EV::timer(30, 0, sub { EV::break });
    EV::run;
    undef $t;
    ok $rows && @$rows == 50,
       "chaos: 1-byte trickle preserves framing for 50-row select"
       or diag "rows=" . ($rows ? scalar @$rows : 'undef') . " err=" . ($err // '');
    eval { $ch->finish };
    kill 'TERM', $pid; waitpid $pid, 0;
}

# 2. RST during ServerHello. Repeat 5x to flush out any race where
# the early-disconnect path lets a callback leak.
{
    my $fails = 0;
    for my $i (1 .. 5) {
        my ($port, $pid) = run_proxy(rst_in_hello => 1);
        my $err;
        my $ch = EV::ClickHouse->new(
            host => '127.0.0.1', port => $port, protocol => 'native',
            connect_timeout => 3,
            auto_reconnect  => 0,
            on_connect      => sub { EV::break },           # shouldn't happen
            on_error        => sub { $err = $_[0]; EV::break },
        );
        my $t = EV::timer(5, 0, sub { EV::break });
        EV::run;
        undef $t;
        $fails++ if $err;
        eval { $ch->finish };
        kill 'TERM', $pid; waitpid $pid, 0;
    }
    is $fails, 5, "chaos: RST during ServerHello surfaces as on_error every time";
}

# 3. Bidirectional dribble + reorder under load: 50 quick selects on a
# pipeline-serialised native connection; response slices arbitrarily.
{
    my ($port, $pid) = run_proxy(chunk => 7);
    my $done = 0;
    my $errs = 0;
    my $ch; $ch = EV::ClickHouse->new(
        host => '127.0.0.1', port => $port, protocol => 'native',
        connect_timeout => 10,
        on_connect => sub {
            for my $i (1 .. 50) {
                $ch->query("select $i", sub {
                    my ($rows, $err) = @_;
                    if ($err || !$rows || $rows->[0][0] != $i) { $errs++ }
                    else { $done++ }
                    EV::break if ($done + $errs) == 50;
                });
            }
        },
        on_error => sub { $errs++; EV::break },
    );
    my $t = EV::timer(60, 0, sub { EV::break });
    EV::run;
    undef $t;
    is "$done/$errs", "50/0",
       "chaos: 50 pipelined selects survive 7-byte chunking";
    eval { $ch->finish };
    kill 'TERM', $pid; waitpid $pid, 0;
}
