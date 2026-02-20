use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use EV;
use EV::ClickHouse;

# In-process TCP proxy that can:
#   * accept the client's bytes
#   * forward to the real ClickHouse with a fixed dribble size
#   * RST mid-stream after N bytes
# Validates that the parser tolerates partial reads and that abrupt
# disconnects surface as on_error / auto_reconnect rather than hangs.

my $host       = $ENV{TEST_CLICKHOUSE_HOST} || '127.0.0.1';
my $nat_port   = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;

my $real = IO::Socket::INET->new(
    PeerAddr => $host, PeerPort => $nat_port, Timeout => 2,
);
plan skip_all => "ClickHouse native port not reachable" unless $real;
$real->close;

plan tests => 2;

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
        my $cap = $opts{rst_after} || 0;     # 0 = no RST, just dribble
        my $chunk = $opts{chunk} || 16;
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
                    if ($cap && $forwarded > $cap) {
                        # Force RST by enabling SO_LINGER=0 then closing.
                        setsockopt($client, SOL_SOCKET, SO_LINGER, pack('II', 1, 0));
                        $client->close;
                        $upstream->close;
                        exit 0;
                    }
                    syswrite($client, $buf);
                }
            }
        } };
        $client->close; $upstream->close; exit 0;
    }
    return ($port, $pid);
}

require IO::Select;

# 1: dribbled reads — proxy forwards in 16-byte chunks, parser must reassemble.
{
    my ($port, $pid) = run_proxy(chunk => 16);
    my ($ch, $rows, $err);
    $ch = EV::ClickHouse->new(
        host => '127.0.0.1', port => $port, protocol => 'native',
        on_connect => sub {
            $ch->query("select number from numbers(100)", sub {
                ($rows, $err) = @_; EV::break;
            });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    my $t = EV::timer(15, 0, sub { EV::break });
    EV::run;
    ok($rows && @$rows >= 100,
       "fault-injection: dribbled-read proxy preserves a 100-row select")
       or diag "rows=" . ($rows ? scalar @$rows : '<undef>') . " err=" . ($err // '');
    $ch->finish if $ch->is_connected;
    kill 'TERM', $pid; waitpid $pid, 0;
}

# 2: mid-stream RST — proxy yanks the connection after a few KB; client must
# fire on_error (and not hang).
{
    my ($port, $pid) = run_proxy(rst_after => 256);
    my ($ch, $err);
    $ch = EV::ClickHouse->new(
        host => '127.0.0.1', port => $port, protocol => 'native',
        on_connect => sub {
            $ch->query("select number from numbers(1_000_000)", sub {
                (undef, $err) = @_; EV::break;
            });
        },
        on_error => sub { $err //= $_[0]; EV::break },
    );
    my $t = EV::timer(10, 0, sub { EV::break });
    EV::run;
    ok($err, "fault-injection: mid-stream RST surfaces as on_error / cb error")
       or diag "no error reported";
    $ch->finish if $ch->is_connected;
    kill 'TERM', $pid; waitpid $pid, 0;
}
