use strict;
use warnings;
use Test::More;
use Time::HiRes qw(time);
use EV;
use EV::ClickHouse;

my $host = $ENV{TEST_CLICKHOUSE_HOST} || '127.0.0.1';
my $port = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;
my $N    = $ENV{EV_CH_STRESS_N} || 10_000;

require IO::Socket::INET;
plan skip_all => "ClickHouse native port not reachable"
    unless IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Timeout => 2);

plan tests => 4;

my $ch;
my ($done, $errors) = (0, 0);
my $start;

$ch = EV::ClickHouse->new(
    host       => $host,
    port       => $port,
    protocol   => 'native',
    on_connect => sub {
        $start = time();
        for my $i (1 .. $N) {
            $ch->query("SELECT $i", sub {
                my ($rows, $err) = @_;
                $done++;
                $errors++ if $err;
            });
        }
        diag(sprintf "queued %d, pending=%d", $N, $ch->pending_count);

        $ch->drain(sub {
            my $elapsed = time() - $start;
            diag(sprintf "drained in %.2fs (%.0f q/s)",
                 $elapsed, $done / $elapsed);
            EV::break;
        });
    },
    on_error => sub { diag "stress error: $_[0]"; EV::break },
);

# generous overall ceiling — test should always finish well under this
my $watchdog = EV::timer(120, 0, sub { diag "watchdog tripped"; EV::break });

EV::run;

is($done,   $N, "all $N queries completed");
is($errors, 0,  "no per-query errors");
is($ch->pending_count, 0, "pending_count drained to 0");
ok($ch->is_connected, "still connected after stress");

$ch->finish if $ch->is_connected;
