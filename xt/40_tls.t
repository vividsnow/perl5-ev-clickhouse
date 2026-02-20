use strict;
use warnings;
use Test::More;
use EV;
use EV::ClickHouse;

# Author TLS test. Requires a ClickHouse instance reachable over TLS, e.g.:
#
#   stunnel \
#     -accept 127.0.0.1:9440 \
#     -connect 127.0.0.1:9000 \
#     -cert /tmp/test-cert.pem
#
# then:
#
#   TEST_CLICKHOUSE_TLS_HOST=127.0.0.1 \
#   TEST_CLICKHOUSE_TLS_PORT=9440 \
#   prove -lb xt/40_tls.t

my $host = $ENV{TEST_CLICKHOUSE_TLS_HOST};
my $port = $ENV{TEST_CLICKHOUSE_TLS_PORT};

plan skip_all => 'set TEST_CLICKHOUSE_TLS_HOST and TEST_CLICKHOUSE_TLS_PORT'
    unless $host && $port;

require IO::Socket::INET;
plan skip_all => "TLS port $host:$port not reachable"
    unless IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Timeout => 3);

plan tests => 6;

# 1-3: tls_skip_verify (works even with a self-signed cert)
{
    my $ch;
    my $got_rows;
    $ch = EV::ClickHouse->new(
        host             => $host,
        port             => $port,
        protocol         => 'native',
        tls              => 1,
        tls_skip_verify  => 1,
        on_connect       => sub {
            $ch->query("SELECT 1", sub {
                my ($rows, $err) = @_;
                $got_rows = $rows unless $err;
                diag("query error: $err") if $err;
                EV::break;
            });
        },
        on_error         => sub { diag("error: $_[0]"); EV::break },
    );
    my $t = EV::timer(10, 0, sub { EV::break });
    EV::run;

    ok($ch->is_connected, 'tls_skip_verify: connected');
    ok($got_rows, 'tls_skip_verify: got result');
    is($got_rows->[0][0], 1, 'tls_skip_verify: SELECT 1 returns 1') if $got_rows;
    $ch->finish if $ch->is_connected;
}

# 4-6: tls_ca_file (needs a CA cert at TEST_CLICKHOUSE_TLS_CA)
SKIP: {
    my $ca = $ENV{TEST_CLICKHOUSE_TLS_CA};
    skip 'set TEST_CLICKHOUSE_TLS_CA to test verified TLS', 3 unless $ca && -e $ca;

    my $ch;
    my $got_rows;
    $ch = EV::ClickHouse->new(
        host         => $host,
        port         => $port,
        protocol     => 'native',
        tls          => 1,
        tls_ca_file  => $ca,
        on_connect   => sub {
            $ch->query("SELECT 1", sub {
                my ($rows, $err) = @_;
                $got_rows = $rows unless $err;
                EV::break;
            });
        },
        on_error     => sub { diag("verified TLS error: $_[0]"); EV::break },
    );
    my $t = EV::timer(10, 0, sub { EV::break });
    EV::run;

    ok($ch->is_connected, 'tls_ca_file: connected');
    ok($got_rows, 'tls_ca_file: got result');
    is($got_rows->[0][0], 1, 'tls_ca_file: SELECT 1 returns 1') if $got_rows;
    $ch->finish if $ch->is_connected;
}
