use strict;
use warnings;
use Test::More;
use EV;
use EV::ClickHouse;

eval "use Test::LeakTrace";
plan skip_all => "Test::LeakTrace required" if $@;

my $host = $ENV{TEST_CLICKHOUSE_HOST} || '127.0.0.1';
my $port = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;

require IO::Socket::INET;
plan skip_all => "ClickHouse native port not reachable"
    unless IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Timeout => 2);

plan tests => 6;

# 1. Construct + DESTROY without ever connecting.
no_leaks_ok(
    sub {
        my $ch = EV::ClickHouse->new(
            host       => $host,
            port       => $port,
            protocol   => 'native',
            on_error   => sub { },
        );
        $ch->finish if $ch && $ch->is_connected;
        undef $ch;
    },
    'construct/destroy cycle',
);

# 2. Full connect / query / disconnect cycle.
no_leaks_ok(
    sub {
        my $ch;
        $ch = EV::ClickHouse->new(
            host       => $host,
            port       => $port,
            protocol   => 'native',
            on_connect => sub {
                $ch->query("SELECT 1", sub { EV::break });
            },
            on_error   => sub { EV::break },
        );
        my $t = EV::timer(5, 0, sub { EV::break });
        EV::run;
        $ch->finish if $ch && $ch->is_connected;
        undef $ch;
    },
    'connect / query / disconnect',
);

# 3. INSERT with arrayref (exercises Perl-side encoding paths).
no_leaks_ok(
    sub {
        my $ch;
        $ch = EV::ClickHouse->new(
            host       => $host,
            port       => $port,
            protocol   => 'native',
            on_connect => sub {
                $ch->query(
                    "CREATE TEMPORARY TABLE _ev_leak_test (a UInt32, b String) ENGINE = Memory",
                    sub {
                        $ch->insert("_ev_leak_test",
                            [[1, "x"], [2, "y"], [3, "z"]],
                            sub { EV::break },
                        );
                    },
                );
            },
            on_error => sub { EV::break },
        );
        my $t = EV::timer(5, 0, sub { EV::break });
        EV::run;
        $ch->finish if $ch && $ch->is_connected;
        undef $ch;
    },
    'insert arrayref',
);

# 4. Cancelled query (exercises cancel_pending + cleanup_connection).
no_leaks_ok(
    sub {
        my $ch;
        $ch = EV::ClickHouse->new(
            host       => $host,
            port       => $port,
            protocol   => 'native',
            on_connect => sub {
                $ch->query("SELECT sleep(3)", sub { EV::break });
                EV::timer(0.1, 0, sub { $ch->cancel });
            },
            on_error => sub { EV::break },
        );
        my $t = EV::timer(5, 0, sub { EV::break });
        EV::run;
        $ch->finish if $ch && $ch->is_connected;
        undef $ch;
    },
    'cancelled query',
);

# 5. Drain callback fires after queued queries complete (on_drain SV lifecycle).
no_leaks_ok(
    sub {
        my $ch;
        $ch = EV::ClickHouse->new(
            host       => $host,
            port       => $port,
            protocol   => 'native',
            on_connect => sub {
                $ch->query("SELECT 1", sub { });
                $ch->query("SELECT 2", sub { });
                $ch->query("SELECT 3", sub { });
                $ch->drain(sub { EV::break });
            },
            on_error => sub { EV::break },
        );
        my $t = EV::timer(5, 0, sub { EV::break });
        EV::run;
        $ch->finish if $ch && $ch->is_connected;
        undef $ch;
    },
    'drain callback',
);

# 6. skip_pending with queued send_queue entries (cancel_pending drain path).
no_leaks_ok(
    sub {
        my $ch;
        $ch = EV::ClickHouse->new(
            host       => $host,
            port       => $port,
            protocol   => 'native',
            on_connect => sub {
                $ch->query("SELECT 1", sub { });
                $ch->query("SELECT 2", sub { });
                $ch->query("SELECT 3", sub { });
                $ch->skip_pending;
                EV::break;
            },
            on_error => sub { EV::break },
        );
        my $t = EV::timer(5, 0, sub { EV::break });
        EV::run;
        $ch->finish if $ch && $ch->is_connected;
        undef $ch;
    },
    'skip_pending drain',
);
