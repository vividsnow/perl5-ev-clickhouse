use strict;
use warnings;
use Test::More;
use EV;
use EV::ClickHouse;

# Property-style round-trip: generate random rows for representative
# scalar types, INSERT them into a temporary Memory table, SELECT them
# back, and assert the values match. Each iteration uses a fresh seed
# so failures can be reproduced via PROP_SEED=N.

my $host     = $ENV{TEST_CLICKHOUSE_HOST} || '127.0.0.1';
my $nat_port = $ENV{TEST_CLICKHOUSE_NATIVE_PORT} || 9000;

require IO::Socket::INET;
plan skip_all => "Native ClickHouse not reachable"
    unless IO::Socket::INET->new(
        PeerAddr => $host, PeerPort => $nat_port, Timeout => 2);

my $iters = $ENV{PROP_ITERS} || 25;
plan tests => $iters;

my $seed = $ENV{PROP_SEED} || (time ^ $$);
srand($seed);
diag "PROP_SEED=$seed (PROP_ITERS=$iters to override count)";

sub rand_string {
    my $len = int(rand(40)) + 1;
    join '', map { chr(0x21 + int(rand(0x5d))) } 1..$len;
}

sub rand_row {
    return [
        int(rand(2**31)) - (2**30),                  # i32
        int(rand(2**32)),                             # u32
        sprintf('%.9f', rand() * 1e6),                # f64 (as string for parser tolerance)
        rand_string(),                                # str
        int(rand(2)),                                 # bool
    ];
}

my @rows = map { rand_row() } 1..50;

my $ch;
my $err;
my $got;
$ch = EV::ClickHouse->new(
    host => $host, port => $nat_port, protocol => 'native',
    on_connect => sub {
        $ch->query(
            "create temporary table t (i Int32, u UInt32, f Float64, s String, b Bool) "
          . "ENGINE = Memory",
            sub {
                $ch->insert('t', \@rows, sub {
                    (undef, $err) = @_;
                    return EV::break if $err;
                    $ch->query("select * from t order by rowNumberInBlock()", sub {
                        ($got, $err) = @_; EV::break;
                    });
                });
            });
    },
);
my $t = EV::timer(30, 0, sub { EV::break });
EV::run;
$ch->finish if $ch && $ch->is_connected;
die "property setup failed: $err" if $err;

for my $i (0 .. $iters - 1) {
    my $idx = int(rand(scalar @rows));
    my $exp = $rows[$idx];
    my $g   = $got->[$idx];
    # Float comparison with epsilon, others exact.
    my $ok =  $g->[0] == $exp->[0]
           && $g->[1] == $exp->[1]
           && abs($g->[2] - $exp->[2]) < 1e-3
           && $g->[3] eq $exp->[3]
           && !!$g->[4] == !!$exp->[4];
    ok($ok, "row $idx round-trip")
        or diag "exp=[@$exp] got=[@$g]";
}
