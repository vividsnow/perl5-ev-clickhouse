use strict;
use warnings;
use Test::More;

# Author-only compatibility matrix: spawn ClickHouse via Docker at
# multiple released versions, run a smoke subset against each. Catches
# server-version regressions in the protocol code (revision negotiation,
# new packet types, deprecated fields, etc).
#
# Skipped unless RUN_COMPAT=1 is set and `docker` is in PATH.

plan skip_all => "set RUN_COMPAT=1 to run the compatibility matrix"
    unless $ENV{RUN_COMPAT};
plan skip_all => "docker not in PATH" unless `which docker 2>/dev/null`;

my @versions = $ENV{COMPAT_VERSIONS}
    ? split /,/, $ENV{COMPAT_VERSIONS}
    : qw(23.8 24.3 24.8 latest);

plan tests => scalar @versions;

my @subset = qw(
    t/00_load.t
    t/02_query.t
    t/03_insert.t
    t/05_native.t
    t/06_native_types.t
    t/14_new_accessors.t
    t/24_review_gaps.t
);

for my $ver (@versions) {
    my $name = "ev_ch_compat_$$_$ver";
    $name =~ s/[^A-Za-z0-9_]/_/g;
    diag "starting clickhouse:$ver as $name";
    my $http = 18000 + (rand() * 1000);
    $http = int($http);
    my $nat  = $http + 100;
    system(
        'docker', 'run', '--rm', '-d',
        '--name', $name,
        '-p', "$http:8123",
        '-p', "$nat:9000",
        "clickhouse/clickhouse-server:$ver",
    ) == 0 or do { ok(0, "compat $ver: docker run failed"); next };

    # Wait until the server is reachable (~30s budget).
    my $up = 0;
    for (1..30) {
        sleep 1;
        require IO::Socket::INET;
        $up = 1, last if IO::Socket::INET->new(
            PeerAddr => '127.0.0.1', PeerPort => $http, Timeout => 1);
    }
    if (!$up) {
        ok(0, "compat $ver: server didn't come up in 30s");
        system('docker', 'rm', '-f', $name) and warn "docker rm failed";
        next;
    }

    local $ENV{TEST_CLICKHOUSE_HOST}        = '127.0.0.1';
    local $ENV{TEST_CLICKHOUSE_PORT}        = $http;
    local $ENV{TEST_CLICKHOUSE_NATIVE_PORT} = $nat;
    my $rc = system('prove', '-bj4', @subset) >> 8;
    ok($rc == 0, "compat $ver: subset prove passed");

    system('docker', 'rm', '-f', $name) and warn "docker rm failed for $name";
}
