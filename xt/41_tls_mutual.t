#!/usr/bin/env perl
# TLS mutual authentication (opt-in). Generates a throw-away CA + server
# cert chained to it + client cert chained to it, spins up a CH server
# in podman with mutual-TLS enforcement on a high port, and verifies:
#   1. A connection WITHOUT a client cert is rejected.
#   2. A connection WITH the right client cert connects and queries.
#
# Activate with EV_CH_TLS_MUTUAL=1 (and have podman + openssl in PATH).
# The test silently skips otherwise.
use strict;
use warnings;
use Test::More;
use IO::Socket::INET;
use File::Temp ();
use EV;
use EV::ClickHouse;

plan skip_all => "set EV_CH_TLS_MUTUAL=1 to opt into mutual-TLS test"
    unless $ENV{EV_CH_TLS_MUTUAL};

chomp(my $openssl = `which openssl 2>/dev/null`);
chomp(my $podman  = `which podman  2>/dev/null`);
plan skip_all => "openssl not in PATH" unless $openssl;
plan skip_all => "podman not in PATH"  unless $podman;

my $dir = File::Temp::tempdir(CLEANUP => 1);
chmod 0755, $dir;

sub run { system(@_) == 0 or die "@_ failed: $?\n" }

# CA.
run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
    "-subj", "/CN=ev-ch-test-CA",
    "-keyout", "$dir/ca.key", "-out", "$dir/ca.crt");
# Server + client certs signed by the CA. Server cert needs SAN=127.0.0.1
# so OpenSSL's hostname verification (if we enabled it) would pass.
for my $who (qw(server client)) {
    my $extra = $who eq 'server'
              ? ['-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1']
              : [];
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=$who",
        @$extra,
        "-keyout", "$dir/$who.key", "-out", "$dir/$who.csr");
    run("openssl", "x509", "-req", "-days", "1",
        "-CA", "$dir/ca.crt", "-CAkey", "$dir/ca.key", "-CAcreateserial",
        @$extra ? ("-copy_extensions", "copy") : (),
        "-in", "$dir/$who.csr", "-out", "$dir/$who.crt");
}
chmod 0644, "$dir/server.key", "$dir/client.key", "$dir/ca.crt";

open my $cfg, '>', "$dir/config.xml" or die "open config: $!";
print $cfg <<'XML';
<?xml version="1.0"?>
<clickhouse>
  <logger><level>warning</level><console>1</console></logger>
  <http_port remove="remove"/>
  <tcp_port remove="remove"/>
  <mysql_port remove="remove"/>
  <postgresql_port remove="remove"/>
  <interserver_http_port remove="remove"/>
  <tcp_port_secure>9440</tcp_port_secure>
  <listen_host>0.0.0.0</listen_host>
  <openSSL>
    <server>
      <certificateFile>/etc/ch-tls/server.crt</certificateFile>
      <privateKeyFile>/etc/ch-tls/server.key</privateKeyFile>
      <caConfig>/etc/ch-tls/ca.crt</caConfig>
      <verificationMode>strict</verificationMode>
      <cacheSessions>true</cacheSessions>
      <disableProtocols>sslv2,sslv3</disableProtocols>
      <preferServerCiphers>true</preferServerCiphers>
    </server>
  </openSSL>
</clickhouse>
XML
close $cfg;

open my $uh, '>', "$dir/users.xml" or die "open users: $!";
print $uh <<'XML';
<?xml version="1.0"?>
<clickhouse>
  <profiles><default><max_memory_usage>10000000000</max_memory_usage></default></profiles>
  <users>
    <default replace="replace">
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <ssl_certificates><common_name>client</common_name></ssl_certificates>
    </default>
  </users>
  <quotas><default><interval><duration>3600</duration></interval></default></quotas>
</clickhouse>
XML
close $uh;

my $cname = "ev_ch_mtls_$$";
my $port  = 19440 + int(rand(1000));
system("$podman rm -f $cname 2>/dev/null");
my $out = `$podman run -d --rm --name $cname -p $port:9440 \\
    -v $dir/config.xml:/etc/clickhouse-server/config.d/config.xml:Z \\
    -v $dir/users.xml:/etc/clickhouse-server/users.d/users.xml:Z \\
    -v $dir:/etc/ch-tls:Z \\
    --ulimit nofile=262144:262144 \\
    clickhouse/clickhouse-server:latest 2>&1`;
if ($? != 0) { plan skip_all => "podman run failed: $out" }

my $deadline = time + 60;
my $ready;
while (time < $deadline) {
    my $s = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Timeout => 1);
    if ($s) { $s->close; $ready = 1; last }
    sleep 1;
}
unless ($ready) {
    diag(`$podman logs $cname 2>&1`);
    system("$podman rm -f $cname >/dev/null 2>&1");
    plan skip_all => "ClickHouse mutual-TLS port never came up";
}
sleep 2;

plan tests => 2;

# 1. No client cert → server rejects the handshake.
{
    my ($err, $rows);
    my $ch; $ch = EV::ClickHouse->new(
        host => '127.0.0.1', port => $port, protocol => 'native',
        tls => 1, tls_skip_verify => 1,
        connect_timeout => 5,
        on_connect => sub {
            $ch->query("select 1", sub { ($rows, $err) = @_; EV::break });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    my $t = EV::timer(10, 0, sub { EV::break }); EV::run; undef $t;
    ok defined($err) && !defined($rows),
       'no-cert client is rejected by mutual-TLS server'
       or diag "rows=" . ($rows ? scalar @$rows : 'undef') . " err=" . ($err // 'undef');
    eval { $ch->finish };
}

# 2. Valid client cert → connects + runs a real query.
{
    my ($err, $rows);
    my $ch; $ch = EV::ClickHouse->new(
        host => '127.0.0.1', port => $port, protocol => 'native',
        tls => 1, tls_skip_verify => 1,
        tls_cert_file => "$dir/client.crt",
        tls_key_file  => "$dir/client.key",
        connect_timeout => 5,
        on_connect => sub {
            $ch->query("select 42", sub { ($rows, $err) = @_; EV::break });
        },
        on_error => sub { $err = $_[0]; EV::break },
    );
    my $t = EV::timer(10, 0, sub { EV::break }); EV::run; undef $t;
    ok !$err && $rows && $rows->[0][0] == 42,
       'cert client passes mutual auth and runs a query'
       or diag "rows=" . ($rows ? scalar @$rows : 'undef') . " err=" . ($err // 'undef');
    eval { $ch->finish };
}

system("$podman rm -f $cname >/dev/null 2>&1");
