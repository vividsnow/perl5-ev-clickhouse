use strict;
use warnings;
use Test::More;

eval "use Test::Pod::Coverage 1.08";
plan skip_all => "Test::Pod::Coverage 1.08 required" if $@;

# Underscore-prefixed XSUBs are setters used by lib/EV/ClickHouse.pm to wire
# up constructor arguments into the underlying struct; they are not part of
# the public API and not documented in POD by design.
#
# `connect` is the low-level XSUB that the public `new` constructor calls
# internally; users should not call it directly.
#
# `q`, `reconnect`, `disconnect` are typeglob aliases for query / reset /
# finish, listed in the ALIASES POD section. Pod::Coverage doesn't follow
# typeglob aliases, so whitelist them here.
my $private = qr/^(?:
    _new
  | _set_(?:protocol|compress|session_id
            |connect_timeout|query_timeout
            |tls|tls_ca_file|tls_skip_verify
            |auto_reconnect|keepalive
            |reconnect_delay|reconnect_max_delay|reconnect_max_attempts
            |reconnect_jitter
            |progress_period|max_query_size|max_recv_buffer
            |http_basic_auth|query_log_comment|host
            |dns_pending|decode_flags|settings|failover)
  | _take_dns_pending
  | _streamer_push_row
  | _pool_pick
  | _iterator_next
  | _breaker_observe
  | DESTROY
  | BOOT
  | connect
  | q
  | reconnect
  | disconnect
  | query_duration_p
  | query_duration_count
)$/x;

pod_coverage_ok(
    'EV::ClickHouse',
    {
        also_private => [$private],
        coverage_class => 'Pod::Coverage::CountParents',
    },
);

done_testing;
