use strict;
use warnings;
use Test::More;

# Test::Kwalitee::Extra plans tests on import, so we must use require + a
# single explicit import call — otherwise `use` plans once with defaults
# and our follow-up import call replans, tripping Test::More's plan guard.
eval { require Test::Kwalitee::Extra };
plan skip_all => "Test::Kwalitee::Extra required" if $@;

# Skip checks that fail purely because we run from a git checkout, where
# META.{json,yml} don't exist until `make dist` builds the tarball.
# Indicator names use the Exporter-style `!name` convention.
Test::Kwalitee::Extra->import(qw(
    !has_meta_yml
    !has_meta_json
    !no_pod_errors
));
