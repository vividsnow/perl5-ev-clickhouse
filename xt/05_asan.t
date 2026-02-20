use strict;
use warnings;
use Test::More;

# Author-only sanitizer smoke test. Re-build the module with
# `perl Makefile.PL --with-debug && make` to enable AddressSanitizer +
# UndefinedBehaviorSanitizer, then run a representative subset of the
# regular test suite under the sanitized binary. The runtime emits a
# diagnostic and exits non-zero if anything is found, so the prove run
# fails loudly.

plan skip_all => "set RUN_ASAN=1 to run the ASAN smoke test"
    unless $ENV{RUN_ASAN};

# Confirm the build was actually compiled with -fsanitize.
my $obj = 'ClickHouse.o';
plan skip_all => "no $obj; build with --with-debug" unless -f $obj;
my $strings = `strings $obj 2>/dev/null`;
plan skip_all => "$obj wasn't built with -fsanitize"
    unless $strings =~ /__asan_init|__ubsan_handle/;

plan tests => 1;

# Run a small subset under the sanitized binary; any sanitizer report
# leaves a non-zero exit.
my @subset = qw(
    t/00_load.t
    t/02_query.t
    t/05_native.t
    t/09_insert_arrayref.t
    t/24_review_gaps.t
);
my $cmd = "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 "
        . "UBSAN_OPTIONS=halt_on_error=1 "
        . "prove -b @subset 2>&1";
my $out = `$cmd`;
my $rc  = $? >> 8;
ok($rc == 0, "sanitized prove subset exited cleanly")
    or diag $out;
