use strict;
use warnings;
use Test::More;
use Config;

# Author test: re-execs the test suite under valgrind and parses the leak
# summary. Opt-in via EV_CH_VALGRIND=1 to avoid surprising CI runs.
unless ($ENV{EV_CH_VALGRIND}) {
    plan skip_all => 'set EV_CH_VALGRIND=1 to enable valgrind run';
}

# Avoid infinite recursion when valgrind re-execs us.
if ($ENV{EV_CH_INSIDE_VALGRIND}) {
    plan skip_all => 'inside valgrind re-exec; nothing to do at this level';
}

my $valgrind = $ENV{VALGRIND} || 'valgrind';
chomp(my $which = `command -v $valgrind 2>/dev/null`);
plan skip_all => "valgrind not found in PATH" unless $which;

plan tests => 1;

local $ENV{EV_CH_INSIDE_VALGRIND} = 1;
my $perl   = $Config{perlpath} || $^X;
my $script = 't/02_query.t';

my @cmd = (
    $valgrind,
    '--error-exitcode=99',
    '--leak-check=full',
    '--show-leak-kinds=definite,indirect',
    '--errors-for-leak-kinds=definite,indirect',
    '--num-callers=20',
    '--quiet',
    $perl, '-Iblib/lib', '-Iblib/arch', $script,
);

# Capture (and discard) the child's stdout so its TAP doesn't interleave
# with this script's. valgrind itself writes its findings to stderr, which
# we let through for diagnostic visibility.
my $pid = fork();
die "fork: $!" unless defined $pid;
if ($pid == 0) {
    open STDOUT, '>', '/dev/null' or die $!;
    exec @cmd;
    die "exec: $!";
}
waitpid $pid, 0;
my $exit = $? >> 8;

ok($exit != 99, "valgrind reports no definite/indirect leaks (exit=$exit)")
    or diag("valgrind exit: $exit (99 = leak/error detected)");
