#!/usr/bin/env perl
# EV::ClickHouse benchmark — measures throughput across protocols and compression
use strict;
use warnings;
use Time::HiRes qw(time);
use EV;
use EV::ClickHouse;

$| = 1;

my $host        = $ENV{CLICKHOUSE_HOST}        // '127.0.0.1';
my $http_port   = $ENV{CLICKHOUSE_PORT}        // 8123;
my $native_port = $ENV{CLICKHOUSE_NATIVE_PORT} // 9000;

my $small_n  = $ENV{BENCH_SMALL_N}  // 1000;
my $large_n  = $ENV{BENCH_LARGE_N}  // 100_000;
my $large_r  = $ENV{BENCH_LARGE_R}  // 10;
my $insert_rows = $ENV{BENCH_INSERT_ROWS} // 10_000;
my $insert_r    = $ENV{BENCH_INSERT_R}    // 100;

# --- reachability ---

sub port_open {
    my ($h, $p) = @_;
    require IO::Socket::INET;
    my $s = IO::Socket::INET->new(PeerAddr => $h, PeerPort => $p, Timeout => 2);
    return !!$s;
}

my $http_ok   = port_open($host, $http_port);
my $native_ok = port_open($host, $native_port);

die "Neither HTTP ($http_port) nor native ($native_port) reachable on $host\n"
    unless $http_ok || $native_ok;

# --- configs ---

my @configs;
if ($http_ok) {
    push @configs, { label => 'http',       protocol => 'http',   port => $http_port,   compress => 0 };
    push @configs, { label => 'http+gzip',  protocol => 'http',   port => $http_port,   compress => 1 };
}
if ($native_ok) {
    push @configs, { label => 'native',     protocol => 'native', port => $native_port, compress => 0 };
    push @configs, { label => 'native+lz4', protocol => 'native', port => $native_port, compress => 1 };
}

# --- helpers ---

sub fmt_rate {
    my ($n, $unit, $secs) = @_;
    my $rate = $n / $secs;
    my $r;
    if    ($rate >= 1_000_000) { $r = sprintf("%.1fM", $rate / 1_000_000) }
    elsif ($rate >= 1_000)     { $r = sprintf("%.1fK", $rate / 1_000) }
    else                       { $r = sprintf("%.0f",  $rate) }
    return sprintf("%s %s/s", $r, $unit);
}

sub bench {
    my (%a) = @_;
    my $cfg  = $a{config};
    my $run  = $a{run};
    my $setup = $a{setup};

    my ($ch, $t0);
    my $timeout = EV::timer(120, 0, sub {
        warn "  $cfg->{label}: TIMEOUT\n";
        EV::break;
    });

    $ch = EV::ClickHouse->new(
        host     => $host,
        port     => $cfg->{port},
        protocol => $cfg->{protocol},
        compress => $cfg->{compress},
        on_connect => sub {
            if ($setup) {
                $setup->($ch, sub {
                    $t0 = time;
                    $run->($ch, sub {
                        my ($count, $unit) = @_;
                        my $elapsed = time - $t0;
                        printf "  %-17s %8d %-8s %7.3fs  %s\n",
                            $cfg->{label}, $count, $unit, $elapsed, fmt_rate($count, $unit, $elapsed);
                        $ch->finish;
                        EV::break;
                    });
                });
            } else {
                $t0 = time;
                $run->($ch, sub {
                    my ($count, $unit) = @_;
                    my $elapsed = time - $t0;
                    printf "  %-17s %8d %-8s %7.3fs  %s\n",
                        $cfg->{label}, $count, $unit, $elapsed, fmt_rate($count, $unit, $elapsed);
                    $ch->finish;
                    EV::break;
                });
            }
        },
        on_error => sub {
            warn "  $cfg->{label}: ERROR: $_[0]\n";
            EV::break;
        },
    );
    EV::run;
}

# --- benchmarks ---

my $is_http = sub { $_[0]->{protocol} eq 'http' };
my $fmt_suffix = sub { $_[0]->{protocol} eq 'http' ? ' FORMAT TabSeparated' : '' };

printf "EV::ClickHouse benchmark (host=%s)\n", $host;
print "=" x 50, "\n\n";

# 1. Small SELECT
printf "Small SELECT (select 1) x %d\n", $small_n;
for my $cfg (@configs) {
    bench(
        config => $cfg,
        run => sub {
            my ($ch, $done) = @_;
            my $remain = $small_n;
            my $sql = "SELECT 1" . $fmt_suffix->($cfg);
            for (1 .. $small_n) {
                $ch->query($sql, sub {
                    my ($rows, $err) = @_;
                    warn "  select 1 error: $err\n" if $err;
                    $done->($small_n, 'queries') if --$remain == 0;
                });
            }
        },
    );
}
print "\n";

# 2. Large SELECT
printf "Large SELECT (numbers(%s)) x %d\n", $large_n, $large_r;
for my $cfg (@configs) {
    bench(
        config => $cfg,
        run => sub {
            my ($ch, $done) = @_;
            my $remain = $large_r;
            my $total_rows = 0;
            my $sql = "SELECT number FROM numbers($large_n)" . $fmt_suffix->($cfg);
            for (1 .. $large_r) {
                $ch->query($sql, sub {
                    my ($rows, $err) = @_;
                    if ($err) {
                        warn "  numbers error: $err\n";
                    } else {
                        $total_rows += scalar @$rows;
                    }
                    $done->($total_rows, 'rows') if --$remain == 0;
                });
            }
        },
    );
}
print "\n";

# 3. INSERT
printf "INSERT (%d rows) x %d\n", $insert_rows, $insert_r;
my $insert_data = join '', map { "$_\n" } (1 .. $insert_rows);

for my $cfg (@configs) {
    bench(
        config => $cfg,
        run => sub {
            my ($ch, $done) = @_;
            my $remain = $insert_r;
            my $total = 0;
            for (1 .. $insert_r) {
                $ch->insert("FUNCTION null('n UInt64')", $insert_data, sub {
                    my (undef, $err) = @_;
                    if ($err) {
                        warn "  insert error: $err\n";
                    } else {
                        $total += $insert_rows;
                    }
                    $done->($total, 'rows') if --$remain == 0;
                });
            }
        },
    );
}
print "\n";

# 4. Parse overhead: HTTP parsed vs HTTP raw vs native
my $parse_n = $ENV{BENCH_PARSE_N} // 500_000;
my $parse_r = $ENV{BENCH_PARSE_R} // 5;
printf "Parse overhead (%d rows x %d cols) x %d — HTTP parsed vs raw vs native\n", $parse_n, 5, $parse_r;
{
    my $sql_body = "SELECT number, number+1, number*2, toString(number), number%100 FROM numbers($parse_n)";

    # HTTP parsed
    if ($http_ok) {
        for my $compress (0, 1) {
            my $label = $compress ? 'http+gzip parsed' : 'http parsed';
            my $cfg = { label => $label, protocol => 'http', port => $http_port, compress => $compress };
            bench(
                config => $cfg,
                run => sub {
                    my ($ch, $done) = @_;
                    my $remain = $parse_r;
                    my $total_rows = 0;
                    my $sql = "$sql_body FORMAT TabSeparated";
                    for (1 .. $parse_r) {
                        $ch->query($sql, sub {
                            my ($rows, $err) = @_;
                            if ($err) { warn "  error: $err\n" }
                            else { $total_rows += scalar @$rows }
                            $done->($total_rows, 'rows') if --$remain == 0;
                        });
                    }
                },
            );
        }
    }

    # HTTP raw (no TSV parsing — just deliver body bytes)
    if ($http_ok) {
        for my $compress (0, 1) {
            my $label = $compress ? 'http+gzip raw' : 'http raw';
            my $cfg = { label => $label, protocol => 'http', port => $http_port, compress => $compress };
            bench(
                config => $cfg,
                run => sub {
                    my ($ch, $done) = @_;
                    my $remain = $parse_r;
                    my $total_bytes = 0;
                    my $sql = "$sql_body FORMAT TabSeparated";
                    for (1 .. $parse_r) {
                        $ch->query($sql, { raw => 1 }, sub {
                            my ($body, $err) = @_;
                            if ($err) { warn "  error: $err\n" }
                            else { $total_bytes += length($body) }
                            $done->($total_bytes, 'bytes') if --$remain == 0;
                        });
                    }
                },
            );
        }
    }

    # Native
    if ($native_ok) {
        for my $compress (0, 1) {
            my $label = $compress ? 'native+lz4' : 'native';
            my $cfg = { label => $label, protocol => 'native', port => $native_port, compress => $compress };
            bench(
                config => $cfg,
                run => sub {
                    my ($ch, $done) = @_;
                    my $remain = $parse_r;
                    my $total_rows = 0;
                    my $sql = $sql_body;
                    for (1 .. $parse_r) {
                        $ch->query($sql, sub {
                            my ($rows, $err) = @_;
                            if ($err) { warn "  error: $err\n" }
                            else { $total_rows += scalar @$rows }
                            $done->($total_rows, 'rows') if --$remain == 0;
                        });
                    }
                },
            );
        }
    }
}
print "\n";

# 5. Queue depth — fire all at once, measure total
printf "Queue burst (select 1) x %d\n", $small_n;
for my $cfg (@configs) {
    bench(
        config => $cfg,
        run => sub {
            my ($ch, $done) = @_;
            my $remain = $small_n;
            my $sql = "SELECT 1" . $fmt_suffix->($cfg);
            for (1 .. $small_n) {
                $ch->query($sql, sub {
                    $done->($small_n, 'queries') if --$remain == 0;
                });
            }
            printf "    queued %d (pending_count=%d)\n", $small_n, $ch->pending_count;
        },
    );
}
print "\n";

# 6. LowCardinality multi-block
my $lc_n = $ENV{BENCH_LC_N} // 1_000_000;
my $lc_r = $ENV{BENCH_LC_R} // 5;
printf "LowCardinality (%d rows, multi-block) x %d\n", $lc_n, $lc_r;
for my $cfg (grep { $_->{protocol} eq 'native' } @configs) {
    bench(
        config => $cfg,
        run => sub {
            my ($ch, $done) = @_;
            my $remain = $lc_r;
            my $total_rows = 0;
            my $sql = "SELECT toLowCardinality(toString(number % 100)) FROM numbers($lc_n)";
            for (1 .. $lc_r) {
                $ch->query($sql, sub {
                    my ($rows, $err) = @_;
                    if ($err) { warn "  LC error: $err\n" }
                    else { $total_rows += scalar @$rows }
                    $done->($total_rows, 'rows') if --$remain == 0;
                });
            }
        },
    );
}
print "\n";

# 7. Parameterized queries
my $param_n = $ENV{BENCH_PARAM_N} // 1000;
printf "Parameterized queries x %d\n", $param_n;
for my $cfg (@configs) {
    bench(
        config => $cfg,
        run => sub {
            my ($ch, $done) = @_;
            my $remain = $param_n;
            my $sql = "SELECT {x:UInt64} + {y:UInt64}" . $fmt_suffix->($cfg);
            for my $i (1 .. $param_n) {
                $ch->query($sql,
                    { params => { x => $i, y => $i * 2 } },
                    sub {
                        $done->($param_n, 'queries') if --$remain == 0;
                    });
            }
        },
    );
}
print "\n";

# 8. Streaming vs buffered (native only)
if ($native_ok) {
    my $stream_n = $ENV{BENCH_STREAM_N} // 500_000;
    my $stream_r = $ENV{BENCH_STREAM_R} // 3;
    printf "Streaming vs buffered (%d rows) x %d\n", $stream_n, $stream_r;

    # Buffered
    for my $compress (0, 1) {
        my $label = $compress ? 'native+lz4 buffered' : 'native buffered';
        my $cfg = { label => $label, protocol => 'native', port => $native_port, compress => $compress };
        bench(
            config => $cfg,
            run => sub {
                my ($ch, $done) = @_;
                my $remain = $stream_r;
                my $total = 0;
                for (1 .. $stream_r) {
                    $ch->query("SELECT number FROM numbers($stream_n)", sub {
                        my ($rows, $err) = @_;
                        $total += scalar @$rows unless $err;
                        $done->($total, 'rows') if --$remain == 0;
                    });
                }
            },
        );
    }

    # Streaming
    for my $compress (0, 1) {
        my $label = $compress ? 'native+lz4 streaming' : 'native streaming';
        my $cfg = { label => $label, protocol => 'native', port => $native_port, compress => $compress };
        bench(
            config => $cfg,
            run => sub {
                my ($ch, $done) = @_;
                my $remain = $stream_r;
                my $total = 0;
                for (1 .. $stream_r) {
                    $ch->query("SELECT number FROM numbers($stream_n)",
                        { on_data => sub { $total += scalar @{$_[0]} } },
                        sub {
                            $done->($total, 'rows') if --$remain == 0;
                        });
                }
            },
        );
    }
    print "\n";
}

# 9. Arrayref INSERT vs TSV INSERT (native only)
if ($native_ok) {
    my $av_rows = $ENV{BENCH_AV_ROWS} // 10_000;
    my $av_r    = $ENV{BENCH_AV_R}    // 50;
    printf "Arrayref vs TSV INSERT (%d rows) x %d\n", $av_rows, $av_r;

    my $tsv = join '', map { "$_\n" } (1 .. $av_rows);
    my @av  = map { [$_] } (1 .. $av_rows);

    for my $compress (0, 1) {
        my $csuf = $compress ? '+lz4' : '';

        # TSV
        my $cfg_tsv = { label => "native${csuf} TSV", protocol => 'native',
                        port => $native_port, compress => $compress };
        bench(
            config => $cfg_tsv,
            run => sub {
                my ($ch, $done) = @_;
                my $remain = $av_r;
                my $total = 0;
                for (1 .. $av_r) {
                    $ch->insert("FUNCTION null('n UInt64')", $tsv, sub {
                        $total += $av_rows;
                        $done->($total, 'rows') if --$remain == 0;
                    });
                }
            },
        );

        # Arrayref
        my $cfg_av = { label => "native${csuf} arrayref", protocol => 'native',
                       port => $native_port, compress => $compress };
        bench(
            config => $cfg_av,
            run => sub {
                my ($ch, $done) = @_;
                my $remain = $av_r;
                my $total = 0;
                for (1 .. $av_r) {
                    $ch->insert("FUNCTION null('n UInt64')", \@av, sub {
                        $total += $av_rows;
                        $done->($total, 'rows') if --$remain == 0;
                    });
                }
            },
        );
    }
    print "\n";
}

print "Done.\n";
