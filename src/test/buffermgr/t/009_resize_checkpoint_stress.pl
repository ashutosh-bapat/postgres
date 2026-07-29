# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Stress test racing pg_resize_shared_buffers() against a continuous
# CHECKPOINT under sustained pgbench write load, and asserting the
# server log is free of PANIC / SIGBUS afterwards.

use strict;
use warnings;
use IPC::Run;
use List::Util qw(shuffle);
use PostgreSQL::Test::Cluster;
use Test::More;

# =============================================================================
# Stress knobs
# =============================================================================

# Every dial is env-overridable.  Cap the duration at 80% of
# PG_TEST_TIMEOUT_DEFAULT (20% margin for setup, drain and
# end-of-run assertions) so a shortened CI budget does not wedge
# the harness.
my $pgb_duration    = int($ENV{PG_TEST_RESIZE_STRESS_SECONDS} // 120);
my $pgb_num_clients = int($ENV{PG_TEST_RESIZE_STRESS_CLIENTS} // 10);
my $pgb_num_jobs    = int($ENV{PG_TEST_RESIZE_STRESS_JOBS}    // 1);
my $pgb_scale       = int($ENV{PG_TEST_RESIZE_STRESS_SCALE}   // 10);

my $timeout_cap = int(($ENV{PG_TEST_TIMEOUT_DEFAULT} // 0) * 0.8);
if ($timeout_cap > 0 && $pgb_duration > $timeout_cap)
{
	note "clamping pgbench duration from $pgb_duration to $timeout_cap";
	$pgb_duration = $timeout_cap;
}

# =============================================================================
# Helpers
# =============================================================================

# True while any of the given application_names is still visible
# in pg_stat_activity.
sub pgbench_processes_active
{
	my ($node, @application_names) = @_;

	my $in_list = join ',', map { "'$_'" } @application_names;
	my $result = $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_stat_activity "
		  . "WHERE application_name IN ($in_list);");
	return int($result) > 0;
}

# Fire a CHECKPOINT in a fresh psql session and return the handle so
# the caller can join it later with finish_background_checkpoint().
sub start_background_checkpoint
{
	my ($node) = @_;

	my $session = $node->background_psql('postgres');
	$session->query_until(
		qr/starting_checkpoint/,
		q(
\echo starting_checkpoint
CHECKPOINT;
));
	return $session;
}

# Join a background CHECKPOINT session started above.
sub finish_background_checkpoint
{
	my ($session) = @_;

	$session->query_until(qr/checkpoint_done/,
		qq(\n\\echo checkpoint_done\n));
	$session->quit;
}

# Start a psql session that re-issues CHECKPOINT via \watch, so the
# checkpointer keeps firing regardless of what the main resize loop
# is doing.  Torn down with kill_kill in the END block.
sub start_background_checkpoint_loop
{
	my ($node) = @_;

	my $session = $node->background_psql('postgres');
	$session->query_until(
		qr/starting_checkpoint_loop/,
		q(
\echo starting_checkpoint_loop
CHECKPOINT;
\watch 0.5
));
	return $session;
}

# Resize the buffer pool to $new_size and log the outcome to
# resize_log.  $mode adds a per-iteration CHECKPOINT on top of the
# background CHECKPOINT loop for extra pressure:
#
#   checkpoint-first  - dispatch a CHECKPOINT, then start the resize;
#                       the two may overlap on the server side
#   resize-first      - resize first, then a fresh CHECKPOINT after
#                       it returns; no direct overlap but the extra
#                       CHECKPOINT often falls into the next resize
#   resize-only       - no extra CHECKPOINT; only the background
#                       \watch loop supplies checkpoint pressure
sub apply_and_verify_buffer_change
{
	my ($node, $new_size, $mode) = @_;

	$node->safe_psql('postgres', "ALTER SYSTEM SET shared_buffers = '$new_size'");
	$node->safe_psql('postgres', "SELECT pg_reload_conf()");

	my $ckpt = $mode eq 'checkpoint-first'
	  ? start_background_checkpoint($node) : undef;

	$node->safe_psql('postgres', qq{
		INSERT INTO resize_log(size, started_at, ended_at, num_tries)
		SELECT $new_size, started_at, ended_at, num_tries
		FROM pg_resize_shared_buffers_sql($new_size)
	});

	if ($mode eq 'resize-first')
	{
		$ckpt = start_background_checkpoint($node);
	}
	finish_background_checkpoint($ckpt) if defined $ckpt;

	# Any failure resizing the buffer pool will trip the test's
	# timeout; reaching here means the resize (and the paired
	# CHECKPOINT when one was fired) both completed.
	ok(1, "Buffer pool resized to $new_size ($mode)");
}

# Buffer sizes exercised by the resize loop.
my @buffer_sizes = (128, 28, 16 * 1024, 32 * 1024, 1024, 512, 16, 24, 256, 128 * 1024, 16 * 1024);

# Pick the next buffer size to resize to.  Chosen randomly, but the
# same size is never picked twice in a row.
my %picked_count;
{
	my @queue;
	my $last_picked;

	sub pick_next_size
	{
		if (!@queue)
		{
			@queue = shuffle(@buffer_sizes);
			if (defined $last_picked && @queue > 1 && $queue[0] == $last_picked)
			{
				@queue[0, 1] = @queue[1, 0];
			}
		}
		$last_picked = shift @queue;
		$picked_count{$last_picked}++;
		return $last_picked;
	}
}

# =============================================================================
# Cluster + workload setup
# =============================================================================

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;

$node->append_conf('postgresql.conf', qq{
max_shared_buffers = } . (sort { $b <=> $a } @buffer_sizes)[0] . qq{
shared_buffers = 16
log_statement = none
restart_after_crash = off
});

$node->start;
$node->safe_psql('postgres', "CREATE EXTENSION pg_buffercache");
$node->safe_psql('postgres', "CREATE EXTENSION buffermgr_test");
$node->safe_psql('postgres', "CREATE EXTENSION amcheck");

# Table to capture every resize's target size, timing and retry count.
$node->safe_psql('postgres', qq{
CREATE TABLE resize_log(
    size int NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    num_tries int NOT NULL);
});

# application_names for pg_stat_activity polling and log grepping.
my $app_short = 'pgbench_resize_stress_short';
my $app_long  = 'pgbench_resize_stress_long';

$node->pgbench(
	"--initialize --init-steps=dtpvg --scale=$pgb_scale --quiet",
	0,
	[qr{^$}],
	[	# stderr patterns to verify initialization stages
		qr{dropping old tables},
		qr{creating tables},
		qr{done in \d+\.\d\d s }
	],
	"pgbench initialization (scale=$pgb_scale)"
);

# Two concurrent pgbenches: -C reconnects every transaction so pins
# are released fast; the long-connection pgbench keeps its backends
# alive for the whole run, holding buffer pins across many
# statements.  Shrink is more likely to be legitimately refused under
# long-conn pressure, which is exactly what we want to stress.
my ($pgb_short_stdin, $pgb_short_stdout, $pgb_short_stderr) = ('', '', '');
my $pgb_short = IPC::Run::start(
	[
		'pgbench',
		'-p', $node->port,
		'-h', $node->host,
		'-T', $pgb_duration,
		'-c', $pgb_num_clients,
		'-j', $pgb_num_jobs,
		'-C',
		'--exit-on-abort',
		'--continue-on-error',
		"dbname=postgres application_name=$app_short"
	],
	'<' => \$pgb_short_stdin,
	'>' => \$pgb_short_stdout,
	'2>' => \$pgb_short_stderr
);
ok($pgb_short, "short-connection pgbench started");

my ($pgb_long_stdin, $pgb_long_stdout, $pgb_long_stderr) = ('', '', '');
my $pgb_long = IPC::Run::start(
	[
		'pgbench',
		'-p', $node->port,
		'-h', $node->host,
		'-T', $pgb_duration,
		'-c', $pgb_num_clients,
		'-j', $pgb_num_jobs,
		'--exit-on-abort',
		'--continue-on-error',
		"dbname=postgres application_name=$app_long"
	],
	'<' => \$pgb_long_stdin,
	'>' => \$pgb_long_stdout,
	'2>' => \$pgb_long_stderr
);
ok($pgb_long, "long-connection pgbench started");

# Continuous CHECKPOINT driver so resize-only iterations still race
# against an active checkpointer.
my $ckpt_loop = start_background_checkpoint_loop($node);

# Tear down every background process even if a subtest dies, so a
# stray pgbench / psql cannot outlive the test.  Uses kill_kill (not
# ->quit) on $ckpt_loop because psql inside \watch does not read
# stdin, so a graceful quit would not return.
END
{
	local $?;
	$ckpt_loop->{run}->kill_kill if defined $ckpt_loop;
	$pgb_short->kill_kill if defined $pgb_short;
	$pgb_long->kill_kill if defined $pgb_long;
	if (defined $node)
	{
		$node->stop('immediate', fail_ok => 1);
	}
}

# Snapshot the log tail before the stress loop so log_check() at the
# end scans only what the loop produced.
my $log_offset = -s $node->logfile;

# Reset background writer stats before starting the resize cycle.
$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('bgwriter')");

# =============================================================================
# Main loop
# =============================================================================

my @modes = ('checkpoint-first', 'resize-first', 'resize-only');
my $tests_completed = 0;

# Resize as many times as possible while either pgbench is running,
# cycling through the three race orders so each fires proportionally.
while (pgbench_processes_active($node, $app_short, $app_long))
{
	my $mode = $modes[$tests_completed % scalar(@modes)];
	apply_and_verify_buffer_change($node, pick_next_size(), $mode);
	$tests_completed++;
}

ok($tests_completed > scalar(@buffer_sizes),
	"All buffer size transitions were tested");
note "Completed $tests_completed resize iterations while pgbench was running";

# Every size must have been picked at least once.  Guaranteed by
# construction once $tests_completed >= scalar(@buffer_sizes), but
# assert it explicitly so a regression in the picker is caught here.
my @unpicked = grep { !$picked_count{$_} } @buffer_sizes;
is(scalar @unpicked, 0, "Every buffer size was exercised at least once")
  or diag("unpicked sizes: @unpicked");

# =============================================================================
# End-of-run reports and invariants
# =============================================================================

# Report resize latency distribution and max retry count.  A large
# max_tries relative to n means the resize path spent time waiting on
# pinned buffers -- the interesting stress signal from the long-conn
# pgbench.
note $node->safe_psql('postgres', q{
SELECT format('resize stats: n=%s, min=%s, avg=%s, max=%s, max_tries=%s',
              count(*),
              min(ended_at - started_at),
              avg(ended_at - started_at),
              max(ended_at - started_at),
              max(num_tries))
FROM resize_log});

# Report checkpointer activity.
note $node->safe_psql('postgres', q{
SELECT format('checkpointer stats: timed=%s, requested=%s, buffers_written=%s',
              num_timed, num_requested, buffers_written)
FROM pg_stat_checkpointer});

# Background writer must have done real work during the resize cycle.
is($node->safe_psql('postgres',
		"SELECT buffers_clean > 0 FROM pg_stat_bgwriter"),
	't', "Background writer ran during resize cycle");

# Stop both pgbenches.
$pgb_short->signal('TERM');
ok((IPC::Run::finish $pgb_short), "short-connection pgbench finished");
$pgb_long->signal('TERM');
ok((IPC::Run::finish $pgb_long), "long-connection pgbench finished");
undef $pgb_short;
undef $pgb_long;

# Stop the background CHECKPOINT loop.
$ckpt_loop->{run}->kill_kill;
undef $ckpt_loop;

# Only log pgbench stderr when non-empty, otherwise we drown the TAP
# output in progress-report lines.
note("short-connection pgbench stderr:\n$pgb_short_stderr")
  if $pgb_short_stderr ne '';
note("long-connection pgbench stderr:\n$pgb_long_stderr")
  if $pgb_long_stderr ne '';

# Assert no PANIC or SIGBUS crossed the server log during the run;
# the resize/CHECKPOINT interaction is exactly the code path where
# either would surface.  log_check compares against the offset we
# captured before the main loop.
$node->log_check("no PANIC or SIGBUS during stress run", $log_offset,
	log_unlike => [qr/PANIC/, qr/signal 7/]);

# amcheck sweep over the pgbench_* btree indexes to catch page-level
# corruption that survived the resize cycle.
my $amcheck_indexes = $node->safe_psql('postgres', q{
	SELECT c.oid::regclass::text
	  FROM pg_class c
	  JOIN pg_index i ON i.indexrelid = c.oid
	 WHERE c.relkind = 'i'
	   AND c.relam = (SELECT oid FROM pg_am WHERE amname = 'btree')
	   AND c.relnamespace = 'public'::regnamespace
});
for my $index (split /\n/, $amcheck_indexes)
{
	$node->safe_psql('postgres', "SELECT bt_index_check('$index', true)");
	ok(1, "bt_index_check passed on $index");
}

# pgbench transactions atomically UPDATE pgbench_accounts.abalance
# and INSERT into pgbench_history with the same delta; the sums must
# still agree after all the resize/CHECKPOINT racing.
my $sums = $node->safe_psql('postgres', q{
	SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts),
	       (SELECT COALESCE(SUM(delta), 0)    FROM pgbench_history)
});
my ($acct_sum, $hist_sum) = split /\|/, $sums;
is($acct_sum, $hist_sum,
	"pgbench_accounts.abalance sum matches pgbench_history.delta sum");

# Cluster is still functional after all the resize activity.
$node->connect_ok("dbname=postgres",
	"Database remains accessible after $tests_completed resize operations");

done_testing();
