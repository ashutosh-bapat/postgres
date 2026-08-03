
# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Test crash recovery when pg_resize_shared_buffers() and CHECKPOINT
# run concurrently and are made to wait through an interleaved
# sequence of injection points.  After stop immediate + restart,
# shared_buffers must reflect the requested value and
# pgbench_accounts.abalance must match pgbench_history.delta.
#
# Six shrink and four expand scenarios cover positions of the resize
# barriers relative to the checkpoint phases.  One shrink scenario
# (S6, 'shmem drop before BufferSync flush loop') exposes an upstream
# tight-loop in BufferSync and is skipped by default; see
# BUGS_FOUND.md BUG-005.
#
# Interleaved-injection-point pattern from the checksum TAP series in
# src/test/recovery/t/013_*.pl.

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use ResizeBuffer::Utils;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# =============================================================================
# Initialization
# =============================================================================

my $shrink_size = '1MB';
my $expand_size = '4MB';

my $node = setup_resize_node('resize_node', $expand_size, $expand_size);

# Do not let the postmaster restart itself; the test stops and starts
# the server on its own.
$node->safe_psql('postgres',
	"ALTER SYSTEM SET restart_after_crash = off");
$node->safe_psql('postgres', "SELECT pg_reload_conf()");

my $pgbench;

END
{
	local $?;
	if (defined $node)
	{
		wakeup_all_known_points($node);
		$node->stop('immediate', fail_ok => 1);
	}
}

# =============================================================================
# Helper functions
# =============================================================================

# Run one interleaving.
#
# Make the resize wait at $init and start a CHECKPOINT in the
# background; it waits at 'create-checkpoint-initial'.  Wake the
# points in @steps in order, alternating between the resizer and
# the checkpointer as @steps dictates.  Leave the resize waiting at
# $stop, stop immediate, restart, and verify shared_buffers = $final.
sub test_resize_sequence
{
	my ($start_size, $target_size, $init, $stop, $final, @steps) = @_;
	my $checkpoint_handle;

	$pgbench->finish if $pgbench;
	$pgbench = undef;

	request_shared_buffers($node, $start_size);
	$node->restart;
	$pgbench = background_rw_pgbench($node->port);

	verify_shared_buffers($node, $start_size,
		"setup: shared_buffers = $start_size");

	request_shared_buffers($node, $target_size);
	attach_injection_point($node, $init);
	attach_injection_point($node, $stop);
	attach_injection_point($node, $_) for @steps;

	my $resize_session = trigger_resize_async($node, $target_size);
	wait_injection_point($node, $init);

	$checkpoint_handle = background_checkpoint($node);
	wakeup_injection_point($node, $init);

	for my $point (@steps)
	{
		wait_injection_point($node, $point);
		wakeup_injection_point($node, $point);
	}

	wait_injection_point($node, $stop);

	$node->stop('immediate');
	$pgbench->kill_kill;
	$pgbench = undef;
	$checkpoint_handle->{run}->kill_kill;
	$checkpoint_handle = undef;
	$resize_session->{run}->kill_kill;
	$node->start;

	verify_shared_buffers($node, $final,
		"after crash recovery: shared_buffers = $final");

	$node->safe_psql('postgres', q{
		SELECT bt_index_check(c.oid, true)
		  FROM pg_class c
		  JOIN pg_index i ON i.indexrelid = c.oid
		 WHERE c.relkind = 'i'
		   AND c.relam = (SELECT oid FROM pg_am WHERE amname = 'btree')
		   AND c.relnamespace = 'public'::regnamespace
	});

	my $sums = $node->safe_psql('postgres', q{
		SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts),
			   (SELECT COALESCE(SUM(delta), 0)    FROM pgbench_history)
	});
	my ($acct_sum, $hist_sum) = split(/\|/, $sums);
	is($acct_sum, $hist_sum,
		"pgbench_accounts.abalance sum matches pgbench_history.delta sum");
}

# =============================================================================
# Main tests
# =============================================================================

# Each scenario is [ $label, \@steps, $flag? ].
#   $label -- note()d before the scenario runs.
#   \@steps -- injection points woken in order between $init and $stop;
#     interleaving resizer (pgrsb-*) and checkpointer (create-checkpoint-*,
#     checkpoint-*-wal, buffer-sync-*) points defines the race.
#   $flag -- optional; 'known_hang' skips the scenario unless
#     PG_TEST_RESIZE_KNOWN_HANGS is set (used for scenarios that trip a
#     product bug logged in BUGS_FOUND.md).
# $init is 'pg-resize-shared-buffers-flag-set' for both directions;
# $stop is the last barrier the resizer emits on that path.

# Shrink: 4MB -> 1MB.  Resizer stops at the buffer-pool-resize
# barrier, which is emitted last on the shrink path.
my @shrink_scenarios = (
	[   'shrink: checkpoint finishes before resize barriers',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'checkpoint-before-old-wal-removal',
			'pgrsb-new-buffer-alloc-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent' ] ],
	[   'shrink: resize barriers before checkpoint REDO',
		[   'create-checkpoint-initial',
			'pgrsb-new-buffer-alloc-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'checkpoint-before-old-wal-removal' ] ],
	[   'shrink: resize barriers span the REDO WAL insert',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'pgrsb-new-buffer-alloc-barrier-sent',
			'checkpoint-after-redo-wal',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-before-old-wal-removal' ] ],
	[   'shrink: resize after REDO WAL, before WAL removal',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'pgrsb-new-buffer-alloc-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-before-old-wal-removal' ] ],
	[   'shrink: shmem drop between BufferSync scan and flush',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'buffer-sync-after-scan',
			'pgrsb-new-buffer-alloc-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-before-old-wal-removal' ] ],
	# TODO(BUGS_FOUND.md BUG-005): S6 hangs upstream.  BufferSync's
	# `if (buf_id >= NBuffers) continue;` guard in bufmgr.c does not
	# advance the iterator, so the checkpointer tight-loops on the
	# first stale entry and never reaches
	# `checkpoint-before-old-wal-removal`.  Un-skip once upstream
	# fixes the loop advance and record the fixing SHA in
	# BUGS_FOUND.md BUG-005.  To exercise the hang locally, run with
	# `PG_TEST_RESIZE_KNOWN_HANGS=1`.
	[   'shrink: shmem drop before BufferSync flush loop',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'pgrsb-new-buffer-alloc-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent',
			'buffer-sync-heap-built',
			'checkpoint-before-old-wal-removal' ],
		'known_hang' ],
);
for my $s (@shrink_scenarios)
{
	my ($label, $steps, $flag) = @$s;
	if (defined $flag && $flag eq 'known_hang'
		&& !$ENV{PG_TEST_RESIZE_KNOWN_HANGS})
	{
		note "$label: SKIPPED (see BUGS_FOUND.md BUG-005; "
		  . "set PG_TEST_RESIZE_KNOWN_HANGS=1 to exercise)";
		next;
	}
	note $label;
	test_resize_sequence($expand_size, $shrink_size,
		'pg-resize-shared-buffers-flag-set',
		'pgrsb-buffer-pool-resize-barrier-sent',
		$shrink_size, @$steps);
}

# Expand: 1MB -> 4MB.  Resizer stops at the new-buffer-alloc barrier,
# which is emitted last on the expand path.
my @expand_scenarios = (
	[   'expand: checkpoint finishes before resize barriers',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'checkpoint-before-old-wal-removal',
			'pgrsb-buffer-pool-resize-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent' ] ],
	[   'expand: resize before checkpoint REDO',
		[   'create-checkpoint-initial',
			'pgrsb-buffer-pool-resize-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'checkpoint-before-old-wal-removal' ] ],
	[   'expand: shmem grow across REDO WAL insert',
		[   'create-checkpoint-initial',
			'pgrsb-buffer-pool-resize-barrier-sent',
			'checkpoint-before-redo-wal',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-after-redo-wal',
			'checkpoint-before-old-wal-removal' ] ],
	[   'expand: resize after REDO WAL, before WAL removal',
		[   'create-checkpoint-initial',
			'checkpoint-before-redo-wal',
			'checkpoint-after-redo-wal',
			'pgrsb-buffer-pool-resize-barrier-sent',
			'pgrsb-buffer-pool-size-barrier-sent',
			'checkpoint-before-old-wal-removal' ] ],
);
for my $s (@expand_scenarios)
{
	my ($label, $steps) = @$s;
	note $label;
	test_resize_sequence($shrink_size, $expand_size,
		'pg-resize-shared-buffers-flag-set',
		'pgrsb-new-buffer-alloc-barrier-sent',
		$expand_size, @$steps);
}

done_testing();
