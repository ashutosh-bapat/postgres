
# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Test crash recovery when pg_resize_shared_buffers() is made to wait
# at a resizer barrier while a concurrent CHECKPOINT runs to
# completion, and when the checkpointer is made to wait inside
# BufferSync during a shrink.  After stop immediate + restart,
# shared_buffers must reflect the requested value and pgbench data
# must remain consistent.
#
# pgbench sum invariant and amcheck sweep pattern from recovery/013.

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

# Make the resize wait at $point1 (a client-backend injection point),
# run a full CHECKPOINT while it waits, wake the resize, then either
# make it wait at $point2 or let the resize finish.  Stop the server
# immediate and verify $final shared_buffers on restart.
sub test_resize_crash
{
	my ($start_size, $target_size, $point1, $point2, $final) = @_;

	$pgbench->finish if $pgbench;
	$pgbench = undef;

	# Restart at $start_size so each scenario runs from a clean
	# shared-buffer segment.
	request_shared_buffers($node, $start_size);
	$node->restart;
	$pgbench = background_rw_pgbench($node->port);
	verify_shared_buffers($node, $start_size,
		"setup: shared_buffers = $start_size");

	request_shared_buffers($node, $target_size);
	attach_injection_point($node, $point1);
	attach_injection_point($node, $point2) if defined $point2;

	my $resize_session = trigger_resize_async($node, $target_size);
	wait_injection_point($node, $point1);

	$node->safe_psql('postgres', "CHECKPOINT");

	wakeup_injection_point($node, $point1);

	if (defined $point2)
	{
		wait_injection_point($node, $point2);
	}
	else
	{
		wait_for_resize_done($resize_session);
	}

	$node->stop('immediate');
	$pgbench->kill_kill;
	$pgbench = undef;
	$resize_session->{run}->kill_kill;
	$node->start;

	verify_shared_buffers($node, $final,
		"after crash recovery: shared_buffers = $final");

	# Catch page-level corruption that survived the resize + crash.
	$node->safe_psql('postgres', q{
		SELECT bt_index_check(c.oid, true)
		  FROM pg_class c
		  JOIN pg_index i ON i.indexrelid = c.oid
		 WHERE c.relkind = 'i'
		   AND c.relam = (SELECT oid FROM pg_am WHERE amname = 'btree')
		   AND c.relnamespace = 'public'::regnamespace
	});

	# Invariant: SUM(pgbench_accounts.abalance) == SUM(pgbench_history.delta)
	# because each pgbench transaction updates the two together.  -n on
	# the pgbench command line keeps pgbench_history intact across
	# scenarios.
	my $sums = $node->safe_psql('postgres', q{
		SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts),
			   (SELECT COALESCE(SUM(delta), 0)    FROM pgbench_history)
	});
	my ($acct_sum, $hist_sum) = split(/\|/, $sums);
	is($acct_sum, $hist_sum,
		"pgbench_accounts.abalance sum matches pgbench_history.delta sum");
}

# Make the checkpointer wait at $bufsync_point (inside BufferSync), run
# a shrink to completion while it waits, then wake the checkpointer and
# stop the server immediate.  Verify $final shared_buffers on restart.
sub test_bufsync_crash
{
	my ($start_size, $target_size, $bufsync_point, $final) = @_;

	my $checkpoint_handle;

	$pgbench->finish if $pgbench;
	$pgbench = undef;

	request_shared_buffers($node, $start_size);
	$node->restart;
	$pgbench = background_rw_pgbench($node->port);
	verify_shared_buffers($node, $start_size,
		"setup: shared_buffers = $start_size");

	request_shared_buffers($node, $target_size);
	attach_injection_point($node, $bufsync_point);

	$checkpoint_handle = background_checkpoint($node);
	wait_injection_point($node, $bufsync_point);

	my $resize_session = trigger_resize_async($node, $target_size);
	wait_for_resize_done($resize_session);

	wakeup_injection_point($node, $bufsync_point);

	$node->stop('immediate');
	$pgbench->kill_kill;
	$pgbench = undef;
	$checkpoint_handle->{run}->kill_kill;
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

# Shrink-path resizer injection points, in the order the resizer
# reaches them.  Each adjacent pair defines one bracket: the
# CHECKPOINT runs between them.
my @shrink_seq = qw(
  pg-resize-shared-buffers-flag-set
  pgrsb-new-buffer-alloc-barrier-sent
  pgrsb-buffer-pool-size-barrier-sent
  pgrsb-buffer-pool-resize-barrier-sent
);
for my $i (0 .. $#shrink_seq)
{
	my ($p1, $p2) = @shrink_seq[$i, $i + 1];
	test_resize_crash($expand_size, $shrink_size, $p1, $p2, $shrink_size);
}

# Expand-path resizer injection points, in the order the resizer
# reaches them.
my @expand_seq = qw(
  pg-resize-shared-buffers-flag-set
  pgrsb-buffer-pool-resize-barrier-sent
  pgrsb-buffer-pool-size-barrier-sent
  pgrsb-new-buffer-alloc-barrier-sent
);
for my $i (0 .. $#expand_seq)
{
	my ($p1, $p2) = @expand_seq[$i, $i + 1];
	test_resize_crash($shrink_size, $expand_size, $p1, $p2, $expand_size);
}

# BufferSync has two checkpointer-side injection points.  The resize
# runs to completion between attach and wake, so each point is
# exercised on its own rather than as a consecutive pair.
for my $point ('buffer-sync-after-scan', 'buffer-sync-heap-built')
{
	test_bufsync_crash($expand_size, $shrink_size, $point, $shrink_size);
}

done_testing();
