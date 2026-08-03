
# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Shared helpers for the buffer-pool resize TAP tests that race
# pg_resize_shared_buffers() against checkpoints and crashes.

package ResizeBuffer::Utils;

use strict;
use warnings;

use Exporter 'import';
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

our @EXPORT = qw(
  setup_resize_node
  attach_injection_point
  wait_injection_point
  wakeup_injection_point
  wakeup_all_known_points
  request_shared_buffers
  trigger_resize_async
  wait_for_resize_done
  verify_shared_buffers
  background_rw_pgbench
  background_checkpoint
);

# Build and start a node.  Parallel workers are disabled because they
# allocate against the default shared_buffers regardless of
# max_shared_buffers; see the TODO at t/004_client_join_buffer_resize.pl.
# Skips the caller's test if the platform lacks have_resizable_shmem.
sub setup_resize_node
{
	my ($name, $initial, $max) = @_;

	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf(
		'postgresql.conf', qq{
shared_preload_libraries = injection_points
shared_buffers = $initial
max_shared_buffers = $max
max_parallel_workers_per_gather = 0
max_parallel_maintenance_workers = 0
bgwriter_lru_maxpages = 0
checkpoint_timeout = 1h
});
	$node->start;

	if ($node->safe_psql('postgres', "SHOW have_resizable_shmem") ne 'on')
	{
		$node->stop;
		plan skip_all => 'have_resizable_shmem is off on this platform';
	}

	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
	$node->safe_psql('postgres', 'CREATE EXTENSION pg_buffercache;');
	$node->safe_psql('postgres', 'CREATE EXTENSION amcheck;');
	$node->safe_psql('postgres', 'CREATE EXTENSION buffermgr_test;');

	$node->command_ok(
		[ 'pgbench', '-p', $node->port, '-i', '-s', '1', '-q', 'postgres' ],
		'pgbench -i succeeded');

	return $node;
}

# Backend type that fires each injection point.  Resizer points live in
# buf_resize.c, checkpointer points in xlog.c.
my %point_backend = (
	'pg-resize-shared-buffers-flag-set'     => 'client backend',
	'pgrsb-new-buffer-alloc-barrier-sent'   => 'client backend',
	'pgrsb-buffer-pool-size-barrier-sent'   => 'client backend',
	'pgrsb-buffer-pool-resize-barrier-sent' => 'client backend',
	'create-checkpoint-initial'             => 'checkpointer',
	'checkpoint-before-redo-wal'            => 'checkpointer',
	'checkpoint-after-redo-wal'             => 'checkpointer',
	'checkpoint-before-old-wal-removal'     => 'checkpointer',
	'buffer-sync-after-scan'                => 'checkpointer',
	'buffer-sync-heap-built'                => 'checkpointer',
);

sub attach_injection_point
{
	my ($node, $point) = @_;
	exists $point_backend{$point}
	  or die "attach_injection_point: unknown point '$point'; "
	  . "register it in %point_backend in ResizeBuffer::Utils";
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('$point','wait');");
}

sub wait_injection_point
{
	my ($node, $point) = @_;

	my $backend = $point_backend{$point}
	  or die "wait_injection_point: unknown point '$point'";

	$node->wait_for_event($backend, $point);
}

# Detach $point before waking it so the released backend cannot re-enter
# the same point before the wakeup is delivered.
sub wakeup_injection_point
{
	my ($node, $point) = @_;

	$node->safe_psql('postgres',
		"SELECT injection_points_detach('$point');");
	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('$point');");
}

# Detach and wake any injection point currently blocking a backend.
# Used from END blocks so a backend still waiting on a point does not
# hang the next stop.  No-op if the node is not running, so END-block
# cleanup does not itself die after a crash scenario.
sub wakeup_all_known_points
{
	my ($node) = @_;
	return unless defined $node && $node->is_alive;
	my @stuck = split /\n/, $node->safe_psql(
		'postgres', q{
		SELECT DISTINCT wait_event
		  FROM pg_stat_activity
		 WHERE wait_event_type = 'InjectionPoint'
	});
	for my $point (@stuck)
	{
		$node->safe_psql('postgres',
			"SELECT injection_points_detach('$point');");
		$node->safe_psql('postgres',
			"SELECT injection_points_wakeup('$point');");
	}
}

# ALTER SYSTEM + pg_reload_conf() to stage a new shared_buffers value.
# Must be called before trigger_resize_async(): buffermgr_test's
# pg_resize_shared_buffers_sql() raises if the target value is not
# visible ('setting' or the 'pending: <target>' display form) to the
# calling backend.
sub request_shared_buffers
{
	my ($node, $size) = @_;
	$node->safe_psql('postgres',
		"ALTER SYSTEM SET shared_buffers = '$size'");
	$node->safe_psql('postgres', "SELECT pg_reload_conf()");
}

# Fire pg_resize_shared_buffers() from a new background psql session.
# Uses buffermgr_test.pg_resize_shared_buffers_sql() so a transient
# false return retries and the GUC is checked after the resize.  $size
# is a shared_buffers-style string ('1MB', '128MB', ...); it is
# converted to block count server-side.
sub trigger_resize_async
{
	my ($node, $size) = @_;
	my $session = $node->background_psql('postgres');
	$session->query_until(
		qr/starting_resize/,
		qq{
			\\echo starting_resize
			SELECT pg_resize_shared_buffers_sql(
				(pg_size_bytes('$size') /
				 (SELECT setting::bigint FROM pg_settings
				  WHERE name = 'block_size'))::int);
			\\echo resize_done
		});
	return $session;
}

# Wait for the resize session started by trigger_resize_async() to
# report completion.  Not useful in scenarios that stop immediate
# before pg_resize_shared_buffers_sql() returns.
sub wait_for_resize_done
{
	my ($session) = @_;
	$session->query_until(qr/resize_done/, '');
}

# Verify shared_buffers reports $expected, comparing block counts so
# that display-unit differences do not cause spurious failures.
sub verify_shared_buffers
{
	my ($node, $expected, $label) = @_;
	$label //= "shared_buffers is $expected";
	my $actual = $node->safe_psql('postgres',
		"SELECT setting::bigint FROM pg_settings "
		. "WHERE name = 'shared_buffers'");
	my $want = $node->safe_psql('postgres',
		"SELECT pg_size_bytes('$expected')::bigint "
		. "/ current_setting('block_size')::int");
	is($actual, $want, $label);
}

# Start a long-running pgbench -n against $port in a background
# process.  -n is required so that pgbench_history is not truncated at
# each scenario boundary.
sub background_rw_pgbench
{
	my ($port) = @_;

	# --exit-on-abort makes a server crash (e.g. SIGBUS in the
	# checkpointer) kill pgbench instead of hiding behind a long
	# client-side timeout.  --continue-on-error lets pgbench survive
	# transient errors that surface during a resize race.
	my @cmd = ('pgbench', '-n', '-p', $port, '-T', 600, '-c', 1,
		'--exit-on-abort', '--continue-on-error', 'postgres');

	return IPC::Run::start(
		\@cmd,
		'<' => '/dev/null',
		'>' => '/dev/null',
		'2>' => '/dev/null',
		IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default));
}

# Start a CHECKPOINT in a new background psql session.  The caller is
# expected to make the checkpointer wait at an injection point so the
# CHECKPOINT does not complete before the test drives it forward.
sub background_checkpoint
{
	my ($node) = @_;

	my $session = $node->background_psql('postgres');
	$session->query_until(
		qr/starting_checkpoint/,
		q(\echo starting_checkpoint
CHECKPOINT;
));
	return $session;
}

1;
