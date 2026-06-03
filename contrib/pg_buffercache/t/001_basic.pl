# Copyright (c) 2025-2026, PostgreSQL Global Development Group
#
# Test pg_buffercache scan behavior during shared_buffer resizing using
# injection points.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Skip this test if injection points are not supported
if (($ENV{enable_injection_points} // '') ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('main');
my $shared_buffers_initial = '8MB';
$node->init;
$node->append_conf('postgresql.conf', qq{
	shared_preload_libraries = 'injection_points'
	max_shared_buffers = $shared_buffers_initial
	shared_buffers = $shared_buffers_initial
	restart_after_crash = off
});
$node->start;

# Load injection_points and pg_buffercache extensions
$node->safe_psql('postgres', "CREATE EXTENSION injection_points");
$node->safe_psql('postgres', "CREATE EXTENSION pg_buffercache");

# Create dedicated sessions for injection point handling and test queries,
# so that we don't create new backends for test operations after starting
# resize operation.
my $injection_session = $node->background_psql('postgres');
my $query_session = $node->background_psql('postgres');
my $resize_session = $node->background_psql('postgres');

# Pause the buffer pool resize at the given injection point and run a
# pg_buffercache scan while the resize is paused.  After the scan completes,
# wake up the resize operation and verify that both the resize and the scan
# produce correct results.
sub run_scan_during_paused_resize
{
	my ($test_name, $injection_point, $target_size, $target_buffers,
		$operation_type) = @_;

	my $verbose = 0;

	note("Test $test_name ($operation_type)");

	# Update buffer pool size
	$resize_session->query_safe("ALTER SYSTEM SET shared_buffers = '$target_size'", verbose => $verbose);
	$resize_session->query_safe("SELECT pg_reload_conf()", verbose => $verbose);

	# Set up injection point in injection session
	$injection_session->query_safe("SELECT injection_points_attach('$injection_point', 'wait')", verbose => $verbose);

	# Start the resize in background - it will pause at injection point
	$resize_session->query_until(
		qr/starting_resize/,
		q(
			\echo starting_resize
			SELECT pg_resize_shared_buffers();
		)
	);

	# Wait until resize actually reaches the injection point using the query session
	$query_session->wait_for_event('client backend', $injection_point, verbose => $verbose);

	# Start a client while resize is paused and verify scan succeeds
	my $client = $node->background_psql('postgres');
	my $client_count = $client->query_safe("SELECT count(*) FROM pg_buffercache", verbose => $verbose);
	cmp_ok($client_count, '>', 0, "client scan returned rows during $test_name ($operation_type)");

	# Wake up the injection point from injection session
	$injection_session->query_safe("SELECT injection_points_wakeup('$injection_point')", verbose => $verbose);

	# Wait for the resize operation to complete.
	$resize_session->query_safe(q(\echo 'done'), verbose => $verbose);

	# Detach injection point from injection session
	$injection_session->query_safe("SELECT injection_points_detach('$injection_point')", verbose => $verbose);

	# Check buffer pool size using pg_buffercache after resize completion
	is($query_session->query_safe("SELECT COUNT(*) FROM pg_buffercache", verbose => $verbose),
		$target_buffers, "pg_buffercache count matches after $test_name ($operation_type)");

	# Wait for client to complete
	ok($client->quit, "client succeeded during $test_name ($operation_type)");
}

# Pause a pg_buffercache operation (pg_buffercache_scan and
# pg_buffercache_evict) at the given injection point, resize the buffer pool
# while the operation is paused, then wake it up and verify that the server
# remains functional and the resize took effect.
sub run_resize_during_paused_operation
{
	my ($test_name, $injection_point, $operation_sql, $target_size,
		$target_buffers, $operation_type) = @_;

	my $verbose = 0;

	note("Test $test_name ($operation_type)");

	# Set up injection point in injection session
	$injection_session->query_safe("SELECT injection_points_attach('$injection_point', 'wait')", verbose => $verbose);

	# Start the operation in background - it will pause at injection point.
	# Use on_error_stop => 0 so psql stays alive if the query errors out.
	my $op_session = $node->background_psql('postgres', on_error_stop => 0);
	$op_session->query_until(
		qr/starting_op/,
		qq(
			\\echo starting_op
			$operation_sql
		)
	);

	# Wait until the operation actually reaches the injection point using the query session
	$query_session->wait_for_event('client backend', $injection_point, verbose => $verbose);

	# Start a resize operation while the first operation is paused at injection point
	$node->safe_psql('postgres', "ALTER SYSTEM SET shared_buffers = '$target_size'");
	$node->safe_psql('postgres', "SELECT pg_reload_conf()");

	$node->safe_psql('postgres', "SELECT pg_resize_shared_buffers()", verbose => $verbose);

	# Wake up the injection point from injection session
	$injection_session->query_safe("SELECT injection_points_wakeup('$injection_point')", verbose => $verbose);

	# Collect the operation output and verify session completed
	my $op_output = $op_session->query_safe(q(\echo 'done'), verbose => $verbose);
	note("operation stdout during $test_name ($operation_type): \n" . $op_output);
	note("operation stderr during $test_name ($operation_type): \n" . ($op_session->{stderr} // ''));
	ok($op_session->quit, "operation session completed during $test_name ($operation_type)");

	# Detach injection point from injection session
	$injection_session->query_safe("SELECT injection_points_detach('$injection_point')", verbose => $verbose);

	# Check buffer pool size using pg_buffercache after resize completion
	is($query_session->query_safe("SELECT COUNT(*) FROM pg_buffercache", verbose => $verbose),
		$target_buffers, "pg_buffercache count matches after $test_name ($operation_type)");
}

# Test injection points during buffer resize with client connections
my @common_injection_tests = (
	{
		name => 'flag setting phase',
		injection_point => 'pg-resize-shared-buffers-flag-set',
	},
	{
		name => 'memory remap phase',
		injection_point => 'pgrsb-after-shmem-resize',
	},
	{
		name => 'resize map barrier complete',
		injection_point => 'pgrsb-resize-barrier-sent',
	},
);

# Test common injection points for both shrinking and expanding
foreach my $test (@common_injection_tests)
{
	# Test shrinking scenario
	run_scan_during_paused_resize($test->{name}, $test->{injection_point}, '272kB', '34', 'shrinking');

	# Test expanding scenario
	run_scan_during_paused_resize($test->{name}, $test->{injection_point}, '400kB', '50', 'expanding');
}

my @shrink_only_tests = (
	{
		name => 'shrink barrier complete',
		injection_point => 'pgrsb-shrink-barrier-sent',
		size => '200kB',
	}
);
foreach my $test (@shrink_only_tests)
{
	run_scan_during_paused_resize($test->{name}, $test->{injection_point}, $test->{size}, '25', 'shrinking only');
}

my @expand_only_tests = (
	{
		name => 'expand barrier complete',
		injection_point => 'pgrsb-expand-barrier-sent',
		size => '8MB',
	}
);
foreach my $test (@expand_only_tests)
{
	run_scan_during_paused_resize($test->{name}, $test->{injection_point}, $test->{size}, '1024', 'expanding only');
}

# Test buffercache injection points - pausing buffercache while resize occurs
my @buffercache_injection_tests = (
	{
		name => 'before the buffer pool scan starts',
		injection_point => 'pg-buffercache-scan-start',
	}, # Basic fail where after buffer change there are valid buffers
	# TODO: Enable once pg-buffercache-after-getdesc handles mid-scan
	# descriptor invalidation correctly after a shrink.
	# {
	# 	name => 'before getting buffer description',
	# 	injection_point => 'pg-buffercache-after-getdesc',
	# },
);

foreach my $test (@buffercache_injection_tests)
{
	# Test with shrinking
	run_resize_during_paused_operation($test->{name}, $test->{injection_point},
		'SELECT COUNT(*) FROM pg_buffercache;', '256kB', '32', 'shrinking');

	# Test with expanding
	run_resize_during_paused_operation($test->{name}, $test->{injection_point},
		'SELECT COUNT(*) FROM pg_buffercache;', '384kB', '48', 'expanding');
}

# Test evict with resize - pausing evict while resize occurs.
# After shrinking, buffer 33 is beyond the new pool size.  The evict read
# currentNBuffers (1024) before the shrink, so it considers 33 valid and
# attempts the evict on a buffer that no longer belongs to the pool.
# After expanding, buffer 1 is always valid.
my @evict_injection_tests = (
	{
		name => 'evict invalid buffer after shrink',
		injection_point => 'pg-buffercache-evict-before-check',
		sql => 'SELECT pg_buffercache_evict(33);',
		size => '256kB',
		buffers => '32',
		type => 'shrinking',
	},
	{
		name => 'evict valid buffer after expand',
		injection_point => 'pg-buffercache-evict-before-check',
		sql => 'SELECT pg_buffercache_evict(1);',
		size => '384kB',
		buffers => '48',
		type => 'expanding',
	},
);

foreach my $test (@evict_injection_tests)
{
	run_resize_during_paused_operation($test->{name}, $test->{injection_point},
		$test->{sql}, $test->{size}, $test->{buffers}, $test->{type});
}

$injection_session->quit;
$query_session->quit;
$resize_session->quit;

done_testing();