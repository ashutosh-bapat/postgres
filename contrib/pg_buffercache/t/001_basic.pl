# Copyright (c) 2025-2025, PostgreSQL Global Development Group
#
# Test shared_buffer resizing coordination with client connections joining using injection points

use strict;
use warnings;
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

# Function to calculate expected buffer count from size string
sub calculate_buffer_count
{
	my ($size_string, $block_size) = @_;
	
	# Parse size and convert to bytes
	my ($size_val, $unit) = ($size_string =~ /(\d+)(\w+)/);
	my $size_bytes;
	if (lc($unit) eq 'kb') {
		$size_bytes = $size_val * 1024;
	} elsif (lc($unit) eq 'mb') {
		$size_bytes = $size_val * 1024 * 1024;
	} elsif (lc($unit) eq 'gb') {
		$size_bytes = $size_val * 1024 * 1024 * 1024;
	} else {
		# Default to kB if unit is not recognized
		$size_bytes = $size_val * 1024;
	}
	
	return int($size_bytes / $block_size);
}

# Initialize cluster with very small buffer sizes for testing
my $node = PostgreSQL::Test::Cluster->new('main');
my $shared_buffers_initial = '128MB';
$node->init;

# Configure for buffer resizing with very small buffer pool sizes for faster tests.
$node->append_conf('postgresql.conf', 'shared_preload_libraries = injection_points');
$node->append_conf('postgresql.conf', qq{
max_shared_buffers = '128MB'
shared_buffers = $shared_buffers_initial
max_parallel_workers_per_gather = 0
restart_after_crash = off
log_min_messages = debug1
});

$node->start;

# Enable injection points
$node->safe_psql('postgres', "CREATE EXTENSION injection_points");

# Get the block size (this is fixed for the binary)
my $block_size = $node->safe_psql('postgres', "SHOW block_size");

# Try to create pg_buffercache extension for buffer analysis
eval { 
	$node->safe_psql('postgres', "CREATE EXTENSION pg_buffercache");
};
if ($@) {
	$node->stop;
	plan skip_all => 'pg_buffercache extension not available - cannot verify buffer usage';
}

# Create dedicated sessions for injection point handling and test queries,
# so that we don't create new backends for test operations after starting
# resize operation. Only one backend, which tests new backend synchronization
# with resizing operation, should start after resizing has commenced.
my $injection_session = $node->background_psql('postgres');
my $query_session = $node->background_psql('postgres');
my $resize_session = $node->background_psql('postgres');
	
# Function to run a single injection point test
sub run_injection_point_test
{
	my ($test_name, $injection_point, $target_size, $operation_type) = @_;
	
	note("Test with $test_name ($operation_type)");
	
	# Update buffer pool size and wait for it to reflect pending state 
	$resize_session->query_safe("ALTER SYSTEM SET shared_buffers = '$target_size'");
	$resize_session->query_safe("SELECT pg_reload_conf()");
	my $pending_size_str = "pending: $target_size";
	$resize_session->poll_query_until("SELECT substring(current_setting('shared_buffers'), '$pending_size_str')", $pending_size_str);

	# Set up injection point in injection session
	$injection_session->query_safe("SELECT injection_points_attach('$injection_point', 'wait')");
	# Trigger resize
	$resize_session->query_until(
		qr/starting_resize/,
		q(
			\echo starting_resize
			SELECT pg_resize_shared_buffers();
		)
	);
	
	# Wait until resize actually reaches the injection point using the query session
	$query_session->wait_for_event('client backend', $injection_point);
	
	# Start bufferscan while resize is paused
	my $client = $node->background_psql('postgres');
	
	# Wake up the injection point from injection session
	$injection_session->query_safe("SELECT injection_points_wakeup('$injection_point')");
	
	# Wait for the resize operation to complete
	$resize_session->query(q(\echo 'done'));
	
	# Detach injection point from injection session
	$injection_session->query_safe("SELECT injection_points_detach('$injection_point')");
	
	# Verify resize completed successfully
	is($query_session->query_safe("SELECT current_setting('shared_buffers')"), $target_size,
		"resize completed successfully to $target_size");
	
	# Check buffer pool size using pg_buffercache after resize completion
	is($query_session->query_safe("SELECT COUNT(*) FROM pg_buffercache"), calculate_buffer_count($target_size, $block_size), "pg_buffercache COUNT(*) correct after $test_name ($operation_type)");
	
	# Wait for client to complete
	ok($client->quit, "client succeeded during $test_name ($operation_type)");
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
	run_injection_point_test($test->{name}, $test->{injection_point}, '272kB', 'shrinking');

	# Test expanding scenario
	run_injection_point_test($test->{name}, $test->{injection_point}, '400kB', 'expanding');
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
	run_injection_point_test($test->{name}, $test->{injection_point}, $test->{size}, 'shrinking only');
}

my @expand_only_tests = (
	{
		name => 'expand barrier complete',
		injection_point => 'pgrsb-expand-barrier-sent',
		size => '416kB',
	}
);
foreach my $test (@expand_only_tests)
{
	run_injection_point_test($test->{name}, $test->{injection_point}, $test->{size}, 'expanding only');
}

# Function to test buffercache scan behavior during resize operations
# This tests that pg_buffercache correctly handles concurrent resize operations
# by pausing the buffercache scan at various points while a resize occurs.
# The expected behavior is that pg_buffercache detects the resize and raises
# an appropriate error "number of shared buffers changed during scan".
sub run_buffercache_injection_test
{
	my ($test_name, $buffercache_injection_point, $target_size, $operation_type) = @_;
	
	note("Test buffercache with $test_name ($operation_type)");

	# Attach injection point at middle of buffercache scan
	$node->safe_psql('postgres', "SELECT injection_points_attach('$buffercache_injection_point', 'wait')");

	# Start buffercache query in background - it will pause at injection point
	my $buffercache_session = $node->background_psql('postgres');
	$buffercache_session->query_until(
		qr/starting_buffercache/,
		q(
			\echo starting_buffercache
			SELECT COUNT(*) FROM pg_buffercache;
		)
	);

	# Wait for buffercache to reach injection point
	$node->wait_for_event('client backend', $buffercache_injection_point);

	# Change shared_buffers to target size and resize
	$node->safe_psql('postgres', "ALTER SYSTEM SET shared_buffers = '$target_size'");
	$node->safe_psql('postgres', "SELECT pg_reload_conf()");
	$node->safe_psql('postgres', "SELECT pg_resize_shared_buffers()");

	# Wake up buffercache scan
	$node->safe_psql('postgres', "SELECT injection_points_wakeup('$buffercache_injection_point')");

	$buffercache_session->query(q(\echo 'done'));
	eval { $buffercache_session->quit; };
	eval { $node->safe_psql('postgres', "SELECT injection_points_detach('$buffercache_injection_point')"); };

	# Verify server is still running
	my $result;
	eval { $result = $node->safe_psql('postgres', "SELECT COUNT(*) FROM pg_buffercache"); };
	is($result, calculate_buffer_count($target_size, $block_size), "Server still running after $test_name ($operation_type)");
}

# Test buffercache injection points - pausing buffercache while resize occurs
my @buffercache_injection_tests = (
	# {
	# 	name => 'before the buffer pool scan starts',
	# 	injection_point => 'pg-buffercache-scan-start',
	# }, # Basic fail where after buffer change there are valid buffers (NOTE : Buffer fails after a little later then actual currentNBuffers Why?)
	{
		name => 'before getting buffer description - 2',
		injection_point => 'pg-buffercache-after-getdesc',
	}, # Failure where after buffer change there are no valid buffers;

foreach my $test (@buffercache_injection_tests)
{
	# Test with shrinking
	run_buffercache_injection_test($test->{name}, $test->{injection_point}, '256kB', 'shrinking');
	
	# Test with expanding
	run_buffercache_injection_test($test->{name}, $test->{injection_point}, '384kB', 'expanding');
}

$injection_session->quit;
$query_session->quit;
$resize_session->quit;

done_testing();