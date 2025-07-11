/* contrib/exprs--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION exprs" to load this file. \quit

-----
-- memory context related functions
-----

-- returns time taken by 'num' number of chunks each of size 'csize' to be
-- palloc'ed and pfreed one at a time
CREATE FUNCTION palloc_pfree(num int, csize int default 100) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- returns time taken to palloc 'num' number of chunks, each of size 'csize' in
-- a memory context and delete that memory context.
CREATE FUNCTION mem_context_free(num int, csize int default 100) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-----
-- dynamic hash functions
-----

-- returns time taken to insert and search 'num_entries' number of entries into
-- and from a dynamic hash table with initial size max_entries.
CREATE FUNCTION sparse_hashes(num_entries int, max_entries int, insert_time out float8, search_time out float8, delete_time out float8) RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

-----
-- relids related functions
-----
-- returns time taken to execute bms_equal() 'reps' times on two singleton
-- bitmapsets each containing the given 'member'.
CREATE FUNCTION perf_relids_equal(member int, reps int default 1000000000) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- returns time taken to execute bms_overlap() 'reps' times on two
-- bitmapsets each containing the given respective 'member's.
CREATE FUNCTION perf_relids_overlap(bms1 int[], bms2 int[], reps int default 1000000000) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- returns time taken to execute bms_union() 'reps' times on two
-- bitmapsets each containing the given respective 'member's.
CREATE FUNCTION perf_relids_union(bms1 int[], bms2 int[], reps int default 1000000000) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
