/* contrib/exprs--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION exprs" to load this file. \quit

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

-- returns time taken to execute bms_equal() 'reps' times on two singleton
-- bitmapsets each containing the given 'member'.
CREATE FUNCTION perf_bms_equal(member int, reps int default 1000000000) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- returns time taken to execute bms_overlap() 'reps' times on two
-- bitmapsets each containing the given respective 'member's.
CREATE FUNCTION perf_bms_overlap(bms1 int[], bms2 int[], reps int default 1000000000) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- returns time taken to execute bms_union() 'reps' times on two
-- bitmapsets each containing the given respective 'member's.
CREATE FUNCTION perf_bms_union(bms1 int[], bms2 int[], reps int default 1000000000) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
