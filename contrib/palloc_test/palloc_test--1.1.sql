/* contrib/xml2/palloc_test--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION palloc_test" to load this file. \quit

CREATE FUNCTION palloc_pfree(int, int default 100) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION mem_context_free(int, int default 100) RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
