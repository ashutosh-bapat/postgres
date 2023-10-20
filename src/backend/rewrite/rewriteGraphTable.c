/*-------------------------------------------------------------------------
 *
 * rewriteGraphTable.c
 *		Support for rewriting GRAPH_TABLE clauses.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/rewrite/rewriteGraphTable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "rewrite/rewriteGraphTable.h"


Query *rewriteGraphTable(Query *parsetree, int rt_index)
{
	elog(ERROR, "GRAPH_TABLE is not implemented");
	return NULL;
}
