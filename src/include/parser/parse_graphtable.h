/*-------------------------------------------------------------------------
 *
 * parse_graphtable.h
 *		parsing of GRAPH_TABLE
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_graphtable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_GRAPHTABLE_H
#define PARSE_GRAPHTABLE_H

#include "nodes/pg_list.h"

typedef struct GraphTableParseState
{
	Oid graphid;
	List *variables;
	const char *localvar;
} GraphTableParseState;

extern Node *graph_table_property_reference(ParseState *pstate, ColumnRef *cref);

Node *transformGraphPattern(GraphTableParseState *gpstate, List *graph_pattern);

#endif							/* PARSE_GRAPHTABLE_H */
