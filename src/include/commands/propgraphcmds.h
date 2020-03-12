/*-------------------------------------------------------------------------
 *
 * propgraphcmds.h
 *	  prototypes for propgraphcmds.c.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/propgraphcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PROPGRAPHCMDS_H
#define PROPGRAPHCMDS_H

#include "catalog/objectaddress.h"
#include "parser/parse_node.h"

extern ObjectAddress CreatePropGraph(ParseState *pstate, CreatePropGraphStmt *stmt);
extern void RemovePropgraphEdgeById(Oid peid);
extern void RemovePropgraphVertexById(Oid pvid);

#endif							/* PROPGRAPHCMDS_H */
