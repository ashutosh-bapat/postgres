/*-------------------------------------------------------------------------
 *
 * propgraphcmds.h
 *	  prototypes for propgraphcmds.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
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

extern ObjectAddress CreatePropGraph(ParseState *pstate, const CreatePropGraphStmt *stmt);
extern ObjectAddress AlterPropGraph(ParseState *pstate, const AlterPropGraphStmt *stmt);
extern Oid	GetPropGraphForComponent(const ObjectAddress *object);
extern void CheckPropGraphLabelConsistency(Oid pgrelid);

#endif							/* PROPGRAPHCMDS_H */
