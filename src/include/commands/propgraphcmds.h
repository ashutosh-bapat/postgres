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
#include "nodes/parsenodes.h"

extern ObjectAddress CreatePropGraph(ParseState *pstate, CreatePropGraphStmt *stmt);

#endif							/* PROPGRAPHCMDS_H */
