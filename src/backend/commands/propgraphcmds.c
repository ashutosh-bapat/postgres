/*-------------------------------------------------------------------------
 *
 * propgraphcmds.c
 *	  property graph manipulation
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/commands/propgraphcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/propgraphcmds.h"
#include "commands/tablecmds.h"
#include "utils/lsyscache.h"


ObjectAddress
CreatePropGraph(ParseState *pstate, CreatePropGraphStmt *stmt)
{
	CreateStmt *cstmt = makeNode(CreateStmt);
	ListCell   *lc;
	ObjectAddress address;
	List	   *vertex_relids = NIL;
	List	   *edge_relids = NIL;

	if (stmt->pgname->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("property graphs cannot be unlogged because they do not have storage")));

	// TODO: check whether it's using temp relations (see view.c)

	foreach (lc, stmt->vertex_tables)
	{
		RangeVar   *rv = lfirst_node(RangeVar, lc);
		Oid			relid;

		relid = RangeVarGetRelidExtended(rv, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);
		// TODO: check relkind, relpersistence

		if (list_member_oid(vertex_relids, relid))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("table \"%s\" specified more than once as vertex table", get_rel_name(relid))));

		// TODO: check for primary key or graph table key clause

		vertex_relids = lappend_oid(vertex_relids, relid);
	}

	foreach (lc, stmt->edge_tables)
	{
		List	   *etl = lfirst_node(List, lc);
		RangeVar   *rvedge = linitial_node(RangeVar, etl);
		RangeVar   *rvsource = lsecond_node(RangeVar, etl);
		RangeVar   *rvdest = lthird_node(RangeVar, etl);
		Oid			relid;
		Oid			relid2;
		Oid			relid3;

		relid = RangeVarGetRelidExtended(rvedge, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);
		// TODO: check relkind, relpersistence

		if (list_member_oid(edge_relids, relid))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("table \"%s\" specified more than once as edge table", get_rel_name(relid))));
		// XXX: also check that it's not already a vertex table?

		// TODO: check for primary key or graph table key clause

		relid2 = RangeVarGetRelidExtended(rvsource, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);
		if (!list_member_oid(vertex_relids, relid2))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("table \"%s\" specified as source vertex table is not in the vertex table list",
							get_rel_name(relid2)),
					 parser_errposition(pstate, rvsource->location)));

		relid3 = RangeVarGetRelidExtended(rvdest, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);
		if (!list_member_oid(vertex_relids, relid3))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("table \"%s\" specified as destination vertex table is not in the vertex table list",
							get_rel_name(relid3)),
					 parser_errposition(pstate, rvdest->location)));

		// TODO: check for appropriate foreign keys

		vertex_relids = lappend_oid(vertex_relids, relid);
	}

	cstmt->relation = stmt->pgname;
	cstmt->oncommit = ONCOMMIT_NOOP;
	address = DefineRelation(cstmt, RELKIND_PROPGRAPH, InvalidOid, NULL, NULL);

	return address;
}
