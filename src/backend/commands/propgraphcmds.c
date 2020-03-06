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

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_propgraph_edge.h"
#include "catalog/pg_propgraph_vertex.h"
#include "commands/propgraphcmds.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


ObjectAddress
CreatePropGraph(ParseState *pstate, CreatePropGraphStmt *stmt)
{
	CreateStmt *cstmt = makeNode(CreateStmt);
	ListCell   *lc, *lc2;
	ObjectAddress pgaddress;
	List	   *vertex_relids = NIL;
	List	   *vertex_aliases = NIL;
	List	   *edge_relids = NIL;
	Relation	vertexrel;
	Relation	edgerel;

	if (stmt->pgname->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("property graphs cannot be unlogged because they do not have storage")));

	// TODO: check whether it's using temp relations (see view.c)

	vertexrel = table_open(PropgraphVertexRelationId, RowExclusiveLock);
	edgerel = table_open(PropgraphEdgeRelationId, RowExclusiveLock);

	foreach (lc, stmt->vertex_tables)
	{
		RangeVar   *rv = lfirst_node(RangeVar, lc);
		Oid			relid;
		char	   *aliasname;

		relid = RangeVarGetRelidExtended(rv, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);
		// TODO: check relkind, relpersistence

		if (rv->alias)
			aliasname = rv->alias->aliasname;
		else
			aliasname = rv->relname;

		if (list_member(vertex_aliases, makeString(aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as vertex table", aliasname)));

		// TODO: check for primary key or graph table key clause

		vertex_relids = lappend_oid(vertex_relids, relid);
		vertex_aliases = lappend(vertex_aliases, makeString(aliasname));
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

		edge_relids = lappend_oid(edge_relids, relid);
	}

	cstmt->relation = stmt->pgname;
	cstmt->oncommit = ONCOMMIT_NOOP;
	pgaddress = DefineRelation(cstmt, RELKIND_PROPGRAPH, InvalidOid, NULL, NULL);

	forboth(lc, vertex_relids, lc2, vertex_aliases)
	{
		Oid			relid = lfirst_oid(lc);
		char	   *aliasstr = strVal(lfirst(lc2));
		NameData	aliasname;
		Oid			pvoid;
		Datum		values[Natts_pg_propgraph_vertex] = {0};
		bool		nulls[Natts_pg_propgraph_vertex] = {0};
		HeapTuple	tup;
		ObjectAddress myself;
		ObjectAddress referenced;

		pvoid = GetNewOidWithIndex(vertexrel, PropgraphVertexObjectIndexId,
								   Anum_pg_propgraph_vertex_oid);
		values[Anum_pg_propgraph_vertex_oid - 1] = ObjectIdGetDatum(pvoid);
		values[Anum_pg_propgraph_vertex_pgvpgid - 1] = ObjectIdGetDatum(pgaddress.objectId);
		values[Anum_pg_propgraph_vertex_pgvrelid - 1] = ObjectIdGetDatum(relid);
		namestrcpy(&aliasname, aliasstr);
		values[Anum_pg_propgraph_vertex_pgvalias - 1] = NameGetDatum(&aliasname);
		values[Anum_pg_propgraph_vertex_pgvkey - 1] = PointerGetDatum(buildint2vector(NULL, 0));

		tup = heap_form_tuple(RelationGetDescr(vertexrel), values, nulls);
		CatalogTupleInsert(vertexrel, tup);
		heap_freetuple(tup);

		ObjectAddressSet(myself, PropgraphVertexRelationId, pvoid);

		/* Add dependency on the property graph */
		recordDependencyOn(&myself, &pgaddress, DEPENDENCY_INTERNAL);

		/* Add dependency on the relation */
		ObjectAddressSet(referenced, RelationRelationId, relid);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
	}

	table_close(edgerel, RowExclusiveLock);
	table_close(vertexrel, RowExclusiveLock);

	return pgaddress;
}

void
RemovePropgraphEdgeById(Oid peid)
{
	HeapTuple	tup;
	Relation	rel;

	rel = table_open(PropgraphEdgeRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PROPGRAPHEDGEOID, ObjectIdGetDatum(peid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for property graph edge %u", peid);

	CatalogTupleDelete(rel, &tup->t_self);
	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);
}

void
RemovePropgraphVertexById(Oid pvid)
{
	HeapTuple	tup;
	Relation	rel;

	rel = table_open(PropgraphVertexRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PROPGRAPHVERTEXOID, ObjectIdGetDatum(pvid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for property graph vertex %u", pvid);

	CatalogTupleDelete(rel, &tup->t_self);
	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);
}
