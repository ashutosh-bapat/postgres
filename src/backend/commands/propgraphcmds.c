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

#include "access/genam.h"
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
	char		components_persistence;
	ListCell   *lc, *lc2, *lc3;
	ObjectAddress pgaddress;
	List	   *vertex_relids = NIL;
	List	   *vertex_aliases = NIL;
	List	   *vertex_keys = NIL;
	List	   *edge_relids = NIL;
	List	   *edge_aliases = NIL;
	List	   *edge_keys = NIL;
	Relation	vertexrel;
	Relation	edgerel;

	if (stmt->pgname->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("property graphs cannot be unlogged because they do not have storage")));

	components_persistence = RELPERSISTENCE_PERMANENT;

	vertexrel = table_open(PropgraphVertexRelationId, RowExclusiveLock);
	edgerel = table_open(PropgraphEdgeRelationId, RowExclusiveLock);

	foreach (lc, stmt->vertex_tables)
	{
		PropGraphVertex *vertex = lfirst_node(PropGraphVertex, lc);
		Oid			relid;
		Relation	rel;
		char	   *aliasname;
		int2vector *iv;

		relid = RangeVarGetRelidExtended(vertex->vtable, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);

		rel = table_open(relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			components_persistence = RELPERSISTENCE_TEMP;

		if (!(rel->rd_rel->relkind == RELKIND_RELATION ||
			  rel->rd_rel->relkind == RELKIND_VIEW ||
			  rel->rd_rel->relkind == RELKIND_MATVIEW ||
			  rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
			  rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("invalid relkind TODO")));

		if (vertex->vtable->alias)
			aliasname = vertex->vtable->alias->aliasname;
		else
			aliasname = vertex->vtable->relname;

		if (list_member(vertex_aliases, makeString(aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as vertex table", aliasname),
					 parser_errposition(pstate, vertex->location)));

		if (vertex->vkey == NIL)
		{
			Oid			pkidx = RelationGetPrimaryKeyIndex(rel);

			if (!pkidx)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("vertex table key must be specified for table without primary key"),
						 parser_errposition(pstate, vertex->location)));
			else
			{
				Relation	indexDesc;

				indexDesc = index_open(pkidx, AccessShareLock);
				iv = buildint2vector(indexDesc->rd_index->indkey.values, indexDesc->rd_index->indkey.dim1);
				index_close(indexDesc, NoLock);
			}
		}
		else
		{
			int			numattrs;
			int16	   *attnums;
			int			i;

			numattrs = list_length(vertex->vkey);
			attnums = palloc(numattrs * sizeof(int16));

			i = 0;
			foreach(lc2, vertex->vkey)
			{
				char	   *colname = strVal(lfirst(lc2));
				AttrNumber	attnum;

				attnum = get_attnum(relid, colname);
				if (!attnum)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									colname, get_rel_name(relid)),
							 parser_errposition(pstate, vertex->location)));
				attnums[i++] = attnum;
			}

			for (int j = 0; j < numattrs; j++)
			{
				for (int k = j + 1; k < numattrs; k++)
				{
					if (attnums[j] == attnums[k])
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("graph key columns list must not contain duplicates"),
								 parser_errposition(pstate, vertex->location)));
				}
			}

			iv = buildint2vector(attnums, numattrs);
		}

		table_close(rel, NoLock);

		vertex_relids = lappend_oid(vertex_relids, relid);
		vertex_aliases = lappend(vertex_aliases, makeString(aliasname));
		vertex_keys = lappend(vertex_keys, iv);
	}

	foreach (lc, stmt->edge_tables)
	{
		PropGraphEdge *edge = lfirst_node(PropGraphEdge, lc);
		Oid			relid;
		Relation	rel;
		char	   *aliasname;
		int2vector *iv;

		relid = RangeVarGetRelidExtended(edge->etable, AccessShareLock, 0, RangeVarCallbackOwnsTable, NULL);

		rel = table_open(relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			components_persistence = RELPERSISTENCE_TEMP;

		if (!(rel->rd_rel->relkind == RELKIND_RELATION ||
			  rel->rd_rel->relkind == RELKIND_VIEW ||
			  rel->rd_rel->relkind == RELKIND_MATVIEW ||
			  rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
			  rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("invalid relkind TODO")));

		if (edge->etable->alias)
			aliasname = edge->etable->alias->aliasname;
		else
			aliasname = edge->etable->relname;

		if (list_member(edge_aliases, makeString(aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as edge table", aliasname),
					 parser_errposition(pstate, edge->location)));

		// XXX: also check that it's not already a vertex table?

		if (edge->ekey == NIL)
		{
			Oid			pkidx = RelationGetPrimaryKeyIndex(rel);

			if (!pkidx)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("edge table key must be specified for table without primary key"),
						 parser_errposition(pstate, edge->location)));
			else
			{
				Relation	indexDesc;

				indexDesc = index_open(pkidx, AccessShareLock);
				iv = buildint2vector(indexDesc->rd_index->indkey.values, indexDesc->rd_index->indkey.dim1);
				index_close(indexDesc, NoLock);
			}
		}
		else
		{
			int			numattrs;
			int16	   *attnums;
			int			i;

			numattrs = list_length(edge->ekey);
			attnums = palloc(numattrs * sizeof(int16));

			i = 0;
			foreach(lc2, edge->ekey)
			{
				char	   *colname = strVal(lfirst(lc2));
				AttrNumber	attnum;

				attnum = get_attnum(relid, colname);
				if (!attnum)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									colname, get_rel_name(relid)),
							 parser_errposition(pstate, edge->location)));
				attnums[i++] = attnum;
			}

			for (int j = 0; j < numattrs; j++)
			{
				for (int k = j + 1; k < numattrs; k++)
				{
					if (attnums[j] == attnums[k])
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("graph key columns list must not contain duplicates"),
								 parser_errposition(pstate, edge->location)));
				}
			}

			iv = buildint2vector(attnums, numattrs);
		}

		if (!list_member(vertex_aliases, makeString(edge->esrcvertex)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("source vertex \"%s\" of edge \"%s\" does not exist",
							edge->esrcvertex, aliasname),
					 parser_errposition(pstate, edge->location)));

		if (!list_member(vertex_aliases, makeString(edge->edestvertex)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("destination vertex \"%s\" of edge \"%s\" does not exist",
							edge->edestvertex, aliasname),
					 parser_errposition(pstate, edge->location)));

		// TODO: check for appropriate foreign keys

		table_close(rel, NoLock);

		edge_relids = lappend_oid(edge_relids, relid);
		edge_aliases = lappend(edge_aliases, makeString(aliasname));
		edge_keys = lappend(edge_keys, iv);
	}

	cstmt->relation = stmt->pgname;
	cstmt->oncommit = ONCOMMIT_NOOP;

	/*
	 * Automatically make it temporary if any component tables are temporary
	 * (see also DefineView()).
	 */
	if (stmt->pgname->relpersistence == RELPERSISTENCE_PERMANENT
		&& components_persistence == RELPERSISTENCE_TEMP)
	{
		cstmt->relation = copyObject(cstmt->relation);
		cstmt->relation->relpersistence = RELPERSISTENCE_TEMP;
		ereport(NOTICE,
				(errmsg("property graph \"%s\" will be temporary",
						stmt->pgname->relname)));
	}

	pgaddress = DefineRelation(cstmt, RELKIND_PROPGRAPH, InvalidOid, NULL, NULL);

	forthree(lc, vertex_relids,
			 lc2, vertex_aliases,
			 lc3, vertex_keys)
	{
		Oid			relid = lfirst_oid(lc);
		char	   *aliasstr = strVal(lfirst(lc2));
		int2vector *key = lfirst(lc3);
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
		values[Anum_pg_propgraph_vertex_pgvkey - 1] = PointerGetDatum(key);

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

	forthree(lc, edge_relids,
			 lc2, edge_aliases,
			 lc3, edge_keys)
	{
		Oid			relid = lfirst_oid(lc);
		char	   *aliasstr = strVal(lfirst(lc2));
		int2vector *key = lfirst(lc3);
		NameData	aliasname;
		Oid			peoid;
		Datum		values[Natts_pg_propgraph_edge] = {0};
		bool		nulls[Natts_pg_propgraph_edge] = {0};
		HeapTuple	tup;
		ObjectAddress myself;
		ObjectAddress referenced;

		peoid = GetNewOidWithIndex(edgerel, PropgraphEdgeObjectIndexId,
								   Anum_pg_propgraph_edge_oid);
		values[Anum_pg_propgraph_edge_oid - 1] = ObjectIdGetDatum(peoid);
		values[Anum_pg_propgraph_edge_pgepgid - 1] = ObjectIdGetDatum(pgaddress.objectId);
		values[Anum_pg_propgraph_edge_pgerelid - 1] = ObjectIdGetDatum(relid);
		namestrcpy(&aliasname, aliasstr);
		values[Anum_pg_propgraph_edge_pgealias - 1] = NameGetDatum(&aliasname);
		values[Anum_pg_propgraph_edge_pgesrcvertexid - 1] = 0; // TODO
		values[Anum_pg_propgraph_edge_pgedestvertexid - 1] = 0; // TODO
		values[Anum_pg_propgraph_edge_pgekey - 1] = PointerGetDatum(key);

		tup = heap_form_tuple(RelationGetDescr(edgerel), values, nulls);
		CatalogTupleInsert(edgerel, tup);
		heap_freetuple(tup);

		ObjectAddressSet(myself, PropgraphEdgeRelationId, peoid);

		/* Add dependency on the property graph */
		recordDependencyOn(&myself, &pgaddress, DEPENDENCY_INTERNAL);

		/* Add dependency on the relation */
		ObjectAddressSet(referenced, RelationRelationId, relid);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

#if 0
		/* Add dependencies on vertices */
		ObjectAddressSet(referenced, PropgraphVertexRelationId, XXX);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
		ObjectAddressSet(referenced, PropgraphVertexRelationId, YYY);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
#endif
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
