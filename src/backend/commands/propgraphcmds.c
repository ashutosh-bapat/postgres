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
#include "catalog/pg_propgraph_element.h"
#include "commands/propgraphcmds.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static int2vector *propgraph_element_get_key(ParseState *pstate, List *key_clause, int location, Relation element_rel);
static int2vector *int2vector_from_column_list(ParseState *pstate, List *colnames, int location, Relation element_rel);


struct element_info
{
	char	kind;
	Oid		relid;
	char   *aliasname;
	int2vector *key;
	Oid		srcrelid;
	int2vector *srckey;
	int2vector *srcref;
	Oid		destrelid;
	int2vector *destkey;
	int2vector *destref;
};


ObjectAddress
CreatePropGraph(ParseState *pstate, CreatePropGraphStmt *stmt)
{
	CreateStmt *cstmt = makeNode(CreateStmt);
	char		components_persistence;
	ListCell   *lc;
	ObjectAddress pgaddress;
	List	   *element_infos = NIL;
	List	   *element_aliases = NIL;
	Relation	elrel;

	if (stmt->pgname->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("property graphs cannot be unlogged because they do not have storage")));

	components_persistence = RELPERSISTENCE_PERMANENT;

	elrel = table_open(PropgraphElementRelationId, RowExclusiveLock);

	foreach (lc, stmt->vertex_tables)
	{
		PropGraphVertex *vertex = lfirst_node(PropGraphVertex, lc);
		Oid			relid;
		Relation	rel;
		char	   *aliasname;
		int2vector *iv;
		struct element_info *einfo;

		relid = RangeVarGetRelidExtended(vertex->vtable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

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

		if (list_member(element_aliases, makeString(aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as element table", aliasname),
					 parser_errposition(pstate, vertex->location)));

		iv = propgraph_element_get_key(pstate, vertex->vkey, vertex->location, rel);

		table_close(rel, NoLock);

		einfo = palloc0_object(struct element_info);
		einfo->kind = PGEKIND_VERTEX;
		einfo->relid = relid;
		einfo->aliasname = aliasname;
		einfo->key = iv;
		element_infos = lappend(element_infos, einfo);

		element_aliases = lappend(element_aliases, makeString(aliasname));
	}

	foreach (lc, stmt->edge_tables)
	{
		PropGraphEdge *edge = lfirst_node(PropGraphEdge, lc);
		Oid			relid;
		Relation	rel;
		char	   *aliasname;
		int2vector *iv;
		ListCell   *lc2;
		Oid			srcrelid;
		Oid			destrelid;
		Relation	srcrel;
		Relation	destrel;
		int2vector *srckeyiv;
		int2vector *srcrefiv;
		int2vector *destkeyiv;
		int2vector *destrefiv;
		struct element_info *einfo;

		relid = RangeVarGetRelidExtended(edge->etable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

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

		if (list_member(element_aliases, makeString(aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as element table", aliasname),
					 parser_errposition(pstate, edge->location)));

		iv = propgraph_element_get_key(pstate, edge->ekey, edge->location, rel);

		srcrelid = 0;
		destrelid = 0;
		foreach (lc2, element_infos)
		{
			struct element_info *einfo = lfirst(lc2);

			if (einfo->kind == PGEKIND_VERTEX)
			{
				if (strcmp(einfo->aliasname, edge->esrcvertex) == 0)
					srcrelid = einfo->relid;

				if (strcmp(einfo->aliasname, edge->edestvertex) == 0)
					destrelid = einfo->relid;

				if (srcrelid && destrelid)
					break;
			}
		}
		if (!srcrelid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("source vertex \"%s\" of edge \"%s\" does not exist",
							edge->esrcvertex, aliasname),
					 parser_errposition(pstate, edge->location)));
		if (!destrelid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("destination vertex \"%s\" of edge \"%s\" does not exist",
							edge->edestvertex, aliasname),
					 parser_errposition(pstate, edge->location)));

		if (!edge->esrckey || !edge->esrcvertexcols || !edge->edestkey || !edge->edestvertexcols)
			elog(ERROR, "TODO foreign key support");

		srcrel = table_open(srcrelid, NoLock);
		destrel = table_open(destrelid, NoLock);

		if (list_length(edge->esrckey) != list_length(edge->esrcvertexcols))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("mismatching number of columns in source vertex definition of edge \"%s\"",
							aliasname),
					 parser_errposition(pstate, edge->location)));

		if (list_length(edge->edestkey) != list_length(edge->edestvertexcols))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("mismatching number of columns in destination vertex definition of edge \"%s\"",
							aliasname),
					 parser_errposition(pstate, edge->location)));

		srckeyiv = int2vector_from_column_list(pstate, edge->esrckey, edge->location, rel);
		srcrefiv = int2vector_from_column_list(pstate, edge->esrcvertexcols, edge->location, srcrel);
		destkeyiv = int2vector_from_column_list(pstate, edge->edestkey, edge->location, rel);
		destrefiv = int2vector_from_column_list(pstate, edge->edestvertexcols, edge->location, destrel);

		// TODO: various consistency checks

		table_close(destrel, NoLock);
		table_close(srcrel, NoLock);

		table_close(rel, NoLock);

		einfo = palloc0_object(struct element_info);
		einfo->kind = PGEKIND_EDGE;
		einfo->relid = relid;
		einfo->aliasname = aliasname;
		einfo->key = iv;
		einfo->srcrelid = srcrelid;
		einfo->srckey = srckeyiv;
		einfo->srcref = srcrefiv;
		einfo->destrelid = destrelid;
		einfo->destkey = destkeyiv;
		einfo->destref = destrefiv;
		element_infos = lappend(element_infos, einfo);

		element_aliases = lappend(element_aliases, makeString(aliasname));
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

	foreach(lc, element_infos)
	{
		struct element_info *einfo = lfirst(lc);
		NameData	aliasname;
		Oid			peoid;
		Datum		values[Natts_pg_propgraph_element] = {0};
		bool		nulls[Natts_pg_propgraph_element] = {0};
		HeapTuple	tup;
		ObjectAddress myself;
		ObjectAddress referenced;

		peoid = GetNewOidWithIndex(elrel, PropgraphElementObjectIndexId,
								   Anum_pg_propgraph_element_oid);
		values[Anum_pg_propgraph_element_oid - 1] = ObjectIdGetDatum(peoid);
		values[Anum_pg_propgraph_element_pgepgid - 1] = ObjectIdGetDatum(pgaddress.objectId);
		values[Anum_pg_propgraph_element_pgerelid - 1] = ObjectIdGetDatum(einfo->relid);
		namestrcpy(&aliasname, einfo->aliasname);
		values[Anum_pg_propgraph_element_pgealias - 1] = NameGetDatum(&aliasname);
		values[Anum_pg_propgraph_element_pgekind - 1] = CharGetDatum(einfo->kind);
		values[Anum_pg_propgraph_element_pgesrcvertexid - 1] = ObjectIdGetDatum(einfo->srcrelid);
		values[Anum_pg_propgraph_element_pgedestvertexid - 1] = ObjectIdGetDatum(einfo->destrelid);
		values[Anum_pg_propgraph_element_pgekey - 1] = PointerGetDatum(einfo->key);

		if (einfo->srckey)
			values[Anum_pg_propgraph_element_pgesrckey - 1] = PointerGetDatum(einfo->srckey);
		else
			nulls[Anum_pg_propgraph_element_pgesrckey - 1] = true;
		if (einfo->srcref)
			values[Anum_pg_propgraph_element_pgesrcref - 1] = PointerGetDatum(einfo->srcref);
		else
			nulls[Anum_pg_propgraph_element_pgesrcref - 1] = true;
		if (einfo->destkey)
			values[Anum_pg_propgraph_element_pgedestkey - 1] = PointerGetDatum(einfo->destkey);
		else
			nulls[Anum_pg_propgraph_element_pgedestkey - 1] = true;
		if (einfo->destref)
			values[Anum_pg_propgraph_element_pgedestref - 1] = PointerGetDatum(einfo->destref);
		else
			nulls[Anum_pg_propgraph_element_pgedestref - 1] = true;

		tup = heap_form_tuple(RelationGetDescr(elrel), values, nulls);
		CatalogTupleInsert(elrel, tup);
		heap_freetuple(tup);

		ObjectAddressSet(myself, PropgraphElementRelationId, peoid);

		/* Add dependency on the property graph */
		recordDependencyOn(&myself, &pgaddress, DEPENDENCY_INTERNAL);

		/* Add dependency on the relation */
		ObjectAddressSet(referenced, RelationRelationId, einfo->relid);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

		/* Add dependencies on vertices */
		// TODO: columns
		if (einfo->srcrelid)
		{
			ObjectAddressSet(referenced, PropgraphElementRelationId, einfo->srcrelid);
			recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
		}
		if (einfo->destrelid)
		{
			ObjectAddressSet(referenced, PropgraphElementRelationId, einfo->destrelid);
			recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
		}
	}

	table_close(elrel, RowExclusiveLock);

	return pgaddress;
}

static int2vector *
propgraph_element_get_key(ParseState *pstate, List *key_clause, int location, Relation element_rel)
{
	int2vector *iv;

	if (key_clause == NIL)
	{
		Oid			pkidx = RelationGetPrimaryKeyIndex(element_rel);

		if (!pkidx)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("element table key must be specified for table without primary key"),
					 parser_errposition(pstate, location)));
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
		iv = int2vector_from_column_list(pstate, key_clause, location, element_rel);
	}

	return iv;
}

static int2vector *
int2vector_from_column_list(ParseState *pstate, List *colnames, int location, Relation element_rel)
{
	int			numattrs;
	int16	   *attnums;
	int			i;
	ListCell   *lc;

	numattrs = list_length(colnames);
	attnums = palloc(numattrs * sizeof(int16));

	i = 0;
	foreach(lc, colnames)
	{
		char	   *colname = strVal(lfirst(lc));
		Oid			relid = RelationGetRelid(element_rel);
		AttrNumber	attnum;

		attnum = get_attnum(relid, colname);
		if (!attnum)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							colname, get_rel_name(relid)),
					 parser_errposition(pstate, location)));
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
						 parser_errposition(pstate, location)));
		}
	}

	return buildint2vector(attnums, numattrs);
}

void
RemovePropgraphElementById(Oid peid)
{
	HeapTuple	tup;
	Relation	rel;

	rel = table_open(PropgraphElementRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(peid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for property graph element %u", peid);

	CatalogTupleDelete(rel, &tup->t_self);
	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);
}
