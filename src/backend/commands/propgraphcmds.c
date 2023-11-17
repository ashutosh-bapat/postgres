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
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_propgraph_element.h"
#include "catalog/pg_propgraph_label.h"
#include "catalog/pg_propgraph_property.h"
#include "commands/propgraphcmds.h"
#include "commands/tablecmds.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


struct element_info
{
	Oid		elementid;
	char	kind;
	Oid		relid;
	char   *aliasname;
	int2vector *key;

	char   *srcvertex;
	Oid		srcvertexid;
	int2vector *srckey;
	int2vector *srcref;

	char   *destvertex;
	Oid		destvertexid;
	int2vector *destkey;
	int2vector *destref;

	List   *labels;
};


static int2vector *propgraph_element_get_key(ParseState *pstate, List *key_clause, int location, Relation element_rel);
static int2vector *int2vector_from_column_list(ParseState *pstate, List *colnames, int location, Relation element_rel);
static void insert_element_record(ObjectAddress pgaddress, struct element_info *einfo);
static Oid insert_label_record(Oid peoid, const char *label);
static void insert_property_records(Oid graphid, Oid labeloid, Oid pgerelid, PropGraphProperties *properties);
static void insert_property_record(Oid graphid, Oid labeloid, const char *propname, Expr *expr);
static Oid get_vertex_oid(ParseState *pstate, Oid pgrelid, const char *alias, int location);
static Oid get_edge_oid(ParseState *pstate, Oid pgrelid, const char *alias, int location);
static Oid get_element_relid(Oid peid);


ObjectAddress
CreatePropGraph(ParseState *pstate, CreatePropGraphStmt *stmt)
{
	CreateStmt *cstmt = makeNode(CreateStmt);
	char		components_persistence;
	ListCell   *lc;
	ObjectAddress pgaddress;
	List	   *vertex_infos = NIL;
	List	   *edge_infos = NIL;
	List	   *element_aliases = NIL;

	if (stmt->pgname->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("property graphs cannot be unlogged because they do not have storage")));

	components_persistence = RELPERSISTENCE_PERMANENT;

	foreach (lc, stmt->vertex_tables)
	{
		PropGraphVertex *vertex = lfirst_node(PropGraphVertex, lc);
		struct element_info *vinfo;
		Relation	rel;

		vinfo = palloc0_object(struct element_info);
		vinfo->kind = PGEKIND_VERTEX;

		vinfo->relid = RangeVarGetRelidExtended(vertex->vtable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

		rel = table_open(vinfo->relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			components_persistence = RELPERSISTENCE_TEMP;

		if (vertex->vtable->alias)
			vinfo->aliasname = vertex->vtable->alias->aliasname;
		else
			vinfo->aliasname = vertex->vtable->relname;

		if (list_member(element_aliases, makeString(vinfo->aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as element table", vinfo->aliasname),
					 parser_errposition(pstate, vertex->location)));

		vinfo->key = propgraph_element_get_key(pstate, vertex->vkey, vertex->location, rel);

		vinfo->labels = vertex->labels;

		table_close(rel, NoLock);

		vertex_infos = lappend(vertex_infos, vinfo);

		element_aliases = lappend(element_aliases, makeString(vinfo->aliasname));
	}

	foreach (lc, stmt->edge_tables)
	{
		PropGraphEdge *edge = lfirst_node(PropGraphEdge, lc);
		struct element_info *einfo;
		Relation	rel;
		ListCell   *lc2;
		Oid			srcrelid;
		Oid			destrelid;
		Relation	srcrel;
		Relation	destrel;

		einfo = palloc0_object(struct element_info);
		einfo->kind = PGEKIND_EDGE;

		einfo->relid = RangeVarGetRelidExtended(edge->etable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

		rel = table_open(einfo->relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			components_persistence = RELPERSISTENCE_TEMP;

		if (edge->etable->alias)
			einfo->aliasname = edge->etable->alias->aliasname;
		else
			einfo->aliasname = edge->etable->relname;

		if (list_member(element_aliases, makeString(einfo->aliasname)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("alias \"%s\" used more than once as element table", einfo->aliasname),
					 parser_errposition(pstate, edge->location)));

		einfo->key = propgraph_element_get_key(pstate, edge->ekey, edge->location, rel);

		einfo->srcvertex = edge->esrcvertex;
		einfo->destvertex = edge->edestvertex;

		srcrelid = 0;
		destrelid = 0;
		foreach (lc2, vertex_infos)
		{
			struct element_info *vinfo = lfirst(lc2);

			if (strcmp(vinfo->aliasname, edge->esrcvertex) == 0)
				srcrelid = vinfo->relid;

			if (strcmp(vinfo->aliasname, edge->edestvertex) == 0)
				destrelid = vinfo->relid;

			if (srcrelid && destrelid)
				break;
		}
		if (!srcrelid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("source vertex \"%s\" of edge \"%s\" does not exist",
							edge->esrcvertex, einfo->aliasname),
					 parser_errposition(pstate, edge->location)));
		if (!destrelid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("destination vertex \"%s\" of edge \"%s\" does not exist",
							edge->edestvertex, einfo->aliasname),
					 parser_errposition(pstate, edge->location)));

		if (!edge->esrckey || !edge->esrcvertexcols || !edge->edestkey || !edge->edestvertexcols)
			elog(ERROR, "TODO foreign key support");

		srcrel = table_open(srcrelid, NoLock);
		destrel = table_open(destrelid, NoLock);

		if (list_length(edge->esrckey) != list_length(edge->esrcvertexcols))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("mismatching number of columns in source vertex definition of edge \"%s\"",
							einfo->aliasname),
					 parser_errposition(pstate, edge->location)));

		if (list_length(edge->edestkey) != list_length(edge->edestvertexcols))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("mismatching number of columns in destination vertex definition of edge \"%s\"",
							einfo->aliasname),
					 parser_errposition(pstate, edge->location)));

		einfo->srckey = int2vector_from_column_list(pstate, edge->esrckey, edge->location, rel);
		einfo->srcref = int2vector_from_column_list(pstate, edge->esrcvertexcols, edge->location, srcrel);
		einfo->destkey = int2vector_from_column_list(pstate, edge->edestkey, edge->location, rel);
		einfo->destref = int2vector_from_column_list(pstate, edge->edestvertexcols, edge->location, destrel);

		// TODO: various consistency checks

		einfo->labels = edge->labels;

		table_close(destrel, NoLock);
		table_close(srcrel, NoLock);

		table_close(rel, NoLock);

		edge_infos = lappend(edge_infos, einfo);

		element_aliases = lappend(element_aliases, makeString(einfo->aliasname));
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

	foreach(lc, vertex_infos)
	{
		struct element_info *vinfo = lfirst(lc);

		insert_element_record(pgaddress, vinfo);
	}

	foreach(lc, edge_infos)
	{
		struct element_info *einfo = lfirst(lc);
		ListCell *lc2;

		/*
		 * Look up the vertices again.  Now the vertices have OIDs assigned,
		 * which we need.
		 */
		foreach (lc2, vertex_infos)
		{
			struct element_info *vinfo = lfirst(lc2);

			if (strcmp(vinfo->aliasname, einfo->srcvertex) == 0)
				einfo->srcvertexid = vinfo->elementid;
			if (strcmp(vinfo->aliasname, einfo->destvertex) == 0)
				einfo->destvertexid = vinfo->elementid;
			if (einfo->srcvertexid && einfo->destvertexid)
				break;
		}
		Assert(einfo->srcvertexid);
		Assert(einfo->destvertexid);
		insert_element_record(pgaddress, einfo);
	}

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

static void
insert_element_record(ObjectAddress pgaddress, struct element_info *einfo)
{
	Oid			graphid = pgaddress.objectId;
	Relation	rel;
	NameData	aliasname;
	Oid			peoid;
	Datum		values[Natts_pg_propgraph_element] = {0};
	bool		nulls[Natts_pg_propgraph_element] = {0};
	HeapTuple	tup;
	ObjectAddress myself;
	ObjectAddress referenced;

	rel = table_open(PropgraphElementRelationId, RowExclusiveLock);

	peoid = GetNewOidWithIndex(rel, PropgraphElementObjectIndexId, Anum_pg_propgraph_element_oid);
	einfo->elementid = peoid;
	values[Anum_pg_propgraph_element_oid - 1] = ObjectIdGetDatum(peoid);
	values[Anum_pg_propgraph_element_pgepgid - 1] = ObjectIdGetDatum(graphid);
	values[Anum_pg_propgraph_element_pgerelid - 1] = ObjectIdGetDatum(einfo->relid);
	namestrcpy(&aliasname, einfo->aliasname);
	values[Anum_pg_propgraph_element_pgealias - 1] = NameGetDatum(&aliasname);
	values[Anum_pg_propgraph_element_pgekind - 1] = CharGetDatum(einfo->kind);
	values[Anum_pg_propgraph_element_pgesrcvertexid - 1] = ObjectIdGetDatum(einfo->srcvertexid);
	values[Anum_pg_propgraph_element_pgedestvertexid - 1] = ObjectIdGetDatum(einfo->destvertexid);
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

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PropgraphElementRelationId, peoid);

	/* Add dependency on the property graph */
	recordDependencyOn(&myself, &pgaddress, DEPENDENCY_AUTO);

	/* Add dependency on the relation */
	ObjectAddressSet(referenced, RelationRelationId, einfo->relid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* Add dependencies on vertices */
	// TODO: columns
	if (einfo->srcvertexid)
	{
		ObjectAddressSet(referenced, PropgraphElementRelationId, einfo->srcvertexid);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
	if (einfo->destvertexid)
	{
		ObjectAddressSet(referenced, PropgraphElementRelationId, einfo->destvertexid);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	table_close(rel, NoLock);

	if (einfo->labels)
	{
		ListCell *lc;

		foreach(lc, einfo->labels)
		{
			PropGraphLabelAndProperties *lp = lfirst_node(PropGraphLabelAndProperties, lc);
			Oid			labeloid;

			if (lp->label)
				labeloid = insert_label_record(peoid, lp->label);
			else
				labeloid = insert_label_record(peoid, einfo->aliasname);
			insert_property_records(graphid, labeloid, einfo->relid, lp->properties);
		}
	}
	else
	{
		Oid			labeloid;
		PropGraphProperties *pr = makeNode(PropGraphProperties);

		pr->all = true;
		pr->location = -1;

		labeloid = insert_label_record(peoid, einfo->aliasname);
		insert_property_records(graphid, labeloid, einfo->relid, pr);
	}
}

static Oid
insert_label_record(Oid peoid, const char *label)
{
	Relation	rel;
	NameData	labelname;
	Oid			labeloid;
	Datum		values[Natts_pg_propgraph_label] = {0};
	bool		nulls[Natts_pg_propgraph_label] = {0};
	HeapTuple	tup;
	ObjectAddress myself;
	ObjectAddress referenced;

	rel = table_open(PropgraphLabelRelationId, RowExclusiveLock);

	labeloid = GetNewOidWithIndex(rel, PropgraphLabelObjectIndexId, Anum_pg_propgraph_label_oid);
	values[Anum_pg_propgraph_label_oid - 1] = ObjectIdGetDatum(labeloid);
	namestrcpy(&labelname, label);
	values[Anum_pg_propgraph_label_pgllabel - 1] = NameGetDatum(&labelname);
	values[Anum_pg_propgraph_label_pglelid - 1] = ObjectIdGetDatum(peoid);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PropgraphLabelRelationId, labeloid);

	/* Add dependency on the property graph element */
	ObjectAddressSet(referenced, PropgraphElementRelationId, peoid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	table_close(rel, NoLock);

	return labeloid;
}

static void
insert_property_records(Oid graphid, Oid labeloid, Oid pgerelid, PropGraphProperties *properties)
{
	List	   *proplist = NIL;
	ParseState *pstate;
	ParseNamespaceItem *nsitem;
	List *tp;
	Relation rel;
	ListCell *lc;

	if (properties->all)
	{
		Relation	attRelation;
		SysScanDesc scan;
		ScanKeyData key[1];
		HeapTuple	attributeTuple;

		attRelation = table_open(AttributeRelationId, RowShareLock);
		ScanKeyInit(&key[0],
					Anum_pg_attribute_attrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(pgerelid));
		scan = systable_beginscan(attRelation, AttributeRelidNumIndexId,
								  true, NULL, 1, key);
		while (HeapTupleIsValid(attributeTuple = systable_getnext(scan)))
		{
			Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attributeTuple);
			ColumnRef  *cr;
			ResTarget *rt;

			if (att->attnum <= 0 || att->attisdropped)
				continue;

			cr = makeNode(ColumnRef);
			rt = makeNode(ResTarget);

			cr->fields = list_make1(makeString(NameStr(att->attname)));
			cr->location = -1;

			rt->name = pstrdup(NameStr(att->attname));
			rt->val = (Node *) cr;
			rt->location = -1;

			proplist = lappend(proplist, rt);
		}
		systable_endscan(scan);
		table_close(attRelation, RowShareLock);
	}
	else
	{
		proplist = properties->properties;

		foreach(lc, proplist)
		{
			ResTarget *rt = lfirst_node(ResTarget, lc);

			if (!rt->name && !IsA(rt->val, ColumnRef))
				ereport(ERROR,
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("property name required"),
						parser_errposition(NULL, rt->location));
		}
	}

	rel = table_open(pgerelid, AccessShareLock);

	pstate = make_parsestate(NULL);
	nsitem = addRangeTableEntryForRelation(pstate,
										   rel,
										   AccessShareLock,
										   NULL,
										   false,
										   true);
	addNSItemToQuery(pstate, nsitem, true, true, true);

	table_close(rel, NoLock);

	tp = transformTargetList(pstate, proplist, EXPR_KIND_OTHER);

	foreach(lc, tp)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		insert_property_record(graphid, labeloid, te->resname, te->expr);
	}
}

static void
insert_property_record(Oid graphid, Oid labeloid, const char *propname, Expr *expr)
{
	Relation	rel;
	NameData	propnamedata;
	Oid			propoid;
	Datum		values[Natts_pg_propgraph_property] = {0};
	bool		nulls[Natts_pg_propgraph_property] = {0};
	HeapTuple	tup;
	ObjectAddress myself;
	ObjectAddress referenced;

	rel = table_open(PropgraphPropertyRelationId, RowExclusiveLock);

	propoid = GetNewOidWithIndex(rel, PropgraphPropertyObjectIndexId, Anum_pg_propgraph_property_oid);
	values[Anum_pg_propgraph_property_oid - 1] = ObjectIdGetDatum(propoid);
	values[Anum_pg_propgraph_property_pgppgid - 1] = ObjectIdGetDatum(graphid);
	namestrcpy(&propnamedata, propname);
	values[Anum_pg_propgraph_property_pgpname - 1] = NameGetDatum(&propnamedata);
	values[Anum_pg_propgraph_property_pgptypid - 1] = ObjectIdGetDatum(exprType((Node *) expr));
	values[Anum_pg_propgraph_property_pgplabelid - 1] = ObjectIdGetDatum(labeloid);
	values[Anum_pg_propgraph_property_pgpexpr - 1] = CStringGetTextDatum(nodeToString(expr));

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PropgraphPropertyRelationId, propoid);

	/* Add dependency on the property graph label */
	ObjectAddressSet(referenced, PropgraphLabelRelationId, labeloid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	table_close(rel, NoLock);
}

ObjectAddress
AlterPropGraph(ParseState *pstate, AlterPropGraphStmt *stmt)
{
	Oid			pgrelid;
	ListCell   *lc;
	ObjectAddress pgaddress;

	pgrelid = RangeVarGetRelidExtended(stmt->pgname,
									   ShareRowExclusiveLock,
									   stmt->missing_ok ? RVR_MISSING_OK : 0,
									   RangeVarCallbackOwnsRelation,
									   NULL);
	if (pgrelid == InvalidOid)
	{
		ereport(NOTICE,
				(errmsg("relation \"%s\" does not exist, skipping",
						stmt->pgname->relname)));
		return InvalidObjectAddress;
	}

	ObjectAddressSet(pgaddress, RelationRelationId, pgrelid);

	foreach (lc, stmt->add_vertex_tables)
	{
		PropGraphVertex *vertex = lfirst_node(PropGraphVertex, lc);
		struct element_info *vinfo;
		Relation	rel;

		vinfo = palloc0_object(struct element_info);
		vinfo->kind = PGEKIND_VERTEX;

		vinfo->relid = RangeVarGetRelidExtended(vertex->vtable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

		rel = table_open(vinfo->relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			elog(ERROR, "TODO");

		if (vertex->vtable->alias)
			vinfo->aliasname = vertex->vtable->alias->aliasname;
		else
			vinfo->aliasname = vertex->vtable->relname;

		vinfo->key = propgraph_element_get_key(pstate, vertex->vkey, vertex->location, rel);

		vinfo->labels = vertex->labels;

		table_close(rel, NoLock);

		insert_element_record(pgaddress, vinfo);
	}

	CommandCounterIncrement();

	foreach (lc, stmt->add_edge_tables)
	{
		PropGraphEdge *edge = lfirst_node(PropGraphEdge, lc);
		struct element_info *einfo;
		Relation	rel;
		Oid			srcrelid;
		Oid			destrelid;
		Relation	srcrel;
		Relation	destrel;

		einfo = palloc0_object(struct element_info);
		einfo->kind = PGEKIND_EDGE;

		einfo->relid = RangeVarGetRelidExtended(edge->etable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

		rel = table_open(einfo->relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
			elog(ERROR, "TODO");

		if (edge->etable->alias)
			einfo->aliasname = edge->etable->alias->aliasname;
		else
			einfo->aliasname = edge->etable->relname;

		einfo->key = propgraph_element_get_key(pstate, edge->ekey, edge->location, rel);

		einfo->srcvertexid = get_vertex_oid(pstate, pgrelid, edge->esrcvertex, edge->location);
		einfo->destvertexid = get_vertex_oid(pstate, pgrelid, edge->edestvertex, edge->location);

		if (!edge->esrckey || !edge->esrcvertexcols || !edge->edestkey || !edge->edestvertexcols)
			elog(ERROR, "TODO foreign key support");

		srcrelid = get_element_relid(einfo->srcvertexid);
		destrelid = get_element_relid(einfo->destvertexid);

		srcrel = table_open(srcrelid, AccessShareLock);
		destrel = table_open(destrelid, AccessShareLock);

		if (list_length(edge->esrckey) != list_length(edge->esrcvertexcols))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("mismatching number of columns in source vertex definition of edge \"%s\"",
							einfo->aliasname),
					 parser_errposition(pstate, edge->location)));

		if (list_length(edge->edestkey) != list_length(edge->edestvertexcols))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("mismatching number of columns in destination vertex definition of edge \"%s\"",
							einfo->aliasname),
					 parser_errposition(pstate, edge->location)));

		einfo->srckey = int2vector_from_column_list(pstate, edge->esrckey, edge->location, rel);
		einfo->srcref = int2vector_from_column_list(pstate, edge->esrcvertexcols, edge->location, srcrel);
		einfo->destkey = int2vector_from_column_list(pstate, edge->edestkey, edge->location, rel);
		einfo->destref = int2vector_from_column_list(pstate, edge->edestvertexcols, edge->location, destrel);

		// TODO: various consistency checks

		einfo->labels = edge->labels;

		table_close(destrel, NoLock);
		table_close(srcrel, NoLock);

		table_close(rel, NoLock);

		insert_element_record(pgaddress, einfo);
	}

	foreach (lc, stmt->drop_vertex_tables)
	{
		char	   *alias = strVal(lfirst(lc));
		Oid			peoid;
		ObjectAddress obj;

		peoid = get_vertex_oid(pstate, pgrelid, alias, -1);
		ObjectAddressSet(obj, PropgraphElementRelationId, peoid);
		performDeletion(&obj, stmt->drop_behavior, 0);
	}

	foreach (lc, stmt->drop_edge_tables)
	{
		char	   *alias = strVal(lfirst(lc));
		Oid			peoid;
		ObjectAddress obj;

		peoid = get_edge_oid(pstate, pgrelid, alias, -1);
		ObjectAddressSet(obj, PropgraphElementRelationId, peoid);
		performDeletion(&obj, stmt->drop_behavior, 0);
	}

	foreach (lc, stmt->add_labels)
	{
		PropGraphLabelAndProperties *lp = lfirst_node(PropGraphLabelAndProperties, lc);
		Oid			peoid;
		Oid			pgerelid;
		Oid			labeloid;

		Assert(lp->label);

		if (stmt->element_kind == PROPGRAPH_ELEMENT_KIND_VERTEX)
			peoid = get_vertex_oid(pstate, pgrelid, stmt->element_alias, -1);
		else
			peoid = get_edge_oid(pstate, pgrelid, stmt->element_alias, -1);

		pgerelid = get_element_relid(peoid);

		labeloid = insert_label_record(peoid, lp->label);
		insert_property_records(pgrelid, labeloid, pgerelid, lp->properties);
	}

	if (stmt->drop_label)
	{
		Oid			peoid;
		Oid			labeloid;
		ObjectAddress obj;

		if (stmt->element_kind == PROPGRAPH_ELEMENT_KIND_VERTEX)
			peoid = get_vertex_oid(pstate, pgrelid, stmt->element_alias, -1);
		else
			peoid = get_edge_oid(pstate, pgrelid, stmt->element_alias, -1);

		labeloid = GetSysCacheOid2(PROPGRAPHLABELNAME,
								   Anum_pg_propgraph_label_oid,
								   ObjectIdGetDatum(peoid),
								   CStringGetDatum(stmt->drop_label));
		if (!labeloid)
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("property graph \"%s\" element \"%s\" has no label \"%s\"",
						   get_rel_name(pgrelid), stmt->element_alias, stmt->drop_label),
					parser_errposition(pstate, -1));

		ObjectAddressSet(obj, PropgraphLabelRelationId, labeloid);
		performDeletion(&obj, stmt->drop_behavior, 0);
	}

	if (stmt->add_properties)
	{
		Oid			peoid;
		Oid			pgerelid;
		Oid			labeloid;

		if (stmt->element_kind == PROPGRAPH_ELEMENT_KIND_VERTEX)
			peoid = get_vertex_oid(pstate, pgrelid, stmt->element_alias, -1);
		else
			peoid = get_edge_oid(pstate, pgrelid, stmt->element_alias, -1);

		pgerelid = get_element_relid(peoid);

		labeloid = GetSysCacheOid2(PROPGRAPHLABELNAME,
								   Anum_pg_propgraph_label_oid,
								   ObjectIdGetDatum(peoid),
								   CStringGetDatum(stmt->alter_label));
		if (!labeloid)
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("property graph \"%s\" element \"%s\" has no label \"%s\"",
						   get_rel_name(pgrelid), stmt->element_alias, stmt->alter_label),
					parser_errposition(pstate, -1));

		insert_property_records(pgrelid, labeloid, pgerelid, stmt->add_properties);
	}

	if (stmt->drop_properties)
	{
		Oid			peoid;
		Oid			labeloid;
		ObjectAddress obj;

		if (stmt->element_kind == PROPGRAPH_ELEMENT_KIND_VERTEX)
			peoid = get_vertex_oid(pstate, pgrelid, stmt->element_alias, -1);
		else
			peoid = get_edge_oid(pstate, pgrelid, stmt->element_alias, -1);

		labeloid = GetSysCacheOid2(PROPGRAPHLABELNAME,
								   Anum_pg_propgraph_label_oid,
								   ObjectIdGetDatum(peoid),
								   CStringGetDatum(stmt->alter_label));
		if (!labeloid)
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("property graph \"%s\" element \"%s\" has no label \"%s\"",
						   get_rel_name(pgrelid), stmt->element_alias, stmt->drop_label),
					parser_errposition(pstate, -1));

		foreach (lc, stmt->drop_properties)
		{
			char	   *propname = strVal(lfirst(lc));
			Oid			propoid;

			propoid = GetSysCacheOid2(PROPGRAPHPROPNAME,
									  Anum_pg_propgraph_property_oid,
									  CStringGetDatum(propname),
									  ObjectIdGetDatum(labeloid));
			if (!propoid)
				ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("property graph \"%s\" element \"%s\" label \"%s\" has no property \"%s\"",
							   get_rel_name(pgrelid), stmt->element_alias, stmt->alter_label, propname),
						parser_errposition(pstate, -1));

			ObjectAddressSet(obj, PropgraphPropertyRelationId, propoid);
			performDeletion(&obj, stmt->drop_behavior, 0);
		}
	}

	return pgaddress;
}

static Oid
get_vertex_oid(ParseState *pstate, Oid pgrelid, const char *alias, int location)
{
	HeapTuple	tuple;
	Oid			peoid;

	tuple = SearchSysCache2(PROPGRAPHELALIAS, ObjectIdGetDatum(pgrelid), CStringGetDatum(alias));
	if (!tuple)
		ereport(ERROR,
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("property graph \"%s\" has no element with alias \"%s\"",
					   get_rel_name(pgrelid), alias),
				parser_errposition(pstate, location));

	if (((Form_pg_propgraph_element) GETSTRUCT(tuple))->pgekind != PGEKIND_VERTEX)
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("element \"%s\" of property graph \"%s\" is not a vertex",
					   alias, get_rel_name(pgrelid)),
				parser_errposition(pstate, location));

	peoid = ((Form_pg_propgraph_element) GETSTRUCT(tuple))->oid;

	ReleaseSysCache(tuple);

	return peoid;
}

static Oid
get_edge_oid(ParseState *pstate, Oid pgrelid, const char *alias, int location)
{
	HeapTuple	tuple;
	Oid			peoid;

	tuple = SearchSysCache2(PROPGRAPHELALIAS, ObjectIdGetDatum(pgrelid), CStringGetDatum(alias));
	if (!tuple)
		ereport(ERROR,
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("property graph \"%s\" has no element with alias \"%s\"",
					   get_rel_name(pgrelid), alias),
				parser_errposition(pstate, location));

	if (((Form_pg_propgraph_element) GETSTRUCT(tuple))->pgekind != PGEKIND_EDGE)
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("element \"%s\" of property graph \"%s\" is not an edge",
					   alias, get_rel_name(pgrelid)),
				parser_errposition(pstate, location));

	peoid = ((Form_pg_propgraph_element) GETSTRUCT(tuple))->oid;

	ReleaseSysCache(tuple);

	return peoid;
}

static Oid
get_element_relid(Oid peid)
{
	HeapTuple	tuple;
	Oid			pgerelid;

	tuple = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(peid));
	if (!tuple)
		elog(ERROR, "cache lookup failed for property graph element %u", peid);

	pgerelid = ((Form_pg_propgraph_element) GETSTRUCT(tuple))->pgerelid;

	ReleaseSysCache(tuple);

	return pgerelid;
}
