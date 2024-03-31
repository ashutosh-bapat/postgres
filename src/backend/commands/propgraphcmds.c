/*-------------------------------------------------------------------------
 *
 * propgraphcmds.c
 *	  property graph manipulation
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
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
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


struct element_info
{
	Oid			elementid;
	char		kind;
	Oid			relid;
	char	   *aliasname;
	ArrayType  *key;

	char	   *srcvertex;
	Oid			srcvertexid;
	Oid			srcrelid;
	ArrayType  *srckey;
	ArrayType  *srcref;

	char	   *destvertex;
	Oid			destvertexid;
	Oid			destrelid;
	ArrayType  *destkey;
	ArrayType  *destref;

	List	   *labels;
};


static ArrayType *propgraph_element_get_key(ParseState *pstate, const List *keycols, Relation element_rel,
											const char *aliasname, int location);
static void propgraph_edge_get_ref_keys(ParseState *pstate, const List *keycols, const List *refcols,
										Relation edge_rel, Relation ref_rel,
										const char *aliasname, int location, const char *type,
										ArrayType **outkey, ArrayType **outref);
static ArrayType *array_from_column_list(ParseState *pstate, const List *colnames, int location, Relation element_rel);
static ArrayType *array_from_attnums(int numattrs, const AttrNumber *attnums);
static void insert_element_record(ObjectAddress pgaddress, struct element_info *einfo);
static Oid	insert_label_record(Oid graphid, Oid peoid, const char *label);
static void insert_property_records(Oid graphid, Oid labeloid, Oid pgerelid, const PropGraphProperties *properties);
static void insert_property_record(Oid graphid, Oid labeloid, Oid pgerelid, const char *propname, const Expr *expr);
static void check_propgraph_properties(Oid pgrelid);
static Oid	get_vertex_oid(ParseState *pstate, Oid pgrelid, const char *alias, int location);
static Oid	get_edge_oid(ParseState *pstate, Oid pgrelid, const char *alias, int location);
static Oid	get_element_relid(Oid peid);


/*
 * CREATE PROPERTY GRAPH
 */
ObjectAddress
CreatePropGraph(ParseState *pstate, const CreatePropGraphStmt *stmt)
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

	foreach(lc, stmt->vertex_tables)
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

		vinfo->key = propgraph_element_get_key(pstate, vertex->vkey, rel, vinfo->aliasname, vertex->location);

		vinfo->labels = vertex->labels;

		table_close(rel, NoLock);

		vertex_infos = lappend(vertex_infos, vinfo);

		element_aliases = lappend(element_aliases, makeString(vinfo->aliasname));
	}

	foreach(lc, stmt->edge_tables)
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

		einfo->key = propgraph_element_get_key(pstate, edge->ekey, rel, einfo->aliasname, edge->location);

		einfo->srcvertex = edge->esrcvertex;
		einfo->destvertex = edge->edestvertex;

		srcrelid = 0;
		destrelid = 0;
		foreach(lc2, vertex_infos)
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

		srcrel = table_open(srcrelid, NoLock);
		destrel = table_open(destrelid, NoLock);

		propgraph_edge_get_ref_keys(pstate, edge->esrckey, edge->esrcvertexcols, rel, srcrel,
									einfo->aliasname, edge->location, "SOURCE",
									&einfo->srckey, &einfo->srcref);
		propgraph_edge_get_ref_keys(pstate, edge->edestkey, edge->edestvertexcols, rel, destrel,
									einfo->aliasname, edge->location, "DESTINATION",
									&einfo->destkey, &einfo->destref);

		/* TODO: various consistency checks */

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
		ListCell   *lc2;

		/*
		 * Look up the vertices again.  Now the vertices have OIDs assigned,
		 * which we need.
		 */
		foreach(lc2, vertex_infos)
		{
			struct element_info *vinfo = lfirst(lc2);

			if (strcmp(vinfo->aliasname, einfo->srcvertex) == 0)
			{
				einfo->srcvertexid = vinfo->elementid;
				einfo->srcrelid = vinfo->relid;
			}
			if (strcmp(vinfo->aliasname, einfo->destvertex) == 0)
			{
				einfo->destvertexid = vinfo->elementid;
				einfo->destrelid = vinfo->relid;
			}
			if (einfo->srcvertexid && einfo->destvertexid)
				break;
		}
		Assert(einfo->srcvertexid);
		Assert(einfo->destvertexid);
		Assert(einfo->srcrelid);
		Assert(einfo->destrelid);
		insert_element_record(pgaddress, einfo);
	}

	CommandCounterIncrement();

	check_propgraph_properties(pgaddress.objectId);

	return pgaddress;
}

/*
 * Process the key clause specified for an element.  If key_clause is non-NIL,
 * then it is a list of column names.  Otherwise, the primary key of the
 * relation is used.  The return value is an array of column numbers.
 */
static ArrayType *
propgraph_element_get_key(ParseState *pstate, const List *key_clause, Relation element_rel, const char *aliasname, int location)
{
	ArrayType  *a;

	if (key_clause == NIL)
	{
		Oid			pkidx = RelationGetPrimaryKeyIndex(element_rel);

		if (!pkidx)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("no key specified and no suitable primary key exists for definition of element \"%s\"", aliasname),
					parser_errposition(pstate, location));
		else
		{
			Relation	indexDesc;

			indexDesc = index_open(pkidx, AccessShareLock);
			a = array_from_attnums(indexDesc->rd_index->indkey.dim1, indexDesc->rd_index->indkey.values);
			index_close(indexDesc, NoLock);
		}
	}
	else
	{
		a = array_from_column_list(pstate, key_clause, location, element_rel);
	}

	return a;
}

/*
 * Process the source or destination link of an edge.
 *
 * keycols and refcols are column names representing the local and referenced
 * (vertex) columns.  If they are both NIL, a matching foreign key is looked
 * up.
 *
 * edge_rel and ref_rel are the local and referenced element tables.
 *
 * aliasname, location, and type are for error messages.  type is either
 * "SOURCE" or "DESTINATION".
 *
 * The outputs are arrays of column numbers in outkey and outref.
 */
static void
propgraph_edge_get_ref_keys(ParseState *pstate, const List *keycols, const List *refcols,
							Relation edge_rel, Relation ref_rel,
							const char *aliasname, int location, const char *type,
							ArrayType **outkey, ArrayType **outref)
{
	Assert((keycols && refcols) || (!keycols && !refcols));

	if (keycols)
	{
		if (list_length(keycols) != list_length(refcols))
			ereport(ERROR,
					errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("mismatching number of columns in %s vertex definition of edge \"%s\"", type, aliasname),
					parser_errposition(pstate, location));

		*outkey = array_from_column_list(pstate, keycols, location, edge_rel);
		*outref = array_from_column_list(pstate, refcols, location, ref_rel);
	}
	else
	{
		List	   *fkeys;
		ListCell   *lc;
		int			count = 0;
		ForeignKeyCacheInfo *fk = NULL;

		fkeys = RelationGetFKeyList(edge_rel);
		foreach(lc, fkeys)
		{
			fk = lfirst_node(ForeignKeyCacheInfo, lc);

			if (fk->confrelid == RelationGetRelid(ref_rel))
				count++;
		}

		if (count == 0)
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("no %s key specified and no suitable foreign key exists for definition of edge \"%s\"", type, aliasname),
					parser_errposition(pstate, location));
		else if (count > 1)
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("more than one suitable foreign key exists for %s key of edge \"%s\"", type, aliasname),
					parser_errposition(pstate, location));

		Assert(fk);

		*outkey = array_from_attnums(fk->nkeys, fk->conkey);
		*outref = array_from_attnums(fk->nkeys, fk->confkey);
	}
}

/*
 * Convert list of column names in the specified relation into an array of
 * column numbers.
 */
static ArrayType *
array_from_column_list(ParseState *pstate, const List *colnames, int location, Relation element_rel)
{
	int			numattrs;
	Datum	   *attnumsd;
	int			i;
	ListCell   *lc;

	numattrs = list_length(colnames);
	attnumsd = palloc_array(Datum, numattrs);

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
		attnumsd[i++] = Int16GetDatum(attnum);
	}

	for (int j = 0; j < numattrs; j++)
	{
		for (int k = j + 1; k < numattrs; k++)
		{
			if (DatumGetInt16(attnumsd[j]) == DatumGetInt16(attnumsd[k]))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("graph key columns list must not contain duplicates"),
						 parser_errposition(pstate, location)));
		}
	}

	return construct_array_builtin(attnumsd, numattrs, INT2OID);
}

static ArrayType *
array_from_attnums(int numattrs, const AttrNumber *attnums)
{
	Datum	   *attnumsd;

	attnumsd = palloc_array(Datum, numattrs);

	for (int i = 0; i < numattrs; i++)
		attnumsd[i] = Int16GetDatum(attnums[i]);

	return construct_array_builtin(attnumsd, numattrs, INT2OID);
}

static void
array_of_attnums_to_objectaddrs(Oid relid, ArrayType *arr, ObjectAddresses *addrs)
{
	Datum	   *attnumsd;
	int			numattrs;

	deconstruct_array_builtin(arr, INT2OID, &attnumsd, NULL, &numattrs);

	for (int i = 0; i < numattrs; i++)
	{
		ObjectAddress referenced;

		ObjectAddressSubSet(referenced, RelationRelationId, relid, DatumGetInt16(attnumsd[i]));
		add_exact_object_address(&referenced, addrs);
	}
}

/*
 * Insert a record for an element into the pg_propgraph_element catalog.  Also
 * inserts labels and properties into their respective catalogs.
 */
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
	ObjectAddresses *addrs;

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

	addrs = new_object_addresses();

	/* Add dependency on the relation */
	ObjectAddressSet(referenced, RelationRelationId, einfo->relid);
	add_exact_object_address(&referenced, addrs);
	array_of_attnums_to_objectaddrs(einfo->relid, einfo->key, addrs);

	/* Add dependencies on vertices */
	if (einfo->srcvertexid)
	{
		ObjectAddressSet(referenced, PropgraphElementRelationId, einfo->srcvertexid);
		add_exact_object_address(&referenced, addrs);
		array_of_attnums_to_objectaddrs(einfo->relid, einfo->srckey, addrs);
		array_of_attnums_to_objectaddrs(einfo->srcrelid, einfo->srcref, addrs);
	}
	if (einfo->destvertexid)
	{
		ObjectAddressSet(referenced, PropgraphElementRelationId, einfo->destvertexid);
		add_exact_object_address(&referenced, addrs);
		array_of_attnums_to_objectaddrs(einfo->relid, einfo->destkey, addrs);
		array_of_attnums_to_objectaddrs(einfo->destrelid, einfo->destref, addrs);
	}

	/* TODO: dependencies on equality operators, like for foreign keys */

	record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);

	table_close(rel, NoLock);

	if (einfo->labels)
	{
		ListCell   *lc;

		foreach(lc, einfo->labels)
		{
			PropGraphLabelAndProperties *lp = lfirst_node(PropGraphLabelAndProperties, lc);
			Oid			labeloid;

			if (lp->label)
				labeloid = insert_label_record(graphid, peoid, lp->label);
			else
				labeloid = insert_label_record(graphid, peoid, einfo->aliasname);
			insert_property_records(graphid, labeloid, einfo->relid, lp->properties);
		}
	}
	else
	{
		Oid			labeloid;
		PropGraphProperties *pr = makeNode(PropGraphProperties);

		pr->all = true;
		pr->location = -1;

		labeloid = insert_label_record(graphid, peoid, einfo->aliasname);
		insert_property_records(graphid, labeloid, einfo->relid, pr);
	}
}

/*
 * Insert a record for a label into the pg_propgraph_label catalog.
 */
static Oid
insert_label_record(Oid graphid, Oid peoid, const char *label)
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
	values[Anum_pg_propgraph_label_pglpgid - 1] = ObjectIdGetDatum(graphid);
	namestrcpy(&labelname, label);
	values[Anum_pg_propgraph_label_pgllabel - 1] = NameGetDatum(&labelname);
	values[Anum_pg_propgraph_label_pglelid - 1] = ObjectIdGetDatum(peoid);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PropgraphLabelRelationId, labeloid);

	ObjectAddressSet(referenced, RelationRelationId, graphid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
	ObjectAddressSet(referenced, PropgraphElementRelationId, peoid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	table_close(rel, NoLock);

	return labeloid;
}

/*
 * Insert records for properties into the pg_propgraph_property catalog.
 */
static void
insert_property_records(Oid graphid, Oid labeloid, Oid pgerelid, const PropGraphProperties *properties)
{
	List	   *proplist = NIL;
	ParseState *pstate;
	ParseNamespaceItem *nsitem;
	List	   *tp;
	Relation	rel;
	ListCell   *lc;

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
			ResTarget  *rt;

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
			ResTarget  *rt = lfirst_node(ResTarget, lc);

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

		insert_property_record(graphid, labeloid, pgerelid, te->resname, te->expr);
	}
}

/*
 * Insert one record for a property into the pg_propgraph_property catalog.
 */
static void
insert_property_record(Oid graphid, Oid labeloid, Oid pgerelid, const char *propname, const Expr *expr)
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
	values[Anum_pg_propgraph_property_pgptypid - 1] = ObjectIdGetDatum(exprType((const Node *) expr));
	values[Anum_pg_propgraph_property_pgplabelid - 1] = ObjectIdGetDatum(labeloid);
	values[Anum_pg_propgraph_property_pgpexpr - 1] = CStringGetTextDatum(nodeToString(expr));

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PropgraphPropertyRelationId, propoid);

	ObjectAddressSet(referenced, RelationRelationId, graphid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
	ObjectAddressSet(referenced, PropgraphLabelRelationId, labeloid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
	recordDependencyOnSingleRelExpr(&myself, (Node *) copyObject(expr), pgerelid, DEPENDENCY_NORMAL, DEPENDENCY_NORMAL, false);

	table_close(rel, NoLock);
}

/*
 * Check that in a graph, all properties with the same name have the same type
 * (independent of which label they are on).  (See SQL/PGQ subclause
 * "Consistency check of a tabular property graph descriptor".)
 *
 * XXX We check this after all the catalog records are already inserted.  This
 * makes it easier to share this code between CREATE PROPERTY GRAPH and ALTER
 * PROPERTY GRAPH.
 */
static void
check_propgraph_properties(Oid pgrelid)
{
	Relation	rel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	List	   *propnames = NIL;
	List	   *proptypes = NIL;

	rel = table_open(PropgraphPropertyRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_property_pgppgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pgrelid));

	scan = systable_beginscan(rel, PropgraphPropertyGraphNameIndexId, true, NULL, 1, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_propgraph_property prop = (Form_pg_propgraph_property) GETSTRUCT(tuple);
		const char *propname;
		Oid			proptype;
		ListCell   *lc1,
				   *lc2;
		bool		found;

		propname = NameStr(prop->pgpname);
		proptype = prop->pgptypid;

		found = false;
		forboth(lc1, propnames, lc2, proptypes)
		{
			if (strcmp(propname, lfirst(lc1)) == 0)
			{
				found = true;

				if (proptype != lfirst_oid(lc2))
					ereport(ERROR,
							errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("property \"%s\" data type mismatch: %s vs. %s",
								   propname, format_type_be(lfirst_oid(lc2)), format_type_be(proptype)),
							errdetail("In a property graph, a property of the same name has to have the same data type in each label."));

				break;
			}
		}

		if (!found)
		{
			propnames = lappend(propnames, pstrdup(propname));
			proptypes = lappend_oid(proptypes, proptype);
		}
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * ALTER PROPERTY GRAPH
 */
ObjectAddress
AlterPropGraph(ParseState *pstate, const AlterPropGraphStmt *stmt)
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

	foreach(lc, stmt->add_vertex_tables)
	{
		PropGraphVertex *vertex = lfirst_node(PropGraphVertex, lc);
		struct element_info *vinfo;
		Relation	rel;

		vinfo = palloc0_object(struct element_info);
		vinfo->kind = PGEKIND_VERTEX;

		vinfo->relid = RangeVarGetRelidExtended(vertex->vtable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

		rel = table_open(vinfo->relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && get_rel_persistence(pgrelid) != RELPERSISTENCE_TEMP)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cannot add temporary element table to non-temporary property graph"),
					 errdetail("Table \"%s\" is a temporary table.", get_rel_name(vinfo->relid)),
					 parser_errposition(pstate, vertex->vtable->location)));

		if (vertex->vtable->alias)
			vinfo->aliasname = vertex->vtable->alias->aliasname;
		else
			vinfo->aliasname = vertex->vtable->relname;

		vinfo->key = propgraph_element_get_key(pstate, vertex->vkey, rel, vinfo->aliasname, vertex->location);

		vinfo->labels = vertex->labels;

		table_close(rel, NoLock);

		insert_element_record(pgaddress, vinfo);
	}

	CommandCounterIncrement();

	foreach(lc, stmt->add_edge_tables)
	{
		PropGraphEdge *edge = lfirst_node(PropGraphEdge, lc);
		struct element_info *einfo;
		Relation	rel;
		Relation	srcrel;
		Relation	destrel;

		einfo = palloc0_object(struct element_info);
		einfo->kind = PGEKIND_EDGE;

		einfo->relid = RangeVarGetRelidExtended(edge->etable, AccessShareLock, 0, RangeVarCallbackOwnsRelation, NULL);

		rel = table_open(einfo->relid, NoLock);

		if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && get_rel_persistence(pgrelid) != RELPERSISTENCE_TEMP)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("cannot add temporary element table to non-temporary property graph"),
					 errdetail("Table \"%s\" is a temporary table.", get_rel_name(einfo->relid)),
					 parser_errposition(pstate, edge->etable->location)));

		if (edge->etable->alias)
			einfo->aliasname = edge->etable->alias->aliasname;
		else
			einfo->aliasname = edge->etable->relname;

		einfo->key = propgraph_element_get_key(pstate, edge->ekey, rel, einfo->aliasname, edge->location);

		einfo->srcvertexid = get_vertex_oid(pstate, pgrelid, edge->esrcvertex, edge->location);
		einfo->destvertexid = get_vertex_oid(pstate, pgrelid, edge->edestvertex, edge->location);

		einfo->srcrelid = get_element_relid(einfo->srcvertexid);
		einfo->destrelid = get_element_relid(einfo->destvertexid);

		srcrel = table_open(einfo->srcrelid, AccessShareLock);
		destrel = table_open(einfo->destrelid, AccessShareLock);

		propgraph_edge_get_ref_keys(pstate, edge->esrckey, edge->esrcvertexcols, rel, srcrel,
									einfo->aliasname, edge->location, "SOURCE",
									&einfo->srckey, &einfo->srcref);
		propgraph_edge_get_ref_keys(pstate, edge->edestkey, edge->edestvertexcols, rel, destrel,
									einfo->aliasname, edge->location, "DESTINATION",
									&einfo->destkey, &einfo->destref);

		/* TODO: various consistency checks */

		einfo->labels = edge->labels;

		table_close(destrel, NoLock);
		table_close(srcrel, NoLock);

		table_close(rel, NoLock);

		insert_element_record(pgaddress, einfo);
	}

	foreach(lc, stmt->drop_vertex_tables)
	{
		char	   *alias = strVal(lfirst(lc));
		Oid			peoid;
		ObjectAddress obj;

		peoid = get_vertex_oid(pstate, pgrelid, alias, -1);
		ObjectAddressSet(obj, PropgraphElementRelationId, peoid);
		performDeletion(&obj, stmt->drop_behavior, 0);
	}

	foreach(lc, stmt->drop_edge_tables)
	{
		char	   *alias = strVal(lfirst(lc));
		Oid			peoid;
		ObjectAddress obj;

		peoid = get_edge_oid(pstate, pgrelid, alias, -1);
		ObjectAddressSet(obj, PropgraphElementRelationId, peoid);
		performDeletion(&obj, stmt->drop_behavior, 0);
	}

	foreach(lc, stmt->add_labels)
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

		labeloid = insert_label_record(pgrelid, peoid, lp->label);
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

		foreach(lc, stmt->drop_properties)
		{
			char	   *propname = strVal(lfirst(lc));
			Oid			propoid;

			propoid = GetSysCacheOid2(PROPGRAPHPROPNAME,
									  Anum_pg_propgraph_property_oid,
									  ObjectIdGetDatum(labeloid),
									  CStringGetDatum(propname));
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

	CommandCounterIncrement();

	check_propgraph_properties(pgaddress.objectId);

	return pgaddress;
}

/*
 * Get OID of vertex from graph OID and element alias.  Element must be a
 * vertex, otherwise error.
 */
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

/*
 * Get OID of edge from graph OID and element alias.  Element must be an edge,
 * otherwise error.
 */
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

/*
 * Get the element table relation OID from the OID of the element.
 */
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
