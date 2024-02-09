/*-------------------------------------------------------------------------
 *
 * rewriteGraphTable.c
 *		Support for rewriting GRAPH_TABLE clauses.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/rewrite/rewriteGraphTable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_propgraph_element.h"
#include "catalog/pg_propgraph_label.h"
#include "catalog/pg_propgraph_property.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteGraphTable.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static Oid get_labelid(Oid graphid, const char *labelname);
static List *get_elements_for_label(Oid graphid, const char *labelname);
static Oid get_table_for_element(Oid elid);
static Node *replace_property_refs(Node *node, const List *mappings);
static List *build_edge_vertex_link_quals(HeapTuple edgetup, int edgerti, int refrti, AttrNumber catalog_key_attnum, AttrNumber catalog_ref_attnum);

struct elvar_rt_mapping
{
	const char *elvarname;
	Oid labelid;
	int rt_index;
};


/*
 * Convert GRAPH_TABLE clause into a subquery using relational
 * operators.
 */
Query *
rewriteGraphTable(Query *parsetree, int rt_index)
{
	RangeTblEntry *rte;
	Query	   *newsubquery;
	ListCell   *lc;
	List	   *element_patterns;
	List	   *element_ids = NIL;
	List	   *fromlist = NIL;
	List	   *qual_exprs = NIL;
	List	   *elvar_rt_mappings = NIL;

	rte = rt_fetch(rt_index, parsetree->rtable);

	newsubquery = makeNode(Query);
	newsubquery->commandType = CMD_SELECT;

	if (list_length(rte->graph_pattern->path_pattern_list) != 1)
		elog(ERROR, "unsupported path pattern list length");

	element_patterns = linitial(rte->graph_pattern->path_pattern_list);

	foreach(lc, element_patterns)
	{
		GraphElementPattern *gep = lfirst_node(GraphElementPattern, lc);
		Oid		labelid = InvalidOid;
		struct elvar_rt_mapping *erm;

		if (!(gep->kind == VERTEX_PATTERN || gep->kind == EDGE_PATTERN_LEFT || gep->kind == EDGE_PATTERN_RIGHT))
			elog(ERROR, "unsupported element pattern kind: %u", gep->kind);

		if (gep->quantifier)
			elog(ERROR, "element pattern quantifier not supported yet");

		if (IsA(gep->labelexpr, GraphLabelRef))
		{
			GraphLabelRef *glr = castNode(GraphLabelRef, gep->labelexpr);
			RangeTblEntry *r;
			Oid			elid;
			Oid			relid;
			RTEPermissionInfo *rpi;
			RangeTblRef *rtr;

			r = makeNode(RangeTblEntry);
			r->rtekind = RTE_RELATION;
			elid = linitial_oid(get_elements_for_label(rte->relid, glr->labelname));
			element_ids = lappend_oid(element_ids, elid);
			relid = get_table_for_element(elid);
			labelid = get_labelid(rte->relid, glr->labelname);
			r->relid = relid;
			r->relkind = get_rel_relkind(relid);
			r->rellockmode = AccessShareLock;
			r->inh = true;
			newsubquery->rtable = lappend(newsubquery->rtable, r);

			rpi = makeNode(RTEPermissionInfo);
			rpi->relid = relid;
			rpi->checkAsUser = 0;
			rpi->requiredPerms = ACL_SELECT;
			newsubquery->rteperminfos = lappend(newsubquery->rteperminfos, rpi);

			r->perminfoindex = list_length(newsubquery->rteperminfos);

			rtr = makeNode(RangeTblRef);
			rtr->rtindex = list_length(newsubquery->rtable);
			fromlist = lappend(fromlist, rtr);
		}
		else
			elog(ERROR, "unsupported label expression type: %d", (int) nodeTag(gep->labelexpr));

		erm = palloc0_object(struct elvar_rt_mapping);

		erm->elvarname = gep->variable;
		erm->labelid = labelid;
		erm->rt_index = list_length(newsubquery->rtable);

		elvar_rt_mappings = lappend(elvar_rt_mappings, erm);

		if (gep->whereClause)
		{
			Node *tr;

			tr = replace_property_refs(gep->whereClause, list_make1(erm));

			qual_exprs = lappend(qual_exprs, tr);
		}
	}

	/* Iterate over edges only */
	for (int k = 1; k < list_length(element_ids); k+=2)
	{
		Oid			elid = list_nth_oid(element_ids, k);
		HeapTuple	tuple;
		Form_pg_propgraph_element pgeform;
		GraphElementPattern *gep = list_nth_node(GraphElementPattern, element_patterns, k);
		int			srcvertexoffset;
		int			destvertexoffset;

		tuple = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(elid));
		if (!tuple)
			elog(ERROR, "cache lookup failed for property graph element %u", elid);
		pgeform = ((Form_pg_propgraph_element) GETSTRUCT(tuple));

		if (gep->kind == EDGE_PATTERN_RIGHT)
		{
			srcvertexoffset = -1;
			destvertexoffset = +1;
		}
		else if (gep->kind == EDGE_PATTERN_LEFT)
		{
			srcvertexoffset = +1;
			destvertexoffset = -1;
		}
		else
			Assert(false);

		/*
		 * source link
		 */
		if (pgeform->pgesrcvertexid != list_nth_oid(element_ids, k + srcvertexoffset))
		{
			qual_exprs = lappend(qual_exprs, makeBoolConst(false, false));
		}
		else
		{
			qual_exprs = list_concat(qual_exprs,
									 build_edge_vertex_link_quals(tuple, k + 1, k + 1 + srcvertexoffset,
																  Anum_pg_propgraph_element_pgesrckey, Anum_pg_propgraph_element_pgesrcref));
		}

		/*
		 * dest link
		 */
		if (pgeform->pgedestvertexid != list_nth_oid(element_ids, k + destvertexoffset))
		{
			qual_exprs = lappend(qual_exprs, makeBoolConst(false, false));
		}
		else
		{
			qual_exprs = list_concat(qual_exprs,
									 build_edge_vertex_link_quals(tuple, k + 1, k + 1 + destvertexoffset,
																  Anum_pg_propgraph_element_pgedestkey, Anum_pg_propgraph_element_pgedestref));
		}

		ReleaseSysCache(tuple);
	}

	newsubquery->jointree = makeFromExpr(fromlist, (Node *) makeBoolExpr(AND_EXPR, qual_exprs, -1));

	foreach(lc, rte->graph_table_columns)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Node *nte;

		nte = replace_property_refs((Node *) te, elvar_rt_mappings);
		newsubquery->targetList = lappend(newsubquery->targetList, nte);
	}

	AcquireRewriteLocks(newsubquery, true, false);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = newsubquery;

	return parsetree;
}

/*
 * Get label OID from graph OID and label name.
 *
 * TODO: This could match more than one entry.  Right now it only returns the
 * first one.
 */
static Oid
get_labelid(Oid graphid, const char *labelname)
{
	Relation	rel;
	SysScanDesc	scan;
	ScanKeyData	key[2];
	HeapTuple	tup;
	Oid			result = InvalidOid;

	rel = table_open(PropgraphLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pglpgid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(graphid));
	ScanKeyInit(&key[1],
				Anum_pg_propgraph_label_pgllabel,
				BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(labelname));

	scan = systable_beginscan(rel, PropgraphLabelGraphNameIndexId,
							  true, NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		result = ((Form_pg_propgraph_label) GETSTRUCT(tup))->oid;
		break;
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return result;
}

/*
 * Get list of element OIDs that have a given label.
 */
static List *
get_elements_for_label(Oid graphid, const char *labelname)
{
	Relation	rel;
	SysScanDesc	scan;
	ScanKeyData	key[2];
	HeapTuple	tup;
	List	   *result = NIL;

	rel = table_open(PropgraphLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pglpgid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(graphid));
	ScanKeyInit(&key[1],
				Anum_pg_propgraph_label_pgllabel,
				BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(labelname));

	scan = systable_beginscan(rel, PropgraphLabelGraphNameIndexId,
							  true, NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		result = lappend_oid(result, ((Form_pg_propgraph_label) GETSTRUCT(tup))->pglelid);
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return result;
}

/*
 * Get the element table OID for a given element.
 */
static Oid
get_table_for_element(Oid elid)
{
	return GetSysCacheOid1(PROPGRAPHELOID, Anum_pg_propgraph_element_pgerelid, ObjectIdGetDatum(elid));
}

/*
 * Mutating property references into table variables
 */

struct replace_property_refs_context
{
	const List *mappings;
};

static Node *
replace_property_refs_mutator(Node *node, struct replace_property_refs_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, GraphPropertyRef))
	{
		GraphPropertyRef *gpr = (GraphPropertyRef *) node;
		HeapTuple tup;
		Node *n;
		ListCell *lc;
		struct elvar_rt_mapping *found_mapping = NULL;

		foreach(lc, context->mappings)
		{
			struct elvar_rt_mapping *m = lfirst(lc);

			if (m->elvarname && strcmp(gpr->elvarname, m->elvarname) == 0)
			{
				found_mapping = m;
				break;
			}
		}
		if (!found_mapping)
			elog(ERROR, "undefined element variable \"%s\"", gpr->elvarname);

		tup = SearchSysCache2(PROPGRAPHPROPNAME, ObjectIdGetDatum(found_mapping->labelid), CStringGetDatum(gpr->propname));
		if (!tup)
			elog(ERROR, "property \"%s\" of label %u not found", gpr->propname, found_mapping->labelid);

		n = stringToNode(TextDatumGetCString(SysCacheGetAttrNotNull(PROPGRAPHPROPNAME, tup, Anum_pg_propgraph_property_pgpexpr)));
		ChangeVarNodes(n, 1, found_mapping->rt_index, 0);

		ReleaseSysCache(tup);

		return n;
	}
	return expression_tree_mutator(node, replace_property_refs_mutator, context);
}

static Node *
replace_property_refs(Node *node, const List *mappings)
{
	struct replace_property_refs_context context;

	context.mappings = mappings;

	return expression_tree_mutator(node, replace_property_refs_mutator, &context);
}

/*
 * Build join qualification expressions between edge and vertex tables.
 */
static List *
build_edge_vertex_link_quals(HeapTuple edgetup, int edgerti, int refrti, AttrNumber catalog_key_attnum, AttrNumber catalog_ref_attnum)
{
	List	   *quals = NIL;
	Form_pg_propgraph_element pgeform;
	Datum		datum;
	Datum	   *d1, *d2;
	int			n1, n2;

	pgeform = (Form_pg_propgraph_element) GETSTRUCT(edgetup);

	datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, edgetup, catalog_key_attnum);
	deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d1, NULL, &n1);

	datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, edgetup, catalog_ref_attnum);
	deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d2, NULL, &n2);

	if (n1 != n2)
		elog(ERROR, "array size key (%d) vs ref (%d) mismatch for element ID %u", catalog_key_attnum, catalog_ref_attnum, pgeform->oid);

	for (int i = 0; i < n1; i++)
	{
		AttrNumber	keyattn = DatumGetInt16(d1[i]);
		AttrNumber	refattn = DatumGetInt16(d2[i]);
		Oid			atttypid;
		TypeCacheEntry *typentry;
		OpExpr	   *op;

		/*
		 * TODO: Assumes types the same on both sides; no collations yet.
		 * Some of this could probably be shared with foreign key triggers.
		 */
		atttypid = get_atttype(pgeform->pgerelid, keyattn);
		typentry = lookup_type_cache(atttypid, TYPECACHE_EQ_OPR);

		op = makeNode(OpExpr);
		op->location = -1;
		op->opno = typentry->eq_opr;
		op->opresulttype = BOOLOID;
		op->args = list_make2(makeVar(edgerti, keyattn, atttypid, -1, 0, 0),
							  makeVar(refrti, refattn, atttypid, -1, 0, 0));
		quals = lappend(quals, op);
	}

	return quals;
}
