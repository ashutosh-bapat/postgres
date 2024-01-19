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

#include "access/genam.h"
#include "access/table.h"
#include "catalog/pg_propgraph_element.h"
#include "catalog/pg_propgraph_label.h"
#include "catalog/pg_propgraph_property.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteGraphTable.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "utils/syscache.h"

/*  XXX */
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "nodes/print.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"


static Oid get_labelid(Oid graphid, const char *labelname);
static List *get_elements_for_label(Oid graphid, const char *labelname);
static Oid get_table_for_element(Oid elid);
static Node *replace_property_refs(Node *node, Oid labelid, int rt_index);


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

	rte = rt_fetch(rt_index, parsetree->rtable);

	newsubquery = makeNode(Query);
	newsubquery->commandType = CMD_SELECT;

	if (list_length(rte->graph_pattern->path_pattern_list) != 1)
		elog(ERROR, "unsupported path pattern list length");

	element_patterns = linitial(rte->graph_pattern->path_pattern_list);

	foreach(lc, element_patterns)
	{
		ElementPattern *ep = lfirst_node(ElementPattern, lc);
		Oid		labelid = InvalidOid;

		if (!(ep->kind == VERTEX_PATTERN || ep->kind == EDGE_PATTERN_LEFT || ep->kind == EDGE_PATTERN_RIGHT))
			elog(ERROR, "unsupported element pattern kind: %u", ep->kind);

		if (ep->quantifier)
			elog(ERROR, "element pattern quantifier not supported yet");

		if (IsA(ep->labelexpr, GraphLabelRef))
		{
			GraphLabelRef *glr = castNode(GraphLabelRef, ep->labelexpr);
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
			elog(ERROR, "unsupported label expression type: %d", (int) nodeTag(ep->labelexpr));

		if (ep->whereClause)
		{
			Node *tr;

			tr = replace_property_refs(ep->whereClause, labelid, list_length(newsubquery->rtable));

			qual_exprs = lappend(qual_exprs, tr);
		}
	}

	/* Iterate over edges only */
	for (int k = 1; k < list_length(element_ids); k+=2)
	{
		Oid			elid = list_nth_oid(element_ids, k);
		HeapTuple	tuple;
		Form_pg_propgraph_element pgeform;

		tuple = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(elid));
		if (!tuple)
			elog(ERROR, "cache lookup failed for property graph element %u", elid);
		pgeform = ((Form_pg_propgraph_element) GETSTRUCT(tuple));

		/*
		 * source link
		 */
		if (pgeform->pgesrcvertexid != list_nth_oid(element_ids, k - 1))
		{
			qual_exprs = lappend(qual_exprs, makeBoolConst(false, false));
		}
		else
		{
			Datum		datum;
			Datum	   *d1, *d2;
			int			n1, n2;

			datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, tuple, Anum_pg_propgraph_element_pgesrckey);
			deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d1, NULL, &n1);

			datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, tuple, Anum_pg_propgraph_element_pgesrcref);
			deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d2, NULL, &n2);

			if (n1 != n2)
				elog(ERROR, "array size pgesrckey vs pgesrcref mismatch for element ID %u", elid);

			for (int i = 0; i < n1; i++)
			{
				int rti = k + 1;
				OpExpr	   *op;

				op = makeNode(OpExpr);
				op->location = -1;
				op->opno = Int4EqualOperator;
				op->opfuncid = F_INT4EQ;
				op->opresulttype = BOOLOID;
				op->args = list_make2(makeVar(rti, DatumGetInt16(d1[i]), INT4OID, -1, 0, 0), makeVar(rti - 1, DatumGetInt16(d2[i]), INT4OID, -1, 0, 0));
				qual_exprs = lappend(qual_exprs, op);
			}
		}

		/*
		 * dest link
		 */
		if (pgeform->pgedestvertexid != list_nth_oid(element_ids, k + 1))
		{
			qual_exprs = lappend(qual_exprs, makeBoolConst(false, false));
		}
		else
		{
			Datum		datum;
			Datum	   *d1, *d2;
			int			n1, n2;

			datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, tuple, Anum_pg_propgraph_element_pgedestkey);
			deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d1, NULL, &n1);

			datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, tuple, Anum_pg_propgraph_element_pgedestref);
			deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d2, NULL, &n2);

			if (n1 != n2)
				elog(ERROR, "array size pgedestkey vs pgedestref mismatch for element ID %u", elid);

			for (int i = 0; i < n1; i++)
			{
				int rti = k + 1;
				OpExpr	   *op;

				op = makeNode(OpExpr);
				op->location = -1;
				op->opno = Int4EqualOperator;
				op->opfuncid = F_INT4EQ;
				op->opresulttype = BOOLOID;
				op->args = list_make2(makeVar(rti, DatumGetInt16(d1[i]), INT4OID, -1, 0, 0), makeVar(rti + 1, DatumGetInt16(d2[i]), INT4OID, -1, 0, 0));
				qual_exprs = lappend(qual_exprs, op);
			}
		}

		ReleaseSysCache(tuple);
	}

	newsubquery->jointree = makeFromExpr(fromlist, (Node *) makeBoolExpr(AND_EXPR, qual_exprs, -1));

	newsubquery->targetList = list_make1(makeTargetEntry((Expr *) makeVar(1, 2, VARCHAROID, -1, DEFAULT_COLLATION_OID, 0), 1, "customer_name", false));

	AcquireRewriteLocks(newsubquery, true, false);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = newsubquery;

	return parsetree;
}

static Oid
get_labelid(Oid graphid, const char *labelname)
{
	Relation	rel;
	SysScanDesc	scan;
	ScanKeyData	key[1];
	HeapTuple	tup;
	Oid			result = InvalidOid;

	rel = table_open(PropgraphLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pgllabel,
				BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(labelname));

	// FIXME: needs index
	// XXX: maybe pg_propgraph_label should include the graph OID
	scan = systable_beginscan(rel, InvalidOid,
							  true, NULL, 1, key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		result = ((Form_pg_propgraph_label) GETSTRUCT(tup))->oid;
		break;
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return result;
}

static List *
get_elements_for_label(Oid graphid, const char *labelname)
{
	Relation	rel;
	SysScanDesc	scan;
	ScanKeyData	key[1];
	HeapTuple	tup;
	List	   *result = NIL;

	rel = table_open(PropgraphLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pgllabel,
				BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(labelname));

	// FIXME: needs index
	// XXX: maybe pg_propgraph_label should include the graph OID
	scan = systable_beginscan(rel, InvalidOid,
							  true, NULL, 1, key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Oid elid;
		Oid pgepgid;

		elid = ((Form_pg_propgraph_label) GETSTRUCT(tup))->pglelid;
		pgepgid = GetSysCacheOid1(PROPGRAPHELOID, Anum_pg_propgraph_element_pgepgid, ObjectIdGetDatum(elid));

		if (pgepgid == graphid)
			result = lappend_oid(result, elid);
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return result;
}

static Oid
get_table_for_element(Oid elid)
{
	return GetSysCacheOid1(PROPGRAPHELOID, Anum_pg_propgraph_element_pgerelid, ObjectIdGetDatum(elid));
}

struct replace_property_refs_context
{
	Oid labelid;
	int rt_index;
};

static Node *
replace_property_refs_mutator(Node *node, struct replace_property_refs_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, PropertyRef))
	{
		PropertyRef *pr = (PropertyRef *) node;
		HeapTuple tup;
		Node *n;

		tup = SearchSysCache2(PROPGRAPHPROPNAME, CStringGetDatum(pr->propname), ObjectIdGetDatum(context->labelid));
		if (!tup)
			elog(ERROR, "property %s/%u not found", pr->propname, context->labelid);

		n = stringToNode(TextDatumGetCString(SysCacheGetAttrNotNull(PROPGRAPHPROPNAME, tup, Anum_pg_propgraph_property_pgpexpr)));
		ChangeVarNodes(n, 1, context->rt_index, 0);

		ReleaseSysCache(tup);

		return n;
	}
	return expression_tree_mutator(node, replace_property_refs_mutator, context);
}

static Node *
replace_property_refs(Node *node, Oid labelid, int rt_index)
{
	struct replace_property_refs_context context;

	context.labelid = labelid;
	context.rt_index = rt_index;

	return expression_tree_mutator(node, replace_property_refs_mutator, &context);
}
