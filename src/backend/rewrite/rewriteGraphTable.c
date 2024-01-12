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
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteGraphTable.h"
#include "rewrite/rewriteHandler.h"
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


static List *get_elements_for_label(Oid graphid, const char *labelname);
static Oid get_table_for_element(Oid elid);


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

	rte = rt_fetch(rt_index, parsetree->rtable);

	newsubquery = makeNode(Query);
	newsubquery->commandType = CMD_SELECT;

	if (list_length(rte->graph_pattern->path_pattern_list) != 1)
		elog(ERROR, "unsupported path pattern list length");

	element_patterns = linitial(rte->graph_pattern->path_pattern_list);

	/* rtable */
	foreach(lc, element_patterns)
	{
		ElementPattern *ep = lfirst_node(ElementPattern, lc);

		if (IsA(ep->labelexpr, GraphLabelRef))
		{
			GraphLabelRef *glr = castNode(GraphLabelRef, ep->labelexpr);
			RangeTblEntry *r;
			Oid			relid;
			RTEPermissionInfo *rpi;

			r = makeNode(RangeTblEntry);
			r->rtekind = RTE_RELATION;
			relid = get_table_for_element(linitial_oid(get_elements_for_label(rte->relid, glr->labelname)));
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
		}
		else
			elog(ERROR, "unsupported label expression type: %d", (int) nodeTag(ep->labelexpr));
	}

	/* jointree */
	{
		List	   *fromlist = NIL;
		Node	   *quals;
		RangeTblRef *rtr;

		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 1;
		fromlist = lappend(fromlist, rtr);

		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 2;
		fromlist = lappend(fromlist, rtr);

		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 3;
		fromlist = lappend(fromlist, rtr);

		{
			OpExpr	   *op1,
					   *op2,
					   *op3;

			op1 = makeNode(OpExpr);
			op1->location = -1;
			op1->opno = Int4EqualOperator;
			op1->opfuncid = F_INT4EQ;
			op1->opresulttype = BOOLOID;
			op1->args = list_make2(makeVar(2, 2, INT4OID, -1, 0, 0), makeVar(1, 1, INT4OID, -1, 0, 0));

			op2 = makeNode(OpExpr);
			op2->location = -1;
			op2->opno = Int4EqualOperator;
			op2->opfuncid = F_INT4EQ;
			op2->opresulttype = BOOLOID;
			op2->args = list_make2(makeVar(2, 2, INT4OID, -1, 0, 0), makeVar(3, 1, INT4OID, -1, 0, 0));

			op3 = makeNode(OpExpr);
			op3->location = -1;
			op3->opno = TextEqualOperator;
			op3->opfuncid = F_TEXTEQ;
			op3->opresulttype = BOOLOID;
			op3->inputcollid = DEFAULT_COLLATION_OID;
			op3->args = list_make2(makeRelabelType((Expr *) makeVar(1, 3, VARCHAROID, -1, DEFAULT_COLLATION_OID, 0), TEXTOID, -1, DEFAULT_COLLATION_OID, 2),
								   makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1, PointerGetDatum(cstring_to_text("US")), false, false));

			quals = (Node *) makeBoolExpr(AND_EXPR, list_make3(op1, op2, op3), -1);
		}

		newsubquery->jointree = makeFromExpr(fromlist, quals);
	}

	newsubquery->targetList = list_make1(makeTargetEntry((Expr *) makeVar(1, 2, VARCHAROID, -1, DEFAULT_COLLATION_OID, 0), 1, "customer_name", false));

	AcquireRewriteLocks(newsubquery, true, false);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = newsubquery;

	return parsetree;
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

pg_attribute_unused()
static Oid
get_table_for_element(Oid elid)
{
	return GetSysCacheOid1(PROPGRAPHELOID, Anum_pg_propgraph_element_pgerelid, ObjectIdGetDatum(elid));
}
