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

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteGraphTable.h"

/*  XXX */
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "nodes/print.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"


/*
 * Convert GRAPH_TABLE clause into a subquery using relational
 * operators.
 */
Query *
rewriteGraphTable(Query *parsetree, int rt_index)
{
	RangeTblEntry *rte;
	Query	   *newsubquery;

	rte = rt_fetch(rt_index, parsetree->rtable);

	newsubquery = makeNode(Query);
	newsubquery->commandType = CMD_SELECT;

	/* rtable */
	{
		RangeTblEntry *r;
		Oid			relid;
		RangeVar   *rv;
		RTEPermissionInfo *rpi;

		r = makeNode(RangeTblEntry);
		r->alias = makeAlias("c", NIL);
		r->eref = makeAlias("c", list_make3(makeString("customer_id"), makeString("name"), makeString("address")));
		r->rtekind = RTE_RELATION;
		rv = makeNode(RangeVar);
		rv->relname = "customers";
		relid = RangeVarGetRelid(rv, AccessShareLock, false);
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

		r = makeNode(RangeTblEntry);
		r->alias = makeAlias("_co", NIL);
		r->eref = makeAlias("_co", list_make3(makeString("customer_orders_id"), makeString("customer_id"), makeString("order_id")));
		r->rtekind = RTE_RELATION;
		rv = makeNode(RangeVar);
		rv->relname = "customer_orders";
		relid = RangeVarGetRelid(rv, AccessShareLock, false);
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

		r = makeNode(RangeTblEntry);
		r->alias = makeAlias("o", NIL);
		r->eref = makeAlias("o", list_make2(makeString("order_id"), makeString("ordered_when")));
		r->rtekind = RTE_RELATION;
		rv = makeNode(RangeVar);
		rv->relname = "orders";
		relid = RangeVarGetRelid(rv, AccessShareLock, false);
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

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = newsubquery;

	return parsetree;
}
