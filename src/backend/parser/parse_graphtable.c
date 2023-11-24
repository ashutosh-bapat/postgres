/*-------------------------------------------------------------------------
 *
 * parse_graphtable.c
 *	  parsing of GRAPH_TABLE
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_graphtable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_propgraph_property.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_expr.h"
#include "parser/parse_graphtable.h"
#include "parser/parse_node.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"

static Oid
get_property_type(Oid graphid, const char *propname)
{
	Relation	rel;
	SysScanDesc	scan;
	ScanKeyData	key[2];
	HeapTuple	tuple;
	Oid			result = InvalidOid;

	rel = table_open(PropgraphPropertyRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_property_pgppgid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(graphid));
	ScanKeyInit(&key[1],
				Anum_pg_propgraph_property_pgpname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(propname));

	scan = systable_beginscan(rel, PropgraphPropertyGraphNameIndexId, true, NULL, 2, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_propgraph_property prop = (Form_pg_propgraph_property) GETSTRUCT(tuple);

		result = prop->pgptypid;
		break;
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	if (!result)
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("property \"%s\" does not exist", propname));

	return result;
}

Node *
graph_table_property_reference(ParseState *pstate, ColumnRef *cref)
{
	GraphTableParseState *gpstate = pstate->p_ref_hook_state;
	PropertyRef *pr = makeNode(PropertyRef);

	pr->location = cref->location;

	if (list_length(cref->fields) == 2)
	{
		Node	   *field1 = linitial(cref->fields);
		Node	   *field2 = lsecond(cref->fields);
		char	   *elvarname;
		char       *propname;

		elvarname = strVal(field1);
		propname = strVal(field2);

		if (!list_member(gpstate->variables, field1))
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("graph pattern variable \"%s\" does not exist", elvarname),
					parser_errposition(pstate, cref->location));

		pr->elvarname = elvarname;
		pr->propname = propname;

	}
	else if (list_length(cref->fields) == 1 &&
		gpstate->localvar)
	{
		Node	   *field = linitial(cref->fields);
		char	   *elvarname;
		char       *propname;

		elvarname = pstrdup(gpstate->localvar);
		propname = strVal(field);

		if (!list_member(gpstate->variables, makeString(elvarname)))
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("graph pattern variable \"%s\" does not exist", elvarname),
					parser_errposition(pstate, cref->location));

		pr->elvarname = elvarname;
		pr->propname = propname;
	}
	else
		elog(ERROR, "invalid property reference");

	pr->typeId = get_property_type(gpstate->graphid, pr->propname);

	return (Node *) pr;
}

static Node *
transformLabelExpr(GraphTableParseState *gpstate, Node *labelexpr)
{
	Node *result;

	if (labelexpr == NULL)
		return NULL;

	check_stack_depth();

	switch (nodeTag(labelexpr))
	{
		case T_ColumnRef:
			{
				ColumnRef *cref = (ColumnRef *) labelexpr;
				const char *labelname;
				GraphLabelRef *lref;

				Assert(list_length(cref->fields) == 1);
				labelname = strVal(linitial(cref->fields));

				lref = makeNode(GraphLabelRef);
				lref->labelname = labelname;
				lref->location = cref->location;

				result = (Node *) lref;
				break;
			}

		case T_BoolExpr:
			{
				BoolExpr *be = (BoolExpr *) labelexpr;
				ListCell *lc;
				List	   *args = NIL;

				foreach(lc, be->args)
				{
					Node	   *arg = (Node *) lfirst(lc);

					arg = transformLabelExpr(gpstate, arg);
					args = lappend(args, arg);
				}

				result = (Node *) makeBoolExpr(be->boolop, args, be->location);
				break;
			}

		default:
			/* should not reach here */
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(labelexpr));
			result = NULL;		/* keep compiler quiet */
			break;
	}

	return result;
}

static Node *
transformElementPattern(GraphTableParseState *gpstate, ElementPattern *ep)
{
	ParseState *pstate2;

	pstate2 = make_parsestate(NULL);
	pstate2->p_pre_columnref_hook = graph_table_property_reference;
	pstate2->p_ref_hook_state = gpstate;

	if (ep->variable)
		gpstate->variables = lappend(gpstate->variables, makeString(pstrdup(ep->variable)));

	ep->labelexpr = transformLabelExpr(gpstate, ep->labelexpr);

	gpstate->localvar = ep->variable;
	ep->whereClause = transformExpr(pstate2, ep->whereClause, EXPR_KIND_OTHER);
	gpstate->localvar = NULL;

	return (Node *) ep;
}

static Node *
transformPathTerm(GraphTableParseState *gpstate, List *path_term)
{
	List *result = NIL;
	ListCell *lc;

	foreach(lc, path_term)
	{
		Node *n = transformElementPattern(gpstate, lfirst_node(ElementPattern, lc));

		result = lappend(result, n);
	}

	return (Node *) result;
}

static Node *
transformPathPatternList(GraphTableParseState *gpstate, List *path_pattern)
{
	List *result = NIL;
	ListCell *lc;

	foreach(lc, path_pattern)
	{
		Node *n = transformPathTerm(gpstate, lfirst(lc));

		result = lappend(result, n);
	}

	return (Node *) result;
}

Node *
transformGraphPattern(GraphTableParseState *gpstate, List *graph_pattern)
{
	Node *path_pattern_list = linitial(graph_pattern);
	Node *where_clause = lsecond(graph_pattern);
	ParseState *pstate2;

	pstate2 = make_parsestate(NULL);
	pstate2->p_pre_columnref_hook = graph_table_property_reference;
	pstate2->p_ref_hook_state = gpstate;

	path_pattern_list = transformPathPatternList(gpstate, (List *) path_pattern_list);
	where_clause = transformExpr(pstate2, where_clause, EXPR_KIND_OTHER);

	return (Node *) list_make2(path_pattern_list, where_clause);
}
