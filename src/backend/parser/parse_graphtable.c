/*-------------------------------------------------------------------------
 *
 * parse_graphtable.c
 *	  parsing of GRAPH_TABLE
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
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
#include "catalog/pg_propgraph_element.h"
#include "catalog/pg_propgraph_label.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_graphtable.h"
#include "parser/parse_node.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"


/*
 * Get the type of a property.
 *
 * TODO: Properties with the same property name associated with different
 * labels may have different types. This function returns just one of them. Fix
 * it. This function might be useful in insert_property_record() which tries to
 * find a common data type for all properties with the same name.
 */
Oid
get_property_type(Oid graphid, const char *propname)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[2];
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

/*
 * Resolve a property reference.
 */
Node *
graph_table_property_reference(ParseState *pstate, ColumnRef *cref)
{
	GraphTableParseState *gpstate = pstate->p_ref_hook_state;
	GraphPropertyRef *gpr = makeNode(GraphPropertyRef);

	gpr->location = cref->location;

	if (list_length(cref->fields) == 2)
	{
		Node	   *field1 = linitial(cref->fields);
		Node	   *field2 = lsecond(cref->fields);
		char	   *elvarname;
		char	   *propname;

		elvarname = strVal(field1);
		propname = strVal(field2);

		if (!list_member(gpstate->variables, field1))
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("graph pattern variable \"%s\" does not exist", elvarname),
					parser_errposition(pstate, cref->location));

		gpr->elvarname = elvarname;
		gpr->propname = propname;

	}
	else
		elog(ERROR, "invalid property reference");

	gpr->typeId = get_property_type(gpstate->graphid, gpr->propname);

	return (Node *) gpr;
}

/*
 * Transform a label expression.
 */
static Node *
transformLabelExpr(GraphTableParseState *gpstate, Node *labelexpr)
{
	Node	   *result;

	if (labelexpr == NULL)
		return NULL;

	check_stack_depth();

	switch (nodeTag(labelexpr))
	{
		case T_ColumnRef:
			{
				ColumnRef  *cref = (ColumnRef *) labelexpr;
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
				BoolExpr   *be = (BoolExpr *) labelexpr;
				ListCell   *lc;
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

/*
 * Transform a GraphElementPattern.
 */
static Node *
transformGraphElementPattern(GraphTableParseState *gpstate, GraphElementPattern *gep)
{
	ParseState *pstate2;

	pstate2 = make_parsestate(NULL);
	pstate2->p_pre_columnref_hook = graph_table_property_reference;
	pstate2->p_ref_hook_state = gpstate;

	if (gep->variable)
		gpstate->variables = lappend(gpstate->variables, makeString(pstrdup(gep->variable)));

	gep->labelexpr = transformLabelExpr(gpstate, gep->labelexpr);

	gep->whereClause = transformExpr(pstate2, gep->whereClause, EXPR_KIND_OTHER);
	assign_expr_collations(pstate2, gep->whereClause);

	return (Node *) gep;
}

/*
 * Transform a path term (list of GraphElementPattern's).
 */
static Node *
transformPathTerm(GraphTableParseState *gpstate, List *path_term)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, path_term)
	{
		Node	   *n = transformGraphElementPattern(gpstate, lfirst_node(GraphElementPattern, lc));

		result = lappend(result, n);
	}

	return (Node *) result;
}

/*
 * Transform a path pattern list (list of path terms).
 */
static Node *
transformPathPatternList(GraphTableParseState *gpstate, List *path_pattern)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, path_pattern)
	{
		Node	   *n = transformPathTerm(gpstate, lfirst(lc));

		result = lappend(result, n);
	}

	return (Node *) result;
}

/*
 * Transform a GraphPattern.
 */
Node *
transformGraphPattern(GraphTableParseState *gpstate, GraphPattern *graph_pattern)
{
	ParseState *pstate2;

	pstate2 = make_parsestate(NULL);
	pstate2->p_pre_columnref_hook = graph_table_property_reference;
	pstate2->p_ref_hook_state = gpstate;

	graph_pattern->path_pattern_list = (List *) transformPathPatternList(gpstate, graph_pattern->path_pattern_list);
	graph_pattern->whereClause = transformExpr(pstate2, graph_pattern->whereClause, EXPR_KIND_OTHER);
	assign_expr_collations(pstate2, graph_pattern->whereClause);

	return (Node *) graph_pattern;
}

List *
get_element_labelids(Oid graphid, Oid eleoid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tup;
	List	   *result = NIL;

	rel = table_open(PropgraphLabelRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pglpgid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(graphid));
	ScanKeyInit(&key[1],
				Anum_pg_propgraph_label_pglelid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(eleoid));

	scan = systable_beginscan(rel, InvalidOid,
							  false, NULL, 2, key);

	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_propgraph_label label = (Form_pg_propgraph_label) GETSTRUCT(tup);

		if (label->pglelid == eleoid)
			result = lappend_oid(result, label->oid);
	}
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * Return the names of properties associated with the given label name.
 *
 * There may be many labels with the same name in a given property graph, but all of them need to have the same number of properties with the same names and data types. Hence properties associated with any label may be returned.
 *
 * If `labelid` is a valid OID, entry corresponding that OID is not returned. This can be used to compare property from that label with existing property definitions.
 */
List *
get_label_property_names(const Oid graphid, const char *label_name, Oid labelid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tup;
	List	   *result = NIL;
	Oid			ref_labelid = InvalidOid;

	/*
	 * Find a reference label to fetch label properties. The reference label
	 * should be some label other than the one being modified.
	 */
	rel = table_open(PropgraphLabelRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pglpgid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(graphid));
	ScanKeyInit(&key[1],
				Anum_pg_propgraph_label_pgllabel,
				BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(label_name));

	scan = systable_beginscan(rel, PropgraphLabelGraphNameIndexId,
							  true, NULL, 2, key);

	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_propgraph_label label = (Form_pg_propgraph_label) GETSTRUCT(tup);

		if (label->oid != labelid)
			ref_labelid = label->oid;
	}
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	if (!OidIsValid(ref_labelid))
		return NIL;

	/* Get all the properties of the reference label */
	rel = table_open(PropgraphPropertyRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_propgraph_property_pgplabelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ref_labelid));

	scan = systable_beginscan(rel, PropgraphPropertyNameIndexId, true, NULL, 1, key);

	while ((tup = systable_getnext(scan)))
	{
		Form_pg_propgraph_property pgpform = (Form_pg_propgraph_property) GETSTRUCT(tup);

		result = lappend(result, NameStr(pgpform->pgpname));
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}