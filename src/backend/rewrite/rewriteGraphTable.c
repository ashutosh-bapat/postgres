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
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "parser/parse_graphtable.h"
#include "rewrite/rewriteGraphTable.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * Holds information about one graph element (vertex or edge) in a given path calculated from Graph path pattern. This is similar to GraphElementPattern except this deals with only one vertex or edge table at a time.
 *
 * TODO: Should this be promoted to Node?
 */
typedef struct GraphPathElement
{
	Oid			lelemid;
	List		*labelids;
	GraphElementPattern *parent_gep;
} GraphPathElement;

struct elvar_rt_mapping
{
	const char *elvarname;
	List 	 *labelids;
	List	*labelnames;
	int			rt_index;
};

#ifdef TODO_NOT_USED
static Oid	get_labelid(Oid graphid, const char *labelname, Oid elemid);
#endif /* TODO_NOT_USED */
static List *add_elements_for_label(Oid graphid, const char *labelname, GraphElementPattern *gep, List *lelems);
static Oid	get_table_for_element(Oid elid);
static Node *replace_property_refs(Oid propgraphid, Node *node, const List *mappings);
static List *build_edge_vertex_link_quals(HeapTuple edgetup, int edgerti, int refrti, AttrNumber catalog_key_attnum, AttrNumber catalog_ref_attnum);
static List *generate_paths_from_pattern(Oid propgraphid, List *element_patterns);
static Query *generate_query_for_path(RangeTblEntry *rte, List *path);
static Node *generate_setop_from_pathqueries(List *pathqueries, List **rtable, List **targetlist);
static List *generate_paths_from_pattern_recurse(Oid propgraphid, List *element_patterns, int cur_elem, List *cur_path, List *paths);
static Query *generate_query_for_empty_path_pattern(RangeTblEntry *rte);
static GraphPathElement *find_gpe_for_element(List *elems, Oid elemid);
static List *list_append_unique_str(List *list, const char *str);
static bool list_search_str(List *list, const char *str);

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
	List	   *paths;
	List *pathqueries = NIL;
	Node *setop;
	List *rtable = NIL;

	rte = rt_fetch(rt_index, parsetree->rtable);

	if (list_length(rte->graph_pattern->path_pattern_list) != 1)
		elog(ERROR, "unsupported path pattern list length");

	element_patterns = linitial(rte->graph_pattern->path_pattern_list);
	paths = generate_paths_from_pattern(rte->relid, element_patterns);

	foreach (lc, paths)
	{
		List *path = lfirst_node(List, lc);
		Query *pathquery = generate_query_for_path(rte, path);

		if (pathquery)
			pathqueries = lappend(pathqueries, pathquery);
	}

	/*
	 * TODO: Instead of throwing an error in this case, we should construct a
	 * query which does not return any rows like a query with (false) qual.
	 */
	if (!pathqueries)
		pathqueries = list_make1(generate_query_for_empty_path_pattern(rte));

	setop = generate_setop_from_pathqueries(pathqueries, &rtable, NULL);
	/* pathqueries is no more reliable and is not required anymore. */
	pathqueries = NIL;
	if (IsA(setop, RangeTblRef))
	{
		RangeTblRef *rtr = castNode(RangeTblRef, setop);

		newsubquery = rt_fetch(rtr->rtindex, rtable)->subquery;
	}
	else
	{
		Node *node;
		SetOperationStmt	*sostmt = castNode(SetOperationStmt, setop);
		int			leftmostRTI;
		Query	   *leftmostQuery;
		List	   *targetvars,
			   *targetnames;
		int			resno;
		List	*fromlist = NIL;
		ListCell   *left_tlist,
			   *lct,
			   *lcm,
			   *lcc;

		newsubquery = makeNode(Query);
		newsubquery->commandType = CMD_SELECT;
		newsubquery->rtable = rtable;
		newsubquery->setOperations = (Node *) sostmt;
				/* TODO: Most of the following code is copied from TransformSetOperationStmt to construct the top level query list. Can we deduplicate this? */

		node = sostmt->larg;
		while (node && IsA(node, SetOperationStmt))
			node = ((SetOperationStmt *) node)->larg;
		Assert(node && IsA(node, RangeTblRef));
		leftmostRTI = ((RangeTblRef *) node)->rtindex;
		leftmostQuery = rt_fetch(leftmostRTI, rtable)->subquery;
		Assert(leftmostQuery != NULL);

		/*
		 * Generate dummy targetlist for outer query using column names of
		 * leftmost select and common datatypes/collations of topmost set
		 * operation.  Also make lists of the dummy vars and their names for use
		 * in parsing ORDER BY.
		 *
		 * Note: we use leftmostRTI as the varno of the dummy variables. It
		 * shouldn't matter too much which RT index they have, as long as they
		 * have one that corresponds to a real RT entry; else funny things may
		 * happen when the tree is mashed by rule rewriting.
		 */
		newsubquery->targetList = NULL;
		targetvars = NIL;
		targetnames = NIL;
		resno = 1;
		forfour(lct, sostmt->colTypes,
				lcm, sostmt->colTypmods,
				lcc, sostmt->colCollations,
				left_tlist, leftmostQuery->targetList)
		{
			Oid			colType = lfirst_oid(lct);
			int32		colTypmod = lfirst_int(lcm);
			Oid			colCollation = lfirst_oid(lcc);
			TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
			char	   *colName;
			TargetEntry *tle;
			Var		   *var;

			Assert(!lefttle->resjunk);
			colName = pstrdup(lefttle->resname);
			var = makeVar(leftmostRTI,
						  lefttle->resno,
						  colType,
						  colTypmod,
						  colCollation,
						  0);
			var->location = exprLocation((Node *) lefttle->expr);
			tle = makeTargetEntry((Expr *) var,
								  (AttrNumber) resno++,
								  colName,
								  false);
			newsubquery->targetList = lappend(newsubquery->targetList, tle);
			targetvars = lappend(targetvars, var);
			targetnames = lappend(targetnames, makeString(colName));
		}

		newsubquery->rteperminfos = NIL;
		newsubquery->jointree = makeFromExpr(fromlist, NULL);
		/* TODO: Do we need to assign query collations? */
	}

	AcquireRewriteLocks(newsubquery, true, false);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = newsubquery;

	/*
	 * Reset no longer applicable fields, to appease
	 * WRITE_READ_PARSE_PLAN_TREES.
	 */
	rte->graph_pattern = NULL;
	rte->graph_table_columns = NIL;

	return parsetree;
}

static Query *
generate_query_for_path(RangeTblEntry *rte, List *path)
{
	ListCell *lc;
	List	*element_ids = NIL;
	Query	   *newsubquery = makeNode(Query);
	List	   *fromlist = NIL;
	List	   *elvar_rt_mappings = NIL;
	List	   *qual_exprs = NIL;
	bool		broken_path = true;

	newsubquery->commandType = CMD_SELECT;

	foreach(lc, path)
	{
		GraphPathElement *gpe = (GraphPathElement *) lfirst(lc);
		GraphElementPattern *gep = gpe->parent_gep;
		struct elvar_rt_mapping *erm;
#ifdef TODO_NOT_USED
		RangeTblEntry *r = makeNode(RangeTblEntry);
		RTEPermissionInfo *rpi;
#endif /* TODO_NOT_USED */
		Oid			elid = gpe->lelemid;
		Oid			relid;
		RangeTblRef *rtr;
		Relation	rel;
		ParseNamespaceItem *pni;

		Assert(gep->kind == VERTEX_PATTERN || gep->kind == EDGE_PATTERN_LEFT || gep->kind == EDGE_PATTERN_RIGHT);
		Assert(!gep->quantifier);

		element_ids = lappend_oid(element_ids, elid);
		relid = get_table_for_element(elid);
		rel = table_open(relid, AccessShareLock);
		pni = addRangeTableEntryForRelation(make_parsestate(NULL), rel, AccessShareLock, NULL, true, false);
		table_close(rel, NoLock);
#ifdef TODO_NOT_USED
		rpi = makeNode(RTEPermissionInfo);
		rpi->relid = relid;
		rpi->checkAsUser = 0;
		rpi->requiredPerms = ACL_SELECT;
#endif /* TODO_NOT_USED */
		newsubquery->rtable = lappend(newsubquery->rtable, pni->p_rte);
		newsubquery->rteperminfos = lappend(newsubquery->rteperminfos, pni->p_perminfo);
		pni->p_rte->perminfoindex = list_length(newsubquery->rteperminfos);

#ifdef TODO_NOT_USED
		r->rtekind = RTE_RELATION;
		r->relid = relid;
		r->relkind = get_rel_relkind(relid);
		r->rellockmode = AccessShareLock;
		r->inh = true;
#endif /* TODO_NOT_USED */

		rtr = makeNode(RangeTblRef);
		rtr->rtindex = list_length(newsubquery->rtable);
		fromlist = lappend(fromlist, rtr);

		erm = palloc0_object(struct elvar_rt_mapping);
		erm->elvarname = gep->variable;
		erm->labelids = gpe->labelids;
		erm->labelnames = gep->labels;
		erm->rt_index = list_length(newsubquery->rtable);

		elvar_rt_mappings = lappend(elvar_rt_mappings, erm);

		if (gep->whereClause)
		{
			Node	   *tr;

			tr = replace_property_refs(rte->relid, gep->whereClause, list_make1(erm));

			qual_exprs = lappend(qual_exprs, tr);
		}
	}

	/* Iterate over edges only */
	for (int k = 1; k < list_length(element_ids); k += 2)
	{
		GraphPathElement *gpe = list_nth(path, k);
		GraphElementPattern *gep = gpe->parent_gep;
		Oid			elid = gpe->lelemid;
		HeapTuple	tuple;
		Form_pg_propgraph_element pgeform;
		int			srcvertexoffset;
		int			destvertexoffset;

		tuple = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(elid));
		if (!tuple)
			elog(ERROR, "cache lookup failed for property graph element %u", elid);
		pgeform = ((Form_pg_propgraph_element) GETSTRUCT(tuple));

		/*
		 * Element type in the property graph and the graph pattern in the query mismatch. This will probably fail few lines below since vertex elements won't have source and destination. But better to do it here.
		 *
		 * TODO: Better even if we do this while we construct the element list. At that time we should also populate GraphPathElement with other required fields in the tuple. This will also make sure to test vertex types as well.
		 */
		Assert ((pgeform->pgekind == 'v' && gep->kind == VERTEX_PATTERN) ||
			(pgeform->pgekind == 'e' && IS_EDGE_PATTERN(gep->kind)));

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
		{
			Assert(false);
			srcvertexoffset = 0;
			destvertexoffset = 0;
		}

		/*
		 * source link
		 */
		if (pgeform->pgesrcvertexid != list_nth_oid(element_ids, k + srcvertexoffset))
		{
			broken_path = false;
			ReleaseSysCache(tuple);
			break;
		}
		else
			qual_exprs = list_concat(qual_exprs,
									 build_edge_vertex_link_quals(tuple, k + 1, k + 1 + srcvertexoffset,
																  Anum_pg_propgraph_element_pgesrckey, Anum_pg_propgraph_element_pgesrcref));

		/*
		 * dest link
		 */
		if (pgeform->pgedestvertexid != list_nth_oid(element_ids, k + destvertexoffset))
		{
			broken_path = false;
			ReleaseSysCache(tuple);
			break;
		}
		else
			qual_exprs = list_concat(qual_exprs,
									 build_edge_vertex_link_quals(tuple, k + 1, k + 1 + destvertexoffset,
																  Anum_pg_propgraph_element_pgedestkey, Anum_pg_propgraph_element_pgedestref));

		ReleaseSysCache(tuple);
	}

	/* If the path is broken, the query won't return any results. */
	/*
	 * TODO: We could probably avoid creating a path in
	 * generate_paths_from_pattern()in such a case. But then we would duplicate
	 * some code there. We waste some memory building half-complete Query that
	 * we will throw away. But that can be fixed by using a memory context to
	 * construct the query. If complete query is built, it is copied in the
	 * callers context and the temporary context can be destroyed, destroying
	 * any half-baked query.
	 */
	if (!broken_path)
		return NULL;

	newsubquery->jointree = makeFromExpr(fromlist, (Node *) makeBoolExpr(AND_EXPR, qual_exprs, -1));

	foreach(lc, rte->graph_table_columns)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Node	   *nte;

		nte = replace_property_refs(rte->relid, (Node *) te, elvar_rt_mappings);
		newsubquery->targetList = lappend(newsubquery->targetList, nte);
	}

	return newsubquery;
}

static Query *
generate_query_for_empty_path_pattern(RangeTblEntry *rte)
{
	Query	   *newsubquery = makeNode(Query);
	ListCell *lc;

	newsubquery->commandType = CMD_SELECT;


	newsubquery->rtable = NIL;
	newsubquery->rteperminfos = NIL;


	newsubquery->jointree = makeFromExpr(NIL, (Node *) makeBoolConst(false, false));

	foreach(lc, rte->graph_table_columns)
	{
		TargetEntry *te = copyObject(lfirst_node(TargetEntry, lc));
		Node *nte = (Node *) te->expr;

		te->expr = (Expr *) makeNullConst(exprType(nte), exprTypmod(nte), exprCollation(nte));
		newsubquery->targetList = lappend(newsubquery->targetList, te);
	}

	return newsubquery;
}

static Node *
generate_setop_from_pathqueries(List *pathqueries, List **rtable, List **targetlist)
{
	SetOperationStmt *sostmt;
	Query *lquery;
	Node  *rarg;
#ifdef TODO_NOT_USED
	RangeTblEntry *lrte = makeNode(RangeTblEntry);
#endif /* TODO_NOT_USED */
	RangeTblRef	*lrtr = makeNode(RangeTblRef);
	List *rtargetlist;
	ParseNamespaceItem	*pni;

	if (list_length(pathqueries) == 0)
	{
		*targetlist = NIL;
		return NULL;
	}

	lquery = linitial_node(Query, pathqueries);
	pni = addRangeTableEntryForSubquery(make_parsestate(NULL), lquery, NULL, false, false);
#ifdef TODO_NOT_USED
	lrte->rtekind = RTE_SUBQUERY;
	lrte->subquery = lquery;
	lrte->lateral = false;
	lrte->inh = false;	/* Never true for subqueries. */
	lrte->inFromCl = false;
#endif /* TODO_NOT_USED */
	*rtable = lappend(*rtable, pni->p_rte);
	lrtr->rtindex = list_length(*rtable);

	rarg = generate_setop_from_pathqueries(list_delete_first(pathqueries), rtable, &rtargetlist);
	if (rarg == NULL)
	{
	/*
	 * Just a single element list results in a simple query. Extract a list of
	 * the non-junk TLEs for upper-level processing.
	 */

	if (targetlist)
	{
		ListCell   *tl;

		*targetlist = NIL;
		foreach(tl, lquery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tl);

			if (!tle->resjunk)
				*targetlist = lappend(*targetlist, tle);
		}
	}
		return (Node *) lrtr;
	}

	sostmt = makeNode(SetOperationStmt);
	sostmt->op = SETOP_UNION;	/* Should be UNION ALL? */
	sostmt->all = true;
	sostmt->larg = (Node *) lrtr;
	sostmt->rarg = rarg;
	constructSetOpTargetlist(sostmt, lquery->targetList, rtargetlist, targetlist, "UNION", NULL, false);

	return (Node *) sostmt;
}

/*
 * Given a path pattern generate all the possible paths.
 */
static List *
generate_paths_from_pattern(Oid propgraphid, List *element_patterns)
{
	int	num_elems = list_length(element_patterns);
	List *paths;

	Assert(num_elems > 0);

	paths = generate_paths_from_pattern_recurse(propgraphid, element_patterns, 0, NIL, NIL);
	return paths;
}

/*
 * TODO: The way this function makes copies of paths is in-efficient. Try to avoid it.
 */
static List *
generate_paths_from_pattern_recurse(Oid propgraphid, List *element_patterns, int cur_elem, List *cur_path, List *paths)
{
	GraphElementPattern *gep = list_nth_node(GraphElementPattern, element_patterns, cur_elem);
	List *lelems;
	ListCell *lc;

	if (!(gep->kind == VERTEX_PATTERN || gep->kind == EDGE_PATTERN_LEFT || gep->kind == EDGE_PATTERN_RIGHT))
		elog(ERROR, "unsupported element pattern kind: %u", gep->kind);

	if (gep->quantifier)
		elog(ERROR, "element pattern quantifier not supported yet");

	if (!gep->labelexpr)
		lelems = add_elements_for_label(propgraphid, NULL, gep, NIL);
	else if (IsA(gep->labelexpr, GraphLabelRef))
	{
		GraphLabelRef *glr = castNode(GraphLabelRef, gep->labelexpr);

		lelems = add_elements_for_label(propgraphid, glr->labelname, gep, NIL);
	}
	else if (IsA(gep->labelexpr, BoolExpr))
	{
		BoolExpr *be = castNode(BoolExpr, gep->labelexpr);
		List *label_exprs = be->args;
		ListCell *llc;

		lelems = NIL;
		/*
		 * It might seem that the following code should recursively handle
		 * BoolExprs but gram.y's makeOrExpr will flatten the label disjunction
		 * into a list of GraphLabelRef's. This code just iterates over the
		 * list.
		 */
		foreach (llc, label_exprs)
		{
			GraphLabelRef *glr = lfirst_node(GraphLabelRef, llc);

			/* TODO: What to do with duplicate labels? */
			lelems = add_elements_for_label(propgraphid, glr->labelname, gep, lelems);
		}
	}
	else
		elog(ERROR, "unsupported label expression type: %d", (int) nodeTag(gep->labelexpr));

	foreach (lc, lelems)
	{
		GraphPathElement *gpe = lfirst(lc);

		/* Copy current path to scribble upon */
		List *cur_path_copy = list_copy(cur_path);

		/* Add new elment to the current path after copying it. */
		cur_path_copy = lappend(cur_path_copy, gpe);

		/*
		 * If this is the last element in the path, the path is complete,
		 * add it to the paths.
		 */
		if (list_length(element_patterns) == list_length(cur_path_copy))
		{
			Assert(cur_elem == list_length(element_patterns) - 1);
			paths = lappend(paths, cur_path_copy);
		}
		else
		{
			paths = generate_paths_from_pattern_recurse(propgraphid, element_patterns, cur_elem + 1, cur_path_copy, paths);
			/* Our copy with this element won't be required anymore. Any recursive calls must have made their own copy.
			*/
			list_free(cur_path_copy);
		}
	}

	return paths;
}

#ifdef TODO_NOT_USED
/*
 * Get label OID from graph OID, label name and element OID.
 *
 * TODO: combine this with add_elements_for_label()
 */
static Oid
get_labelid(Oid graphid, const char *labelname, Oid elemid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];
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
	ScanKeyInit(&key[2],
				Anum_pg_propgraph_label_pglelid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(elemid));

	scan = systable_beginscan(rel, InvalidOid,
							  true, NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		if (OidIsValid(result))
			elog(ERROR, "more than one label entry found in property graph %u, with label name \"%s\" for element OID %u", graphid, labelname, elemid);
		result = ((Form_pg_propgraph_label) GETSTRUCT(tup))->oid;
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return result;
}
#endif /* TODO_NOT_USED */

/*
 * Add graph elements for a given label in the given list of elements.
 *
 * TODO: May be we should bring calling code down here so that there's more context around how the labels are being calculated. Eases setting the labels in GEP.
 * TODO: Ideally the labels which can not be resolved for a given element should be in GPE which is created here. The resolved label's OIDs are stored in GPE. We should probably add all the labels to GPE to start with and then delete the once which get resolved. That won't be possible for no-label case though.
 */
static List *
add_elements_for_label(Oid graphid, const char *labelname, GraphElementPattern *gep, List *lelems)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tup;
	int			numkeys = labelname ? 2 : 1;
	bool		found = false;

	rel = table_open(PropgraphLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_label_pglpgid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(graphid));
	if (labelname)
		ScanKeyInit(&key[1],
					Anum_pg_propgraph_label_pgllabel,
					BTEqualStrategyNumber,
					F_NAMEEQ, CStringGetDatum(labelname));

	scan = systable_beginscan(rel, PropgraphLabelGraphNameIndexId,
							  true, NULL, numkeys, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_propgraph_label label = (Form_pg_propgraph_label) GETSTRUCT(tup);
		GraphPathElement *gpe = find_gpe_for_element(lelems, label->pglelid);

		if (!gpe)
		{
			HeapTuple eletup = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(label->pglelid));
			Form_pg_propgraph_element pgeform;

			if (!eletup)
				elog(ERROR, "cache lookup failed for property graph element %u", label->pglelid);
			pgeform = ((Form_pg_propgraph_element) GETSTRUCT(eletup));

			if ((pgeform->pgekind == 'v' && gep->kind != VERTEX_PATTERN) ||
			(pgeform->pgekind == 'e' && !IS_EDGE_PATTERN(gep->kind)))
			{
				ReleaseSysCache(eletup);
				continue;
			}
			ReleaseSysCache(eletup);

			gpe = (GraphPathElement *) palloc0_object(GraphPathElement);
			gpe->lelemid = label->pglelid;
			gpe->parent_gep = gep;
			lelems = lappend(lelems, gpe);
		}

		found = true;
		Assert(gpe->parent_gep == gep);
		gpe->labelids = list_append_unique_oid(gpe->labelids, label->oid);
		/* Save labelname if not already known. */
		/* TODO: the unique-ness check is only required when no label is
		 * specified. In all the other cases, the list of labels should be
		 * unique. */
		gep->labels = list_append_unique_str(gep->labels,
		NameStr(label->pgllabel));
	}

	systable_endscan(scan);
	table_close(rel, RowShareLock);

	/* TODO: give name of the kind rather than its code */
	if (!found)
		elog(ERROR, "can not find label \"%s\" in property graph \"%s\" for element type %d", labelname, get_rel_name(graphid), gep->kind);

	return lelems;
}

/*
 * Add a copy of the given string to the list of strings if not already
 * present.
 */
static List *
list_append_unique_str(List *list, const char *str)
{
	if (!list_search_str(list, str))
		list = lappend(list, pstrdup(str));

	return list;
}


static GraphPathElement *
find_gpe_for_element(List *elems, Oid elemid)
{
	ListCell *lc;

	foreach (lc, elems)
	{
		GraphPathElement *gpe = (GraphPathElement *) lfirst(lc);

		if (gpe->lelemid == elemid)
			return gpe;
	}

	return NULL;
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
	Oid	propgraphid;
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
		Node	   *n = NULL;
		ListCell   *lc;
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

		/* Find property definition for given label element. */
		foreach (lc, found_mapping->labelids)
		{
			HeapTuple	tup = SearchSysCache2(PROPGRAPHPROPNAME, ObjectIdGetDatum(lfirst_oid(lc)), CStringGetDatum(gpr->propname));

			if (!tup)
				continue;

			n = stringToNode(TextDatumGetCString(SysCacheGetAttrNotNull(PROPGRAPHPROPNAME, tup, Anum_pg_propgraph_property_pgpexpr)));
			ChangeVarNodes(n, 1, found_mapping->rt_index, 0);

			ReleaseSysCache(tup);
		}

		/*
		 * If the property is defined for any label mentioned in the element
		 * pattern but not for given element it's value is considered NULL for
		 * that element.
		 */
		if (!n)
		{
			foreach (lc, found_mapping->labelnames)
			{
				List *lprops = get_label_property_names(context->propgraphid, lfirst(lc), InvalidOid);

				if (list_search_str(lprops, gpr->propname))
				{
					/* TODO: How to get type, typemod and collation of a property? */
					n = (Node *) makeNullConst(get_property_type(context->propgraphid, gpr->propname),
						-1, InvalidOid);
					break;
				}
			}
		}

		/* The property is not defined for any of the labels. */
		if (!n)
			elog(ERROR, "property \"%s\" of element \"%s\" not found", gpr->propname, found_mapping->elvarname);

		return n;
	}

	return expression_tree_mutator(node, replace_property_refs_mutator, context);
}

static bool
list_search_str(List *list, const char *str)
{
	ListCell *lc;

	foreach (lc, list)
	{
		const char *lstr = (const char *) lfirst(lc);

		if (strcmp(str, lstr) == 0)
			return true;
	}

	return false;
}

static Node *
replace_property_refs(Oid propgraphid, Node *node, const List *mappings)
{
	struct replace_property_refs_context context;

	context.mappings = mappings;
	context.propgraphid = propgraphid;

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
	Datum	   *d1,
			   *d2;
	int			n1,
				n2;

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
		 * TODO: Assumes types the same on both sides; no collations yet. Some
		 * of this could probably be shared with foreign key triggers.
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
