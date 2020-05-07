/*-------------------------------------------------------------------------
 *
 * pg_propgraph_edge.h
 *	  definition of the "property graph edges" system catalog (pg_propgraph_edge)
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_propgraph_edge.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROPGRAPH_EDGE_H
#define PG_PROPGRAPH_EDGE_H

#include "catalog/genbki.h"
#include "catalog/pg_propgraph_edge_d.h"

/* ----------------
 *		pg_propgraph_edge definition.  cpp turns this into
 *		typedef struct FormData_pg_propgraph_edge
 * ----------------
 */
CATALOG(pg_propgraph_edge,8299,PropgraphEdgeRelationId)
{
	Oid			oid;
	Oid			pgepgid;		/* OID of the property graph */
	Oid			pgerelid;		/* OID of the underlying relation */
	NameData	pgealias;		/* edge alias */
	Oid			pgesrcvertexid;	/* source vertex */
	Oid			pgedestvertexid;/* destination vertex */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	int2vector	pgekey;			/* column numbers in pgerelid relation */
	int2vector	pgesrckey;		/* column numbers in pgerelid relation */
	int2vector	pgesrcref;		/* column numbers in pgesrcvertexid relation */
	int2vector	pgedestkey;		/* column numbers in pgerelid relation */
	int2vector	pgedestref;		/* column numbers in pgedestvertexid relation */
#endif
} FormData_pg_propgraph_edge;

/* ----------------
 *		Form_pg_propgraph_edge corresponds to a pointer to a tuple with
 *		the format of pg_propgraph_edge relation.
 * ----------------
 */
typedef FormData_pg_propgraph_edge *Form_pg_propgraph_edge;

#endif							/* PG_PROPGRAPH_EDGE_H */
