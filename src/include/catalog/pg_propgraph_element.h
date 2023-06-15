/*-------------------------------------------------------------------------
 *
 * pg_propgraph_element.h
 *	  definition of the "property graph elements" system catalog (pg_propgraph_element)
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_propgraph_element.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROPGRAPH_ELEMENT_H
#define PG_PROPGRAPH_ELEMENT_H

#include "catalog/genbki.h"
#include "catalog/pg_propgraph_element_d.h"

/* ----------------
 *		pg_propgraph_element definition.  cpp turns this into
 *		typedef struct FormData_pg_propgraph_element
 * ----------------
 */
CATALOG(pg_propgraph_element,8299,PropgraphElementRelationId)
{
	Oid			oid;
	Oid			pgepgid;		/* OID of the property graph */
	Oid			pgerelid;		/* OID of the underlying relation */
	NameData	pgealias;		/* element alias */
	char		pgekind;		/* see PGEKIND_* below */
	Oid			pgesrcvertexid;	/* source vertex */
	Oid			pgedestvertexid;/* destination vertex */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	int2vector	pgekey;			/* column numbers in pgerelid relation */
	int2vector	pgesrckey;		/* column numbers in pgerelid relation */
	int2vector	pgesrcref;		/* column numbers in pgesrcvertexid relation */
	int2vector	pgedestkey;		/* column numbers in pgerelid relation */
	int2vector	pgedestref;		/* column numbers in pgedestvertexid relation */
#endif
} FormData_pg_propgraph_element;

/* ----------------
 *		Form_pg_propgraph_element corresponds to a pointer to a tuple with
 *		the format of pg_propgraph_element relation.
 * ----------------
 */
typedef FormData_pg_propgraph_element *Form_pg_propgraph_element;

DECLARE_UNIQUE_INDEX_PKEY(pg_propgraph_element_oid_index, 8300, PropgraphElementObjectIndexId, on pg_propgraph_element using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_propgraph_element_alias_index, 8301, PropgraphElementAliasIndexId, on pg_propgraph_element using btree(pgealias name_ops));

#ifdef EXPOSE_TO_CLIENT_CODE

/*
 * Symbolic values for pgekind column
 */
#define PGEKIND_VERTEX 'v'
#define PGEKIND_EDGE 'e'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_PROPGRAPH_ELEMENT_H */
