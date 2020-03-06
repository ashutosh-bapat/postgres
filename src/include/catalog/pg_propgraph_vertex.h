/*-------------------------------------------------------------------------
 *
 * pg_propgraph_vertex.h
 *	  definition of the "property graph vertices" system catalog (pg_propgraph_vertex)
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_propgraph_vertex.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROPGRAPH_VERTEX_H
#define PG_PROPGRAPH_VERTEX_H

#include "catalog/genbki.h"
#include "catalog/pg_propgraph_vertex_d.h"

/* ----------------
 *		pg_propgraph_vertex definition.  cpp turns this into
 *		typedef struct FormData_pg_propgraph_vertex
 * ----------------
 */
CATALOG(pg_propgraph_vertex,8298,PropgraphVertexRelationId)
{
	Oid			oid;
	Oid			pgvpgid;		/* OID of the property graph */
	Oid			pgvrelid;		/* OID of underlying relation */
	NameData	pgvalias;		/* vertex alias */
	int2vector	pgvkey;			/* column numbers in pgvrelid relation */
} FormData_pg_propgraph_vertex;

/* ----------------
 *		Form_pg_propgraph_vertex corresponds to a pointer to a tuple with
 *		the format of pg_propgraph_vertex relation.
 * ----------------
 */
typedef FormData_pg_propgraph_vertex *Form_pg_propgraph_vertex;

#endif							/* PG_PROPGRAPH_VERTEX_H */
