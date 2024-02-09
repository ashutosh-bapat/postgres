/*-------------------------------------------------------------------------
 *
 * pg_propgraph_property.h
 *	  definition of the "property graph properties" system catalog (pg_propgraph_property)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_propgraph_property.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROPGRAPH_PROPERTY_H
#define PG_PROPGRAPH_PROPERTY_H

#include "catalog/genbki.h"
#include "catalog/pg_propgraph_property_d.h"

/* ----------------
 *		pg_propgraph_property definition.  cpp turns this into
 *		typedef struct FormData_pg_propgraph_property
 * ----------------
 */
CATALOG(pg_propgraph_property,8306,PropgraphPropertyRelationId)
{
	Oid			oid;

	/*
	 * OID of the property graph relation.  This can also be found out by
	 * chasing via pgplabelid, but having it here is more efficient.
	 */
	Oid         pgppgid BKI_LOOKUP(pg_class);

	/* property name */
	NameData	pgpname;

	/*
	 * Type of the property.  (This can be computed from pgpexpr, but storing
	 * it makes parsing GRAPH_TABLE more efficient.)
	 */
	Oid			pgptypid BKI_LOOKUP_OPT(pg_type);

	/* OID of the label */
	Oid			pgplabelid BKI_LOOKUP(pg_propgraph_label);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */

	/* property expression */
	pg_node_tree pgpexpr BKI_FORCE_NOT_NULL;

#endif
} FormData_pg_propgraph_property;

/* ----------------
 *		Form_pg_propgraph_property corresponds to a pointer to a tuple with
 *		the format of pg_propgraph_property relation.
 * ----------------
 */
typedef FormData_pg_propgraph_property *Form_pg_propgraph_property;

DECLARE_TOAST(pg_propgraph_property, 8309, 8310);

DECLARE_UNIQUE_INDEX_PKEY(pg_propgraph_property_oid_index, 8307, PropgraphPropertyObjectIndexId, pg_propgraph_property, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_propgraph_property_name_index, 8308, PropgraphPropertyNameIndexId, pg_propgraph_property, btree(pgplabelid oid_ops, pgpname name_ops));

DECLARE_INDEX(pg_propgraph_property_graph_name_index, 8311, PropgraphPropertyGraphNameIndexId, pg_propgraph_property, btree(pgppgid oid_ops, pgpname name_ops));

MAKE_SYSCACHE(PROPGRAPHPROPNAME, pg_propgraph_property_name_index, 128);

#endif							/* PG_PROPGRAPH_PROPERTY_H */
