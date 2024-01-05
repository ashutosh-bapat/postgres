/*-------------------------------------------------------------------------
 *
 * pg_propgraph_label.h
 *	  definition of the "property graph labels" system catalog (pg_propgraph_label)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_propgraph_label.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROPGRAPH_LABEL_H
#define PG_PROPGRAPH_LABEL_H

#include "catalog/genbki.h"
#include "catalog/pg_propgraph_label_d.h"

/* ----------------
 *		pg_propgraph_label definition.  cpp turns this into
 *		typedef struct FormData_pg_propgraph_label
 * ----------------
 */
CATALOG(pg_propgraph_label,8303,PropgraphLabelRelationId)
{
	Oid			oid;

	/* label name */
	NameData	pgllabel;

	/* OID of the property graph element */
	Oid			pglelid BKI_LOOKUP(pg_propgraph_element);
} FormData_pg_propgraph_label;

/* ----------------
 *		Form_pg_propgraph_label corresponds to a pointer to a tuple with
 *		the format of pg_propgraph_label relation.
 * ----------------
 */
typedef FormData_pg_propgraph_label *Form_pg_propgraph_label;

DECLARE_UNIQUE_INDEX_PKEY(pg_propgraph_label_oid_index, 8304, PropgraphLabelObjectIndexId, pg_propgraph_label, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_propgraph_label_label_index, 8305, PropgraphLabelLabelIndexId, pg_propgraph_label, btree(pglelid oid_ops, pgllabel name_ops));

#endif							/* PG_PROPGRAPH_LABEL_H */
