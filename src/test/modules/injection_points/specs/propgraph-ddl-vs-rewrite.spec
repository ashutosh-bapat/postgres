# Test a GRAPH_TABLE rewrite against a concurrent property redefinition.  This
# is a more invasive test version of propgraph-ddl-vs-query isolation test.
# This test uses injection point to create a window to attempt to change the
# property graph. When rewriting a query involving a property graph, property
# graph catalogs are consulted at various times. If we allow a property graph to
# be modified while the rewriting is happening, we will encounter errors or the
# rewritten query may be wrong. Test that that does not happen.

setup
{
	CREATE EXTENSION injection_points;

	CREATE FUNCTION pgq_f(t text) RETURNS text IMMUTABLE LANGUAGE sql
		AS $$ SELECT t $$;

	CREATE TABLE pgq_vt (id int PRIMARY KEY, name text);
	INSERT INTO pgq_vt VALUES (1, 'a1');

	CREATE PROPERTY GRAPH pgq
		VERTEX TABLES (pgq_vt KEY (id) LABEL pgql PROPERTIES (pgq_f(name) AS name));
}

teardown
{
	DROP PROPERTY GRAPH IF EXISTS pgq;
	DROP TABLE IF EXISTS pgq_vt;
	DROP FUNCTION IF EXISTS pgq_f(text);
	DROP EXTENSION injection_points;
}

session s1

# Make the query wait before it resolves elements into the underlying
# tables. It reads pg_propgraph_property catalog before and after this
# injection point.
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('graph-table-resolve-elements', 'wait');
}
step s1_begin { BEGIN; }
step s1_query
{
	SELECT * FROM GRAPH_TABLE (pgq MATCH (a IS pgql) COLUMNS (a.name)) ORDER BY name;
}
step s1_commit { COMMIT; }
step s1_noop { }

session s2
# Redefine a property so that its OID changes. If these modifications happen
# while the query is waiting at the injection point, rewriter will read two
# different pg_propgraph_property rows for the same property before and after
# the wait leading to an error.
step s2_redefine_prop
{
	ALTER PROPERTY GRAPH pgq ALTER VERTEX TABLE pgq_vt ALTER LABEL pgql
		DROP PROPERTIES (name);
	ALTER PROPERTY GRAPH pgq ALTER VERTEX TABLE pgq_vt ALTER LABEL pgql
		ADD PROPERTIES (upper(name) AS name);
}

# Drop the same property through DROP CASCADE.
step s2_drop_prop_func { DROP FUNCTION pgq_f(text) CASCADE; }

session s3
step s3_wakeup { SELECT injection_points_wakeup('graph-table-resolve-elements'); }
step s3_detach { SELECT injection_points_detach('graph-table-resolve-elements'); }

permutation s1_begin s1_query s2_redefine_prop s3_wakeup s1_noop s3_detach s1_commit
permutation s1_begin s1_query s2_drop_prop_func s3_wakeup s1_noop s3_detach s1_commit
