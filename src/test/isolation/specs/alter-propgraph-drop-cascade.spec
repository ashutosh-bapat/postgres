# Test a cascaded element-table drop concurrent with property graph changes.

setup
{
	CREATE TABLE pgt_drop (a int PRIMARY KEY);
	CREATE TABLE pgt_keep (a int PRIMARY KEY);
	CREATE PROPERTY GRAPH pgg
		VERTEX TABLES (
			pgt_drop LABEL pgl PROPERTIES (a AS p),
			pgt_keep);
}

teardown
{
	DROP PROPERTY GRAPH pgg;
	DROP TABLE IF EXISTS pgt_drop;
	DROP TABLE pgt_keep;
}

session s1
step s1_begin { BEGIN; }
step s1_add_label
{
	ALTER PROPERTY GRAPH pgg ALTER VERTEX TABLE pgt_keep
		ADD LABEL pgl PROPERTIES (a AS p);
}
step s1_commit { COMMIT; }
step s1_rollback { ROLLBACK; }

session s2
step s2_drop { DROP TABLE pgt_drop CASCADE; }
step s2_check
{
	SELECT
		(SELECT count(*)
		 FROM information_schema.pg_labels
		 WHERE property_graph_name = 'pgg' AND label_name = 'pgl') AS labels,
		(SELECT count(*)
		 FROM information_schema.pg_property_data_types
		 WHERE property_graph_name = 'pgg' AND property_name = 'p') AS properties;
}

# Committing ADD LABEL should keep the shared label and property.
permutation s1_begin s1_add_label s2_drop s1_commit s2_check

# Rolling back ADD LABEL should remove the orphaned metadata.
permutation s1_begin s1_add_label s2_drop s1_rollback s2_check

# ADD LABEL after DROP should recreate the label and property on commit.
permutation s1_begin s2_drop s1_add_label s1_commit s2_check

# Rolling back ADD LABEL after DROP should leave the metadata removed.
permutation s1_begin s2_drop s1_add_label s1_rollback s2_check
