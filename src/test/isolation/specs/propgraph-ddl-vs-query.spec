# Test ALTER PROPERTY GRAPH concurrent with a running GRAPH_TABLE query.

setup
{
	CREATE TABLE pgq_v1 (a int PRIMARY KEY);
	CREATE TABLE pgq_v2 (a int PRIMARY KEY);
	INSERT INTO pgq_v1 VALUES (1);
	INSERT INTO pgq_v2 VALUES (2);
	CREATE PROPERTY GRAPH pgq
		VERTEX TABLES (pgq_v1 LABEL pgql PROPERTIES (a AS p));
}

teardown
{
	DROP PROPERTY GRAPH IF EXISTS pgq;
	DROP TABLE IF EXISTS pgq_v1, pgq_v2;
}

session s1
step s1_begin { BEGIN; }
step s1_begin_rr { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s1_query
{
	SELECT * FROM GRAPH_TABLE (pgq MATCH (x IS pgql) COLUMNS (x.p)) ORDER BY p;
}
step s1_commit { COMMIT; }

session s2
step s2_begin { BEGIN; }
step s2_add_element
{
	ALTER PROPERTY GRAPH pgq
		ADD VERTEX TABLES (pgq_v2 LABEL pgql PROPERTIES (a AS p));
}
step s2_drop_elemtable { DROP TABLE pgq_v1 CASCADE; }
step s2_drop_column { ALTER TABLE pgq_v1 DROP COLUMN a CASCADE; }
step s2_commit { COMMIT; }

# Querying a property graph blocks a concurrent modification of it until the
# querying transaction ends.
permutation s1_begin s1_query s2_add_element s1_commit

# Modifying a property graph blocks a concurrent query on it until the
# modifying transaction ends.
permutation s2_begin s2_add_element s1_query s2_commit

# Test READ COMMITTED and REPEATABLE READ semantics
permutation s1_begin s1_query s2_add_element s1_query s1_commit
permutation s1_begin_rr s1_query s2_add_element s1_query s1_commit

# Dropping an element table cascades into the graph, so it must block a
# concurrent query on the graph and vice versa just like ALTER PROPERTY GRAPH
# does.
permutation s1_begin s1_query s2_drop_elemtable s1_commit
permutation s2_begin s2_drop_elemtable s1_query s2_commit

# Same for dropping a column a property is defined on.
permutation s1_begin s1_query s2_drop_column s1_commit
permutation s2_begin s2_drop_column s1_query s2_commit
