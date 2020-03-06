CREATE SCHEMA create_property_graph_tests;
SET search_path = create_property_graph_tests;

CREATE PROPERTY GRAPH g1;

CREATE PROPERTY GRAPH g1;  -- error: duplicate

CREATE TABLE t1 (a int, b text);
CREATE TABLE t2 (i int, j int, k int);
CREATE TABLE t3 (x int, y text, z text);

CREATE PROPERTY GRAPH g2 VERTEX TABLES (t1, t2);

-- error cases
CREATE PROPERTY GRAPH gx VERTEX TABLES (xx, yy);
CREATE PROPERTY GRAPH gx VERTEX TABLES (t1, t2, t1);


DROP TABLE g2;  -- error: wrong object type

DROP PROPERTY GRAPH g2;

DROP PROPERTY GRAPH g2;  -- error: does not exist

DROP PROPERTY GRAPH IF EXISTS g2;

DROP SCHEMA create_property_graph_tests CASCADE;
