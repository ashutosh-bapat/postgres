CREATE SCHEMA create_property_graph_tests;
SET search_path = create_property_graph_tests;

CREATE PROPERTY GRAPH g1;

COMMENT ON PROPERTY GRAPH g1 IS 'a graph';

CREATE PROPERTY GRAPH g1;  -- error: duplicate

CREATE TABLE t1 (a int, b text);
CREATE TABLE t2 (i int PRIMARY KEY, j int, k int);
CREATE TABLE t3 (x int, y text, z text);

CREATE TABLE e1 (a int, i int, t text, PRIMARY KEY (a, i));
CREATE TABLE e2 (a int, x int, t text);

CREATE PROPERTY GRAPH g2
    VERTEX TABLES (t1 KEY (a), t2, t3 KEY (x))
    EDGE TABLES (
        e1 SOURCE t1 DESTINATION t2,
        e2 KEY (a, x) SOURCE t1 DESTINATION t3
    );

-- error cases
CREATE PROPERTY GRAPH gx VERTEX TABLES (xx, yy);
CREATE PROPERTY GRAPH gx VERTEX TABLES (t1 KEY (a), t2 KEY (i), t1 KEY (a));
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (t1 AS tt KEY (a), t2 KEY (i))
    EDGE TABLES (
        e1 SOURCE t1 DESTINATION t2
    );
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (t1 KEY (a), t2 KEY (i))
    EDGE TABLES (
        e1 SOURCE t1 DESTINATION tx
    );
COMMENT ON PROPERTY GRAPH gx IS 'not a graph';

\dG

-- TODO
\d g1
\d+ g1

DROP TABLE g2;  -- error: wrong object type

DROP PROPERTY GRAPH g2;

DROP PROPERTY GRAPH g2;  -- error: does not exist

DROP PROPERTY GRAPH IF EXISTS g2;

DROP SCHEMA create_property_graph_tests CASCADE;
