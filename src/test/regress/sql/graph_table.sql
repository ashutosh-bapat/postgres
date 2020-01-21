CREATE PROPERTY GRAPH g1;

CREATE PROPERTY GRAPH g1;  -- error: duplicate

CREATE TABLE t1 (a int, b text);
CREATE TABLE t2 (i int, j int, k int);
CREATE TABLE t3 (x int, y text, z text);

CREATE PROPERTY GRAPH g2 VERTEX TABLES (t1, t2);

-- error cases
CREATE PROPERTY GRAPH gx VERTEX TABLES (xx, yy);
CREATE PROPERTY GRAPH gx VERTEX TABLES (t1, t2, t1);

SELECT gt.creation_date, gt.content
FROM g1 GRAPH_TABLE (
  MATCH
    (creator IS person WHERE creator.email = 'foo@example.com')
      -[ IS created ]->
    (m IS message)
      <-[ IS commented ]-
    (commenter IS person WHERE commenter.email = 'bar@example.com')
    WHERE creator.email <> commenter.email
    COLUMNS (m.creation_date, m.content)
) AS gt;

DROP TABLE g2;  -- error: wrong object type

DROP PROPERTY GRAPH g2;

DROP PROPERTY GRAPH g2;  -- error: does not exist

DROP PROPERTY GRAPH IF EXISTS g2;
