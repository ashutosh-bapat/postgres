CREATE PROPERTY GRAPH my_graph;

CREATE PROPERTY GRAPH my_graph;  -- error: duplicate

CREATE PROPERTY GRAPH my_graph2;

SELECT gt.creation_date, gt.content
FROM my_graph GRAPH_TABLE (
  MATCH
    (creator IS person WHERE creator.email = 'foo@example.com')
      -[ IS created ]->
    (m IS message)
      <-[ IS commented ]-
    (commenter IS person WHERE commenter.email = 'bar@example.com')
    WHERE creator.email <> commenter.email
    COLUMNS (m.creation_date, m.content)
) AS gt;

DROP TABLE my_graph2;  -- error: wrong object type

DROP PROPERTY GRAPH my_graph2;

DROP PROPERTY GRAPH my_graph2;  -- error: does not exist

DROP PROPERTY GRAPH IF EXISTS my_graph2;
