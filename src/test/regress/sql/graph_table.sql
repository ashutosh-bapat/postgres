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
