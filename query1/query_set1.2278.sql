SELECT Coalesce(a.level1, b.level1)                         AS level1,         a.gnuramv, a.cob_date,        Coalesce(a.gnuramv, 0) - Coalesce(b.gnuramv, 0)      AS diff_gnuramv FROM   (SELECT aggregation_detail.level1, aggregation_detail.cob_date,                VALUE AS gnuramv          FROM   CDWUSER.U_AGGREGATION_DETAIL  Aggregation_Detail,                 CDWUSER.U_AGGREGATION_SCHEMA  Aggregation_Schema         WHERE  ( aggregation_detail.cob_date =  ('2018-02-28') AND                  aggregation_detail.cob_date = aggregation_schema.cob_date )                 AND ( aggregation_detail.hierarchy_id = 5                       AND aggregation_detail.hierarchy_id =                           aggregation_schema.hierarchy_id )                 AND ( aggregation_detail.aggregation_name = 'EQ_GNURAM'                       AND aggregation_detail.aggregation_name =                           aggregation_schema.aggregation_name )                 AND ( aggregation_detail.version_id =                       aggregation_schema.version_id )                 AND ( aggregation_schema.is_latest = 1 )                 AND ( aggregation_detail.level2 IS NULL )                 AND ( aggregation_detail.level1 IS NOT NULL )                ) a         FULL OUTER JOIN (SELECT aggregation_detail.level1, aggregation_detail.cob_date,                                VALUE AS gnuramv                          FROM   CDWUSER.U_AGGREGATION_DETAIL  Aggregation_Detail,                                 CDWUSER.U_AGGREGATION_SCHEMA  Aggregation_Schema         WHERE  ( aggregation_detail.cob_date =  ('2018-02-27') AND                                   aggregation_detail.cob_date =                                       aggregation_schema.cob_date )                                 AND ( aggregation_detail.hierarchy_id = 5                                       AND aggregation_detail.hierarchy_id =                                           aggregation_schema.hierarchy_id )                                 AND (                         aggregation_detail.aggregation_name = 'EQ_GNURAM'                         AND aggregation_detail.aggregation_name =                             aggregation_schema.aggregation_name )                                 AND ( aggregation_detail.version_id =                                       aggregation_schema.version_id )                                 AND ( aggregation_schema.is_latest = 1 )                                 AND ( aggregation_detail.level2 IS NULL )                                 AND aggregation_detail.level1 IS NOT NULL                                 ) b                      ON a.level1 = b.level1