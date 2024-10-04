USE ${env:ENVIRONMENT};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;

INSERT OVERWRITE TABLE asp_page_set_pathing_agg partition(denver_date)
SELECT
  CASE WHEN (grouping_id & 2) != 0 THEN current_page_name   ELSE 'All Current Pages'  END AS current_page_name,
  CASE WHEN (grouping_id & 4) != 0 THEN previous_page_name  ELSE 'All Previous Pages' END AS previous_page_name,
  CASE WHEN (grouping_id & 8) != 0 THEN mso                 ELSE 'All Companies'      END AS mso,
  CASE WHEN (grouping_id & 16)!= 0 THEN application_name    ELSE 'All Applications'   END AS application_name,
  'All Select Actions'                                      AS select_action_types_value,
  grouping_id,
  metric_name,
  metric_value,
  denver_date
FROM (
      SELECT
        mso,
        application_name,
        current_page_name,
        previous_page_name,
        grouping_id,
        denver_date,
        MAP(
            'avg_previous_page_viewed_time_sec', avg_previous_page_viewed_time_ms/1000,
            'median_previous_page_viewed_time_sec', previous_page_viewed_time_ms_array[0]/1000,
            'previous_page_viewed_time_sec_95th', previous_page_viewed_time_ms_array[1]/1000,
            'previous_page_viewed_time_sec_99th', previous_page_viewed_time_ms_array[2]/1000,
            'previous_page_viewed_time_sec_max', max_previous_page_viewed_time_ms/1000,
            'previous_page_viewed_time_sec_min', min_previous_page_viewed_time_ms/1000,
            'page_views', page_views
           )                                                   AS tmp_map
      FROM (SELECT
              mso,
              application_name,
              current_page_name,
              COALESCE(previous_page_name, 'No Previous Page') AS previous_page_name,
              --previous_page_viewed_time_ms metrics
              AVG(previous_page_viewed_time_ms)                AS avg_previous_page_viewed_time_ms,
              MAX(previous_page_viewed_time_ms)                AS max_previous_page_viewed_time_ms,
              MIN(IF(previous_page_viewed_time_ms > 0, previous_page_viewed_time_ms, NULL)) AS min_previous_page_viewed_time_ms,
              PERCENTILE(IF(previous_page_viewed_time_ms > 0, CAST(previous_page_viewed_time_ms AS BIGINT), NULL), array(0.5,0.95, 0.99)) AS previous_page_viewed_time_ms_array,

              SUM(page_view_cnts)                              AS page_views,
              CAST(grouping__id AS INT)                        AS grouping_id,
              denver_date
      FROM (SELECT
              visit_id,
              mso,
              application_name,
              current_page_name,
              previous_page_name,
              MAX(previous_page_viewed_time_ms)               AS previous_page_viewed_time_ms,
              MAX(page_view_instances)                        AS page_view_cnts,
              denver_date
             FROM asp_page_agg
            WHERE denver_date         >= '${env:START_DATE}'
              AND denver_date         <  '${env:END_DATE}'
            GROUP BY visit_id,
                    mso,
                    application_name,
                    current_page_name,
                    previous_page_name,
                    denver_date
              ) A
      GROUP BY denver_date,
               current_page_name,
               previous_page_name,
               mso,
               application_name,
               'All Select Actions'
      GROUPING SETS (
                      (denver_date, current_page_name, application_name),
                      (denver_date, previous_page_name, application_name),
                      (denver_date, current_page_name, previous_page_name, application_name))
          ) B
    ) C
LATERAL VIEW EXPLODE(tmp_map) explode_table                    AS metric_name, metric_value

UNION

SELECT
  CASE WHEN (grouping_id & 2) != 0 THEN current_page_name      ELSE 'All Current Pages'  END AS current_page_name,
  CASE WHEN (grouping_id & 4) != 0 THEN previous_page_name     ELSE 'All Previous Pages' END AS previous_page_name,
  CASE WHEN (grouping_id & 8) != 0 THEN mso                    ELSE 'All Companies'      END AS mso,
  CASE WHEN (grouping_id & 16)!= 0 THEN application_name       ELSE 'All Applications'   END AS application_name,
  CASE WHEN (grouping_id & 32)!= 0 THEN select_action_types_value    ELSE 'All Select Actions'   END AS select_action_types_value,
  grouping_id,
  metric_name,
  metric_value,
  denver_date
  FROM (
      SELECT mso,
            application_name,
            current_page_name,
            COALESCE(previous_page_name, 'No Previous Page')     AS previous_page_name,
            select_action_types_value,
            MAP('select_action_type', SUM(select_action_count))  AS tmp_map,
            CAST(grouping__id AS INT)                            AS grouping_id,
            denver_date
      FROM (
            SELECT mso,
                   application_name,
                   current_page_name,
                   previous_page_name,
                   COALESCE(standardized_name, 'No Name Value')      AS select_action_types_value,
                   1                                                 AS select_action_count,
                   denver_date
              FROM asp_page_agg
             WHERE denver_date                  >= '${env:START_DATE}'
               AND denver_date                  <  '${env:END_DATE}'
               AND select_action_instances      > 0
          )      A
        GROUP BY denver_date,
                 current_page_name,
                 previous_page_name,
                 mso,
                 application_name,
                 select_action_types_value
        GROUPING SETS (
--                     (denver_date, current_page_name, select_action_types_value),
                       (denver_date, current_page_name, application_name, select_action_types_value)
                      )
    ) B
LATERAL VIEW EXPLODE(tmp_map) explode_table           AS metric_name, metric_value;
