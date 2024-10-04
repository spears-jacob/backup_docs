USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

SELECT "\n\nFor: referring_traffic \n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_referring_traffic_tmp PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_referring_traffic_tmp AS
SELECT
    CAST(grouping__id AS INT) AS grouping_id
  , COUNT(*) AS count_of_platform_type_fixed
  , platform_type_fixed
  , app_name
  , page_title
FROM
    (SELECT
          message__event_case_id msg_case_id
        , CASE
          WHEN visit__application_details__platform_type IS NOT NULL THEN visit__application_details__platform_type
          WHEN visit__device__browser__user_agent LIKE '%iPhone%' OR visit__device__browser__user_agent LIKE '%Android%' THEN 'mobile'
          WHEN visit__device__browser__user_agent LIKE '%Windows%' OR visit__device__browser__user_agent LIKE '%Mac%' THEN 'web'
          WHEN visit__device__browser__user_agent IS NULL THEN 'Unknown'
          ELSE 'other' END AS platform_type_fixed
        , visit__application_details__application_name app_name
        , COALESCE(state__view__current_page__page_title, 'NA') page_title
      FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
      WHERE ( partition_date_utc  >= '${hiveconf:START_DATE}' AND partition_date_utc < '${hiveconf:END_DATE}')
        AND visit__application_details__application_name IN ('SMB', 'SpecNet', 'MySpectrum')
        AND message__event_case_id LIKE '%_pageView_support_supportArticle'
    ) subset
GROUP BY
  platform_type_fixed
  , app_name
  , page_title
GROUPING SETS (
  (platform_type_fixed), --3
  (platform_type_fixed, app_name), --1
  (platform_type_fixed, app_name, page_title), --0
  (app_name) --5
)
;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_monthly_referring_traffic partition (label_date_denver)
SELECT
    count_of_platform_type_fixed as pageview_count
  , CASE
      WHEN grouping_id=5 THEN '<All Platforms>'
      ELSE platform_type_fixed END AS platform_type
  , CASE
        WHEN grouping_id=3 THEN '<All Apps>'
        ELSE app_name END AS app_name
  , CASE
        WHEN grouping_id=0 THEN page_title
        ELSE '<All Pages>' END AS page_title
  , CASE
      WHEN grouping_id=1 THEN 1
      WHEN grouping_id=0 THEN 0
      ELSE 2 END AS rollup_
  , '${hiveconf:label_date_denver}' as label_date_denver
FROM ${env:TMP_db}.asp_monthly_referring_traffic_tmp
;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_referring_traffic_tmp PURGE;
