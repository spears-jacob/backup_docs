USE ${env:DASP_db};

SET RollingLag=2;

SET TabView=core_quantum_events_sspp;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.size.per.task=1024000000;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.tezfiles=true;
SET hive.vectorized.execution.enabled=false;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET orc.force.positional.evolution=true;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_bounces_entries PARTITION (domain, date_denver)
SELECT page_name,
       SUM(IF(received__timestamp = min_message_time AND message__sequence_number = min_seq, 1, 0)) AS entries,
       SUM(IF(view_count = 1 AND action_count = 0 AND message__name = 'pageView', 1, 0))            AS bounces,
       CASE
    WHEN app_name = 'myspectrum' THEN 'app'
    WHEN app_name = 'specnet' THEN 'resi'
    WHEN app_name =  'smb' THEN 'smb'
    ELSE 'UNKNOWN'
  ENd AS domain,
  min_denver_date as date_denver
FROM
  (
  SELECT
    LOWER(visit__application_details__application_name) AS app_name,
    message__name,
    message__sequence_number,
    received__timestamp,
    state__view__current_page__page_name AS page_name,
    MIN(epoch_converter(received__timestamp, 'America/Denver')) OVER visit_win AS min_denver_date,
    MIN(IF(message__name = 'pageView', received__timestamp, NULL)) OVER visit_win AS min_message_time,
    MIN(IF(message__name = 'pageView', message__sequence_number, NULL)) OVER visit_win AS min_seq,
    SUM(IF(message__name = 'pageView', 1, 0)) OVER visit_win AS view_count,
    SUM(IF((message__name IN('modalCancel','modalView','searchClosed','searched')
        OR(message__name = 'selectAction' AND state__view__current_page__elements__standardized_name != 'page-level-alert-link-click')) , 1, 0)) OVER visit_win AS action_count
  FROM
    ${env:ENVIRONMENT}.${hiveconf:TabView}
  WHERE
    partition_date_hour_utc >= CONCAT(DATE_SUB('${hiveconf:START_DATE}',${hiveconf:RollingLag}),'_00')
    AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}'
    AND visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement','PrivacyMicrosite')
  WINDOW
    visit_win AS (PARTITION BY visit__visit_id)
  ) q_win
WHERE
  min_denver_date >= '${hiveconf:START_DATE}'
GROUP BY
  app_name,
  page_name,
  min_denver_date
HAVING
  SUM(IF(received__timestamp = min_message_time AND message__sequence_number = min_seq, 1, 0)) > 0
  OR SUM(IF(view_count = 1 AND action_count = 0 AND message__name = 'pageView', 1, 0)) > 0
;
