USE ${env:ENVIRONMENT};

SET RollingLag=8;

SET TabView=asp_v_venona_events_portals;

-----------------------------------------------------
--Populate base table, all pages in all application types
-----------------------------------------------------

INSERT OVERWRITE TABLE asp_bounces_entries PARTITION(domain,date_denver)
SELECT
  page_name,
  SUM(IF(received__timestamp = min_message_time AND message__sequence_number = min_seq, 1, 0)) AS entries,
  SUM(IF(view_count = 1 AND action_count = 0 AND message__name = 'pageView', 1, 0)) AS bounces,
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
    MIN(${env:ENVIRONMENT}.epoch_converter(received__timestamp, 'America/Denver')) OVER visit_win AS min_denver_date,
    MIN(IF(message__name = 'pageView', received__timestamp, NULL)) OVER visit_win AS min_message_time,
    MIN(IF(message__name = 'pageView', message__sequence_number, NULL)) OVER visit_win AS min_seq,
    SUM(IF(message__name = 'pageView', 1, 0)) OVER visit_win AS view_count,
    SUM(IF((message__name IN('modalCancel','modalView','searchClosed','searched')
        OR(message__name = 'selectAction' AND state__view__current_page__elements__standardized_name != 'page-level-alert-link-click')) , 1, 0)) OVER visit_win AS action_count
  FROM
    ${hiveconf:TabView}
  WHERE
    partition_date_hour_utc >= CONCAT(DATE_SUB('${env:START_DATE}',${hiveconf:RollingLag}),'_00')
    AND partition_date_hour_utc < '${env:END_DATE_TZ}'
  WINDOW
    visit_win AS (PARTITION BY visit__visit_id)
  ) q_win
WHERE
  min_denver_date >= '${env:START_DATE}'
GROUP BY
  app_name,
  page_name,
  min_denver_date
HAVING
  SUM(IF(received__timestamp = min_message_time AND message__sequence_number = min_seq, 1, 0)) > 0
  OR SUM(IF(view_count = 1 AND action_count = 0 AND message__name = 'pageView', 1, 0)) > 0
;
