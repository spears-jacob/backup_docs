SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT
  visit__screen_resolution AS resolution,
  visit__device__operating_system AS operating_system,
  SIZE(COLLECT_SET(visit__visit_id)) AS unique_visits,
  SIZE(COLLECT_SET(visit__account__account_number)) AS unique_accounts
FROM prod.venona_events
WHERE
  partition_date_hour_utc >= '2017-04-01_06' AND partition_date_hour_utc < '2017-05-01_06'
  AND visit__application_details__application_type = 'OVP'
  AND ((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat')
GROUP BY
visit__screen_resolution,
visit__device__operating_system;