USE ${env:DASP_db};

set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_idm_paths_time PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__previous_page__page_name as pagename,
      state__view__previous_page__page_viewed_time_ms/1000 as page_viewed_time_sec,
      epoch_converter(received__timestamp, 'America/Denver') as date_denver
FROM ${env:TMP_db}.temp_idm_paths_core_quantum_events
WHERE message__name ='pageView'
  AND state__view__previous_page__page_name is not null
  AND state__view__previous_page__page_viewed_time_ms > 0
;
