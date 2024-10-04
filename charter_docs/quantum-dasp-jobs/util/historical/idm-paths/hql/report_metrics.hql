USE ${env:DASP_db};

set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_idm_paths_metrics PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      '' as api_code,
      '' as api_text,
      'pageview' as metric_name,
      count(1) as metric_value,
      epoch_converter(received__timestamp, 'America/Denver') as date_denver
FROM ${env:TMP_db}.temp_idm_paths_core_quantum_events
WHERE message__name ='pageView'
group by
      epoch_converter(received__timestamp, 'America/Denver'),
      visit__application_details__application_name,
      parse_url(visit__application_details__referrer_link,'HOST'),
      visit__device__browser__name,
      visit__device__device_type,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name;

INSERT INTO TABLE ${env:DASP_db}.asp_idm_paths_metrics PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      '' as api_code,
      '' as api_text,
      'buttonClick' as metric_name,
      count(1) as metric_value,
      epoch_converter(received__timestamp, 'America/Denver') as date_denver
FROM ${env:TMP_db}.temp_idm_paths_core_quantum_events
WHERE message__name ='selectAction'
  AND operation__operation_type = 'buttonClick'
group by
      epoch_converter(received__timestamp, 'America/Denver'),
      visit__application_details__application_name,
      parse_url(visit__application_details__referrer_link,'HOST'),
      visit__device__browser__name,
      visit__device__device_type,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name
;

INSERT INTO TABLE ${env:DASP_db}.asp_idm_paths_metrics PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      application__api__response_code as api_code,
      application__api__response_text as api_text,
      'api_response' as metric_name,
      count(1) as metric_value,
      epoch_converter(received__timestamp, 'America/Denver') as date_denver
FROM ${env:TMP_db}.temp_idm_paths_core_quantum_events
WHERE application__api__response_code != '200'
group by
      epoch_converter(received__timestamp, 'America/Denver'),
      visit__application_details__application_name,
      parse_url(visit__application_details__referrer_link,'HOST'),
      visit__device__browser__name,
      visit__device__device_type,
      application__api__response_code,
      application__api__response_text,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name
;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.temp_idm_paths_core_quantum_events PURGE;
