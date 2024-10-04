USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE quantum_metric_agg_portals PARTITION (denver_date)
SELECT
  CASE
  WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
  WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
  WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
  WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
        OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
  ELSE visit__account__details__mso
  END AS mso,
  visit__application_details__application_type AS application_type,
  LOWER(visit__device__device_type) AS device_type,
  visit__connection__connection_type AS connection_type,
  visit__connection__network_status AS network_status,
  state__content__stream__playback_type AS playback_type,
  "cust_type" as cust_type,
  "stream_subtype" AS stream_subtype,
  IF(visit__application_details__application_type = 'Android', IF(visit__device__operating_system RLIKE '^(Android 4.0)' OR visit__device__operating_system RLIKE '^(Android 4.1)' OR visit__device__operating_system RLIKE '^(Android 4.2)' OR visit__device__operating_system RLIKE '^(Android 4.3)', 'Android 4.1 - 4.3', 'Android 4.4+'), NULL) AS android_type,
  CASE
    WHEN visit__application_details__application_type IN('iOS','Android') THEN 'Mobile'
    WHEN visit__application_details__application_type IN('Xbox','SamsungTV') THEN 'Connected Devices'
    ELSE visit__application_details__application_type
  END AS application_group_type,
  state__content__stream__playback_id AS stream_id,
  visit__visit_id AS visit_id,
  visit__device__enc_uuid AS device_id,
  visit__account__enc_account_number AS acct_id,
  NULL AS unique_stream_id,
  NULL AS unique_visit_id,
  NULL AS unique_device_id,
  NULL AS unique_account_number,
  visit__variant_uuids AS variant_uuids,
  state__content__details__episode_number AS episode_number,
  state__content__details__season_number AS season_number,
  state__content__details__genres AS genres,
  state__content__programmer__linear__channel_name AS linear_channel_name,
  visit__device__browser__name AS browser_name,
  visit__device__browser__user_agent AS browser_user_agent,
  visit__device__browser__version AS browser_version,
  state__content__identifiers__tms_program_id AS tms_program_id,

  MIN(received__timestamp) AS min_received_timestamp,
  MAX(received__timestamp) AS max_received_timestamp,

  SUM(IF(message__name = 'loginStop' AND operation__operation_type = 'manualAuth' AND operation__success = TRUE
    AND (visit__application_details__application_type != 'OVP' OR (visit__application_details__application_type = 'OVP' AND visit__application_details__app_version >= '6.1')), 1, 0)) AS manual_login_success,
  SUM(IF(message__name = 'loginStart' AND operation__operation_type = 'manualAuth'
    AND (visit__application_details__application_type != 'OVP' OR (visit__application_details__application_type = 'OVP' AND visit__application_details__app_version >= '6.1')), 1, 0)) AS manual_login_attempt,
  SUM(IF(message__name = 'loginStop' AND operation__operation_type = 'resumeAuth' AND operation__success = TRUE
    AND (visit__application_details__application_type != 'OVP' OR (visit__application_details__application_type = 'OVP' AND visit__application_details__app_version >= '6.1')), 1, 0)) AS resume_login_success,
  SUM(IF(message__name = 'loginStart' AND operation__operation_type = 'resumeAuth'
    AND (visit__application_details__application_type != 'OVP' OR (visit__application_details__application_type = 'OVP' AND visit__application_details__app_version >= '6.1')), 1, 0)) AS resume_login_attempt,
  SUM(IF(message__name = 'loginStop' AND operation__operation_type = 'verifierAuth' AND operation__success = TRUE
    AND (visit__application_details__application_type != 'OVP' OR (visit__application_details__application_type = 'OVP' AND visit__application_details__app_version >= '6.1')), 1, 0)) AS verifier_login_success,
  SUM(IF(message__name = 'loginStart' AND operation__operation_type = 'verifierAuth'
    AND (visit__application_details__application_type != 'OVP' OR (visit__application_details__application_type = 'OVP' AND visit__application_details__app_version >= '6.1')), 1, 0)) AS verifier_login_attempt,

  COLLECT_LIST(IF(message__name = 'loginStop' AND operation__operation_type = 'manualAuth'
    AND operation__success = TRUE, IF(visit__login__login_duration_ms > 15000, 15000, if(visit__login__login_duration_ms > 0,visit__login__login_duration_ms,0)), NULL)) AS manual_duration_ms_list,
  COLLECT_LIST(IF(message__name = 'loginStop' AND operation__operation_type = 'verifierAuth'
    AND operation__success = TRUE, IF(visit__login__login_duration_ms > 15000, 15000, if(visit__login__login_duration_ms > 0,visit__login__login_duration_ms,0)), NULL)) AS verifier_duration_ms_list,
  NULL AS token_exchange_attempts,
  NULL AS stream_init_failures,
  NULL AS tune_time_ms_sum,
  NULL AS tune_time_count,
  COLLECT_SET(0) AS tune_time_ms_list,
  NULL AS watch_time_ms,
  NULL AS buffering_time_ms,
  NULL AS bitrate_downshifts,
  NULL AS stream_init_starts,
  NULL AS stream_noninit_failures,
  NULL AS bandwidth_consumed_mb,
  NULL AS bitrate_content_elapsed_ms,
  NULL AS api_search_error_count,
  NULL AS keyword_search,
  NULL AS predictive_search,
  NULL AS search_closed,
  NULL AS search_entered,
  NULL AS search_entered_space,
  NULL AS search_entered_total,
  COLLECT_SET(0) AS search_response_time_ms_list,
  NULL AS search_result_selected,
  COLLECT_SET('') AS search_result_type_list,
  NULL AS search_started,
  NULL AS search_text_length,
  NULL AS zero_results,
  NULL AS zero_results_space,
  visit__account__enc_account_billing_division AS billing_division,
  visit__account__enc_account_billing_id AS billing_id,
  visit__location__enc_city AS location_city,
  visit__location__state AS location_state,
  visit__location__enc_zip_code AS location_zipcode,
  visit__application_details__app_version AS app_version,
  COLLECT_SET(0) AS complete_buffering_time_ms_list,
  COLLECT_SET(0) AS complete_manual_duration_ms_list,
  COLLECT_SET(0) AS complete_tune_time_ms_list,
  COLLECT_SET(0) AS complete_verifier_duration_ms_list,
  COLLECT_SET(0) AS complete_watch_time_ms,
  NULL AS nonactive_account_number,
  NULL AS nonactive_device_id,
  NULL AS nonactive_stream_id,
  NULL AS nonactive_visit_id,
  visit__device__model as device_model,
  NULL AS critical_api_attempts,
  NULL AS critical_api_fails,
  NULL AS application_errors,
  NULL AS critical_api_success,
  NULL AS other_api_response_count,
  'stream_choice' AS stream_choice,
  state__content__content_class AS content_class,
  visit__account__enc_account_billing_division_id AS billing_division_id,
  CONCAT(
  visit__account__enc_account_billing_division_id ,
  visit__account__enc_account_billing_division ,
  visit__account__enc_account_billing_id
  ) AS billing_combo_key,
  IF(LOWER(visit__application_details__application_name) IN('oneapp','specu','bulkmdu'), 'IPTV', LOWER(visit__application_details__application_name)) AS app_vertical,
  LOWER(visit__application_details__application_name) AS application_name,
  visit__login__logged_in AS logged_in,
  visit__device__operating_system AS operating_system,
  visit__device__device_form_factor AS form_factor,
  visit__account__details__billing_status  AS billing_status,
  visit__account__details__auto_pay_status AS autopay_status,
