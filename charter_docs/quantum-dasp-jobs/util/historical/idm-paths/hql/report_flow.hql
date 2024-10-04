USE ${env:DASP_db};

set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

DROP TABLE IF EXISTS ${env:TMP_db}.temp_idm_paths_core_quantum_events;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.temp_idm_paths_core_quantum_events
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION 'hdfs:///tmp/tmp_idm_path/temp_idm_paths_core_quantum_events'
    TBLPROPERTIES ('orc.compress' = 'snappy') AS
SELECT *
FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
WHERE (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}' and partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
  AND visit__application_details__application_name = 'IDManagement';

DROP TABLE IF EXISTS ${env:TMP_db}.asp_idm_paths_flow1 ;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_idm_paths_flow1
 ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION 'hdfs:///tmp/tmp_idm_path/flow1'
 TBLPROPERTIES('orc.compress'='snappy') AS
select
      epoch_converter(received__timestamp, 'America/Denver') as date_denver,
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      visit__visit_id as visit_id,
      message__sequence_number as visit_order,
      max(message__sequence_number) over(partition by visit__visit_id) as visit_max,
      case
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'confirmYourAccount'
                  and state__view__current_page__app_section='primaryCreate'
                 then 'createStart'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'enterYourPassword'
                  and state__view__current_page__app_section='primaryCreate'
                  and state__view__current_page__elements__standardized_name='enterYourPasswordSaveAndSignIn'
                  and operation__operation_type='buttonClick'
                 then 'createEnterPWSuccess'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'enterYourPassword'
                  and state__view__current_page__app_section='primaryCreate'
                 then 'createEnterPWFailure'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'verificationCode'
                  and state__view__current_page__app_section='primaryCreate'
                 then 'createUNFailure'
           when message__name = 'pageView'
                       AND state__view__current_page__page_name = 'checkYourInfo'
                       and state__view__current_page__app_section='subCreate'
                 then 'createSubUserStart'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'enterYourPassword'
                  and state__view__current_page__app_section='subCreate'
                  and state__view__current_page__elements__standardized_name='enterYourPasswordSaveAndSignIn'
                  and operation__operation_type='buttonClick'
                 then 'createSubUserEnterPWSuccess'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'enterYourPassword'
                  and state__view__current_page__app_section='subCreate'
                 then 'createSubUserEnterPWFailure'
           when message__name = 'modalView'
                  AND state__view__modal__name = 'verifyYourIdentityModal'
                  and state__view__modal__modal_type='message'
                 then 'carePropmtPage'
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'accessUnavailableBlock'
                  and state__view__current_page__app_section='primaryCreate'
                 then 'createRoadBlock'
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'accessUnavailableBlock'
                  and state__view__current_page__app_section='subCreate'
                 then 'createSubUserRoadBlock'
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'confirmYourAccount'
                  and state__view__current_page__app_section='recover'
                 then 'recoverStart'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'username'
                  and state__view__current_page__app_section='recover'
                  and state__view__current_page__elements__standardized_name='userNameSignIn'
                  and operation__operation_type='buttonClick'
                 then 'recoverUNSuccess'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'verificationCode'
                  and state__view__current_page__app_section='recover'
                 then 'recoverUNFailure'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'resetYourPassword'
                  and state__view__current_page__app_section='recover'
                  and state__view__current_page__elements__standardized_name='resetYourPasswordSaveAndSignIn'
                  and operation__operation_type='buttonClick'
                 then 'recoverResetPWSuccess'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'resetYourPassword'
                  and state__view__current_page__app_section='recover'
                 then 'recoverResetPWFailure'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'enterYourPassword'
                  and state__view__current_page__app_section='recover'
                  and state__view__current_page__elements__standardized_name='enterYourPasswordSaveAndSignIn'
                  and operation__operation_type='buttonClick'
                 then 'recoverEnterPWSuccess'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'enterYourPassword'
                  and state__view__current_page__app_section='recover'
                 then 'recoverEnterPWFailure'
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'accessUnavailableBlock'
                  and state__view__current_page__app_section='recover'
                 then 'recoverRoadBlock'
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'resetYourPassword'
                  and state__view__current_page__app_section='forcedPasswordReset'
                 then 'blackListStart'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'resetYourPassword'
                  and state__view__current_page__app_section='forcedPasswordReset'
                 then 'blackListPWFailure'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'resetYourPassword'
                  and state__view__current_page__app_section='forcedPasswordReset'
                  and state__view__current_page__elements__standardized_name='resetYourPasswordSaveAndSignIn'
                  and operation__operation_type='buttonClick'
                 then 'blackListPWSuccess'
           when message__name = 'pageView'
                  AND state__view__current_page__page_name = 'forcedPasswordReset'
                  and state__view__current_page__app_section='forcedPasswordReset'
                 then 'weakPWStart'
           when message__name = 'error'
                  AND state__view__current_page__page_name = 'forcedPasswordReset'
                  and state__view__current_page__app_section='forcedPasswordReset'
                 then 'weakPWFailure'
           when message__name = 'loginStop'
                  AND operation__success=true
                 then 'loginSuccess'
           when message__name = 'selectAction'
                  AND state__view__current_page__page_name = 'username'
                  and state__view__current_page__app_section='recover'
                  and state__view__current_page__elements__standardized_name='usernameResetPassword'
                  and operation__operation_type='buttonClick'
                 then 'recoverUNResetPWSuccess'
           else 'eventOther'
      end as event_type,
      lag(message__name) over(partition by visit__visit_id order by message__sequence_number) as prev_message_name,
      message__name as message_name,
      lEAD(message__name) over(partition by visit__visit_id order by message__sequence_number) as next_message_name,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      state__view__modal__name as modal_name,
      state__view__modal__modal_type AS modal_type,
      message__category as message_category,
      message__triggered_by as message_triggered_by,
      operation__operation_type as operation_type,
      application__api__response_code as api_code,
      application__api__response_text as api_text,
      first_value(application__api__response_code)
         over(partition by visit__visit_id order by message__sequence_number desc) as last_api_code,
      first_value(application__api__response_text)
         over(partition by visit__visit_id order by message__sequence_number desc) as last_api_text,
      first_value(state__view__current_page__page_name)
         over(partition by visit__visit_id order by message__sequence_number desc) as last_pagename,
      first_value(state__view__current_page__app_section)
         over(partition by visit__visit_id order by message__sequence_number desc) as last_app_section,
      state__view__previous_page__page_viewed_time_ms as prev_page_viewed_time_ms
FROM ${env:TMP_db}.temp_idm_paths_core_quantum_events
Order by visit_id, visit_order;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_idm_paths_flow2;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_idm_paths_flow2
 ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION 'hdfs:///tmp/tmp_idm_path/flow2'
 TBLPROPERTIES('orc.compress'='snappy') AS
SELECT case when last_event_tmp ='loginSuccess' and last_event_minus_2 in ('createEnterPWSuccess','createSubUserEnterPWSuccess','recoverResetPWSuccess','recoverEnterPWSuccess','blackListPWSuccess') then last_event_minus_2
            when last_event_tmp ='loginSuccess' and last_event_minus_1 in ('createEnterPWSuccess','createSubUserEnterPWSuccess','recoverResetPWSuccess','recoverEnterPWSuccess','blackListPWSuccess') then last_event_minus_1
            when last_event_tmp !='loginSuccess' THEN last_event_tmp
            else 'eventOther'
       end as last_event,
       *
FROM
     (select first_value(event_type) over(partition by visit_id order by visit_order) as first_event,
             first_value(event_type) over(partition by visit_id order by visit_order desc) as last_event_tmp,
             first_value(event_minus_1) over(partition by visit_id order by visit_order desc) as last_event_minus_1,
             first_value(event_minus_2) over(partition by visit_id order by visit_order desc) as last_event_minus_2,
             first_value(message_name) over(partition by visit_id order by visit_order desc) as last_msg_name,
             first_value(prev_message_name) over(partition by visit_id order by visit_order) as msg_before_first_event,
             first_value(next_message_name) over(partition by visit_id order by visit_order desc) as msg_after_last_event,
             first_value(visit_order) over(partition by visit_id order by visit_order desc) as last_order,
             *
      FROM
            (SELECT
                   lag(event_type) over(partition by visit_id order by visit_order) as event_minus_1,
                   lag(event_type,2) over(partition by visit_id order by visit_order) as event_minus_2,
                   *
            FROM ${env:TMP_db}.asp_idm_paths_flow1
            WHERE message_name != 'startSession'
              AND message_name !='apiCall'
              AND message_name !='pageViewPerformance'
            order by visit_id, visit_order) a) b;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_idm_paths_flow PARTITION(date_denver)
select
        platform,
        referrer_link,
        browser_name,
        device_type,
        visit_id,
        max(last_pagename) as last_pagename,
        max(last_app_section) as last_app_section,
        max(last_api_code) as last_api_code,
        max(last_api_text) as last_api_text,
        max(first_event) as first_event, --excluded apicall and pageViewPerformance
        max(last_event) as last_event,   --excluded apicall and pageViewPerformance
        max(last_msg_name) as last_msg_name,  --excluded apicall and pageViewPerformance
        max(msg_before_first_event) as msg_before_first_event, --excluded apicall and pageViewPerformance
        MAX(msg_after_last_event) as msg_after_last_event, --excluded apicall and pageViewPerformance
        mAX(last_order) as last_order, --excluded apicall and pageViewPerformance
        mAX(visit_max) as visit_max,
        min(date_denver) as date_denver
  FROM ${env:TMP_db}.asp_idm_paths_flow2
  group bY platform,
           referrer_link,
           browser_name,
           device_type,
           visit_id
;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_idm_paths_flow1 ;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_idm_paths_flow2 ;
