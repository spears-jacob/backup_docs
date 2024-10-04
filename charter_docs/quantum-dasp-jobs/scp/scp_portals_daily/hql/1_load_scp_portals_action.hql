USE ${env:DASP_db};


SET hive.vectorized.execution.enabled=false;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.size.per.task=2048000000;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET hivevar:case_ids_arr=array('mySpectrum_selectAction_equipment_deviceManagement_pauseActionSheet_cancel', 'mySpectrum_selectAction_equipment_deviceManagement_pauseActionSheet', 'mySpectrum_selectAction_equipment_deviceManagement_pauseDeviceCancel', 'mySpectrum_selectAction_equipment_deviceManagement_pauseDevice', 'mySpectrum_selectAction_equipment_deviceManagement_pauseDeviceConfirm', 'mySpectrum_applicationActivity_equipment_deviceManagement_pauseActionSheet', 'mySpectrum_modalView_equipment_pauseDevice', 'mySpectrum_modalView_equipment_unpauseDevice', 'mySpectrum_selectAction_equipment_deviceManagement_unpauseDevice', 'mySpectrum_selectAction_equipment_deviceManagement_unpauseDeviceConfirm', 'mySpectrum_selectAction_equipment_deviceManagement_unpauseDeviceCancel', 'mySpectrum_selectAction_equipment_deviceManagement_scheduledPauseCancel', 'mySpectrum_selectAction_equipment_deviceManagement_createPauseSchedule', 'mySpectrum_selectAction_equipment_deviceManagement_swipedToSchedulePause', 'mySpectrum_selectAction_equipment_deviceManagement_scheduledPauseSave', 'mySpectrum_selectAction_equipment_deviceManagement_pauseActionSheet_createPauseSchedule', 'mySpectrum_selectAction_equipment_deviceManagement_customPauseScheduleToggle', 'mySpectrum_pageView_equipment_editNetwork', 'mySpec_selectAction_home_ssidManageWifiNetworkTile', 'mySpectrum_selectAction_equipment_saveEditWifi', 'mySpectrum_selectAction_equipment_cancelEditWifi', 'mySpectrum_selectAction_equipment_editNetworkInfo','mySpectrum_applicationActivity_generic_finalConfirm','mySpectrum_Generic_API_Call_Failure','mySpectrum_Generic_API_Call_Success','mySpectrum_Generic_Error', 'mySpectrum_applicationActivity_manualPauseDevice_finalConfirm', 'mySpectrum_applicationActivity_manualUnpauseDevice_finalConfirm', 'mySpectrum_applicationActivity_createPauseScheduleSave_finalConfirm', 'mySpectrum_applicationActivity_groupPauseSave_finalConfirm', 'mySpectrum_applicationActivity_groupUnpauseSave_finalConfirm', 'mySpectrum_applicationActivity_groupCreatePauseScheduleSave_finalConfirm', 'mySpectrum_applicationActivity_equipment_selfInstall_status', 'mySpectrum_featureStop_equipment_troubleshootFlowStop_success', 'mySpectrum_selectAction_resetRouter_deleteNetworkSettingsConfirm', 'mySpectrum_pageView_advancedSettings_factoryResetSuccess', 'mySpectrum_pageView_advancedSettings_factoryResetError');

-- prepare temp tables for all-local processing
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_cqe_${env:CLUSTER}_${env:STEP};
CREATE TABLE ${env:TMP_db}.scp_pa_cqe_${env:CLUSTER}_${env:STEP} AS
SELECT
      visit__account__enc_account_number AS acct_number_enc
    , visit__account__enc_system_sys AS sys_enc
    , visit__account__enc_system_prin AS prin_enc
    , visit__account__enc_system_agent AS agent_enc
    , message__event_case_id AS msg_event_case_id
    , state__view__current_page__elements__element_string_value AS cur_page_ele
    , state__view__current_page__page_name AS cur_page_nm
    , application__api__response_code AS app_api_response_code
    , message__timestamp AS msg_ts
    , partition_date_hour_utc AS data_utc_hr
    , application__api__trace_id AS app_api_trace_id
    , operation__toggle_state AS op_toggle_state
    , COALESCE(visit__account__details__service_subscriptions["multiAp"],'unknown') AS msa_multi_ap
    , COALESCE(visit__account__details__service_subscriptions["scp"],'unknown') AS msa_scp_flag
    , application__api__api_cached
    , application__api__api_category
    , application__api__api_name
    , application__api__architecture
    , application__api__client_service_result
    , application__api__data_center
    , application__api__gql__operation_name
    , application__api__gql__operation_type
    , application__api__host
    , application__api__http_verb
    , application__api__max_retry_count
    , application__api__path
    , application__api__reported_headers
    , application__api__response_size
    , application__api__enc_Query_parameters
    , application__api__query_parameters
    , application__api__response_text
    , application__api__response_time_ms
    , application__api__retry
    , application__api__retry_count
    , application__api__service_result
    , application__api__will_retry
    , message__feature__feature_name AS feature_name
    , message__feature__featuretype AS feature_type
    , message__feature__feature_step_name AS feature_step_name
    , CASE
         WHEN (visit__account__details__mso='TWC' or visit__account__details__mso='"TWC"') THEN 'TWC'
         WHEN (visit__account__details__mso= 'BH' or visit__account__details__mso='"BHN"' OR visit__account__details__mso='BHN') THEN 'BHN'
         WHEN (visit__account__details__mso= 'CHARTER' or visit__account__details__mso='"CHTR"' OR visit__account__details__mso='CHTR') THEN 'CHR'
         WHEN (visit__account__details__mso= 'NONE' or visit__account__details__mso='UNKNOWN') THEN NULL
         ELSE visit__account__details__mso
      END AS mso
FROM `${env:CQES}`
WHERE partition_date_utc = '${env:START_DATE}'
    AND visit__application_details__application_name IN ('myspectrum', 'MySpectrum','MYSPECTRUM')
    AND array_contains(${hivevar:case_ids_arr},message__event_case_id)
;

DROP TABLE IF EXISTS ${env:TMP_db}.atom_seq_20190201_${env:CLUSTER}_${env:STEP};
CREATE TABLE         ${env:TMP_db}.atom_seq_20190201_${env:CLUSTER}_${env:STEP} AS
  SELECT
      encrypted_account_key_256
    , encrypted_account_number_256
  FROM `${env:EQUIP}`
  WHERE partition_date_eastern = '${env:START_DATE}'
  GROUP BY
      encrypted_account_key_256
    , encrypted_account_number_256
;

DROP TABLE IF EXISTS  ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP};
CREATE TABLE          ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP} AS
  SELECT
      encrypted_account_key_256
    , CASE WHEN wifi_customer_type IS NULL AND partition_date <='2022-09-28' THEN 'AHW'
           ELSE wifi_customer_type
           END AS wifi_customer_type
    , partition_date
  FROM `${env:BAES}`
  WHERE partition_date = '${env:START_DATE}'
  --AND subscriber_type IN ('reportable_scp_subscriber', 'reportable_scp_smb_subscriber')
  GROUP BY
      encrypted_account_key_256
    , wifi_customer_type
    , partition_date
;

-- final temp table joining CQE to EQUIP table
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_equip_${env:CLUSTER}_${env:STEP};
CREATE TABLE         ${env:TMP_db}.scp_pa_equip_${env:CLUSTER}_${env:STEP} AS
SELECT
      cqe.acct_number_enc
    , cqe.sys_enc
    , cqe.prin_enc
    , cqe.agent_enc
    , cqe.msg_event_case_id
    , cqe.cur_page_ele
    , cqe.cur_page_nm
    , cqe.app_api_response_code
    , cqe.msg_ts
    , cqe.data_utc_hr
    , cqe.app_api_trace_id
    , cqe.op_toggle_state
    , msa_multi_ap
    , msa_scp_flag
    , cqe.application__api__api_cached
    , cqe.application__api__api_category
    , cqe.application__api__api_name
    , cqe.application__api__architecture
    , cqe.application__api__client_service_result
    , cqe.application__api__data_center
    , cqe.application__api__gql__operation_name
    , cqe.application__api__gql__operation_type
    , cqe.application__api__host
    , cqe.application__api__http_verb
    , cqe.application__api__max_retry_count
    , cqe.application__api__path
    , cqe.application__api__reported_headers
    , cqe.application__api__response_size
    , cqe.application__api__enc_Query_parameters
    , cqe.application__api__query_parameters
    , cqe.application__api__response_text
    , cqe.application__api__response_time_ms
    , cqe.application__api__retry
    , cqe.application__api__retry_count
    , cqe.application__api__service_result
    , cqe.application__api__will_retry
    , cqe.feature_name
    , cqe.feature_type
    , cqe.feature_step_name
    , cqe.mso
    , equip.encrypted_account_key_256
FROM  ${env:TMP_db}.scp_pa_cqe_${env:CLUSTER}_${env:STEP} cqe
LEFT JOIN ${env:TMP_db}.atom_seq_20190201_${env:CLUSTER}_${env:STEP} equip
ON (cqe.acct_number_enc = equip.encrypted_account_number_256)
;

-- run the actual enrichment
INSERT OVERWRITE TABLE asp_scp_portals_action PARTITION(data_utc_dt = '${env:START_DATE}')
SELECT
      cqe_equip.acct_number_enc
    , cqe_equip.sys_enc
    , cqe_equip.prin_enc
    , cqe_equip.agent_enc
    , cqe_equip.msg_event_case_id
    , cqe_equip.cur_page_ele
    , cqe_equip.cur_page_nm
    , cqe_equip.app_api_response_code
    , cqe_equip.msg_ts
    , cqe_equip.data_utc_hr
    , cqe_equip.app_api_trace_id
    , cqe_equip.op_toggle_state
    , cqe_equip.msa_multi_ap
    , cqe_equip.msa_scp_flag
    , cqe_equip.application__api__api_cached
    , cqe_equip.application__api__api_category
    , cqe_equip.application__api__api_name
    , cqe_equip.application__api__architecture
    , cqe_equip.application__api__client_service_result
    , cqe_equip.application__api__data_center
    , cqe_equip.application__api__gql__operation_name
    , cqe_equip.application__api__gql__operation_type
    , cqe_equip.application__api__host
    , cqe_equip.application__api__http_verb
    , cqe_equip.application__api__max_retry_count
    , cqe_equip.application__api__path
    , cqe_equip.application__api__reported_headers
    , cqe_equip.application__api__response_size
    , cqe_equip.application__api__enc_Query_parameters
    , cqe_equip.application__api__query_parameters
    , cqe_equip.application__api__response_text
    , cqe_equip.application__api__response_time_ms
    , cqe_equip.application__api__retry
    , cqe_equip.application__api__retry_count
    , cqe_equip.application__api__service_result
    , cqe_equip.application__api__will_retry
    , cqe_equip.feature_name
    , cqe_equip.feature_type
    , cqe_equip.feature_step_name
    , cqe_equip.mso
    , baes.wifi_customer_type
FROM  ${env:TMP_db}.scp_pa_equip_${env:CLUSTER}_${env:STEP} cqe_equip
LEFT JOIN  ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP} baes
ON (cqe_equip.encrypted_account_key_256 = baes.encrypted_account_key_256)
--AND cqe.mso = baes.mso_baes)
;

-- drop/delete temp tables
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_cqe_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.atom_seq_20190201_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_equip_${env:CLUSTER}_${env:STEP};
