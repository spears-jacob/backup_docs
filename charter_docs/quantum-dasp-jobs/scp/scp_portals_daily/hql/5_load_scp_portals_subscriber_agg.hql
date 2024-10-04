USE ${env:DASP_db};


SET hive.vectorized.execution.enabled=false;
SET hive.auto.convert.join=false;
SET hive.optimize.sort.dynamic.partition=false;
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
DROP TABLE IF EXISTS  ${env:TMP_db}.scp_bill_agg_${env:CLUSTER}_${env:STEP};
CREATE TABLE          ${env:TMP_db}.scp_bill_agg_${env:CLUSTER}_${env:STEP} AS
SELECT  *
FROM `${env:BILLAGG}` WHERE partition_date = '${env:START_DATE}'
;

DROP TABLE IF EXISTS  ${env:TMP_db}.scp_login_sub_agg_${env:CLUSTER}_${env:STEP};
CREATE TABLE          ${env:TMP_db}.scp_login_sub_agg_${env:CLUSTER}_${env:STEP} AS
SELECT * FROM `${env:LOGSA}` WHERE partition_date = '${env:START_DATE}'
;

-- run the actual enrichment
INSERT OVERWRITE TABLE asp_scp_portals_subscriber_agg PARTITION(partition_date = '${env:START_DATE}')
SELECT
  b_agg.grouping_id,
  b_agg.grouping_set,
  b_agg.legacy_company,
  b_agg.customer_type,
  b_agg.scp_flag,
  b_agg.wifi_flag,
  b_agg.internet_flag,
  b_agg.future_connect_flag,
  b_agg.account_type,
  COALESCE(p_agg.hhs, 0) AS login_hhs,
  COALESCE(p_agg.equipment_page_view_hhs, 0) AS equipment_page_view_hhs,
  COALESCE(p_agg.pause_event_hhs, 0) AS pause_event_hhs,
  COALESCE(p_agg.unpause_event_hhs, 0) AS unpause_event_hhs,
  COALESCE(p_agg.ssid_change_hhs, 0) AS ssid_change_hhs,
  COALESCE(p_agg.remove_device_hhs, 0) AS remove_device_hhs,
  COALESCE(p_agg.edit_device_nickname_hhs, 0) AS edit_device_nickname_hhs,
  COALESCE(p_agg.connected_device_hhs, 0) AS connected_device_hhs,
  COALESCE(p_agg.equipment_page_view_events, 0) AS equipment_page_view_events,
  COALESCE(p_agg.pause_events, 0) AS pause_events,
  COALESCE(p_agg.unpause_events, 0) AS unpause_events,
  COALESCE(p_agg.ssid_change_events, 0) AS ssid_change_events,
  COALESCE(p_agg.remove_device_events, 0) AS remove_device_events,
  COALESCE(p_agg.edit_device_nickname_events, 0) AS edit_device_nickname_events,
  COALESCE(p_agg.connected_device_events, 0) AS connected_device_events,
  COALESCE(b_agg.total_hhs, 0) AS total_hhs,
  COALESCE(p_agg.hhs / b_agg.total_hhs, 0) AS session_rate,
  COALESCE(p_agg.equipment_page_view_hhs / p_agg.hhs, 0) AS equipment_page_view_hhs_per_hhs_rate,
  COALESCE(p_agg.equipment_page_view_hhs / b_agg.total_hhs, 0) AS equipment_page_view_hhs_per_total_hhs_rate,
  COALESCE(p_agg.pause_event_hhs / p_agg.hhs, 0) AS pause_event_hhs_per_hhs_rate,
  COALESCE(p_agg.pause_event_hhs / b_agg.total_hhs, 0) AS pause_event_hhs_per_total_hhs_rate,
  COALESCE(p_agg.unpause_event_hhs / p_agg.hhs, 0) AS unpause_event_hhs_per_hhs_rate,
  COALESCE(p_agg.unpause_event_hhs / b_agg.total_hhs, 0) AS unpause_event_hhs_per_total_hhs_rate,
  b_agg.wifi_customer_type,
  b_agg.grain
FROM ${env:TMP_db}.scp_bill_agg_${env:CLUSTER}_${env:STEP} b_agg
LEFT JOIN ${env:TMP_db}.scp_login_sub_agg_${env:CLUSTER}_${env:STEP} p_agg
  ON b_agg.grain = p_agg.grain
  AND b_agg.grouping_set = p_agg.grouping_set
  AND b_agg.legacy_company = p_agg.legacy_company
  AND b_agg.customer_type = p_agg.customer_type
  AND b_agg.scp_flag <=> p_agg.scp_flag
  AND b_agg.wifi_flag <=> p_agg.wifi_flag
  AND b_agg.internet_flag <=> p_agg.internet_flag
  AND b_agg.future_connect_flag <=> p_agg.future_connect_flag
  AND b_agg.account_type = p_agg.account_type
  AND b_agg.wifi_customer_type = p_agg.wifi_customer_type
;

-- drop/delete temp tables
DROP TABLE IF EXISTS ${env:TMP_db}.scp_bill_agg_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.scp_login_sub_agg_${env:CLUSTER}_${env:STEP};
