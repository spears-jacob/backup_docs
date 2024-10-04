USE ${env:DASP_db};

SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.size.per.task=2048000000;
SET hive.groupby.orderby.position.alias=true;

SET hivevar:final_confirm='mySpectrum_applicationActivity_generic_finalConfirm';
SET hivevar:final_confirm=array('mySpectrum_applicationActivity_generic_finalConfirm','mySpectrum_applicationActivity_manualPauseDevice_finalConfirm','mySpectrum_applicationActivity_manualUnpauseDevice_finalConfirm','mySpectrum_applicationActivity_createPauseScheduleSave_finalConfirm','mySpectrum_applicationActivity_groupPauseSave_finalConfirm','mySpectrum_applicationActivity_groupUnpauseSave_finalConfirm','mySpectrum_applicationActivity_groupCreatePauseScheduleSave_finalConfirm');
SET hivevar:confirm_arr=array('Equipment:WPA2 Password Change:true','Equipment:WPA2 Password Change:false','Equipment:WPA2 Password & SSID Change:true','Equipment:WPA2 Password & SSID Change:false','Equipment:SSID Change:true','Equipment:SSID Change:false','AIHW:Manual Unpause Device:true','AIHW:Manual Unpause Device:false','AIHW:Manual Pause Device:true','AIHW:Manual Pause Device:false','SCP:Create Pause Schedule Save:true','SCP:Create Pause Schedule Save:false','AIHW:Device Nickname Change:false','AIHW:Device Nickname Change:true','AIHW:Remove Device:false','AIHW:Remove Device:true','SCP:Unpause Group Save:false','SCP:Create Group Pause Schedule Save:true','SCP:Unpause Group Save:true','SCP:Pause Group Save:false','SCP:Pause Group Save:true','SCP:Create Group Pause Schedule Save:false','SCP:Edit Group:true','SCP:Create Group:true','SCP:Edit Group:false','SCP:Create Group:false');
SET hivevar:eid_arr=array('mySpectrum_applicationActivity_generic_finalConfirm','mySpectrum_Generic_API_Call_Failure','mySpectrum_Generic_API_Call_Success','mySpectrum_Generic_Error');
SET hivevar:eid2_arr=array('mySpectrum_applicationActivity_equipment_selfInstall_status');
SET hivevar:eid3_arr=array('mySpectrum_featureStop_equipment_troubleshootFlowStop_success', 'mySpectrum_featureStop_equipment_troubleshootFlowStop_failure', 'mySpectrum_featureStop_equipment_troubleshootFlowStop_abandon');

INSERT OVERWRITE TABLE asp_scp_portals_acct_agg PARTITION(data_utc_dt = '${env:START_DATE}')
SELECT
  acct_number_enc
  , sys_enc
  , prin_enc
  , agent_enc
  , msa_scp_flag
  , CASE
      WHEN array_contains(${hivevar:final_confirm},msg_event_case_id)
      THEN CONCAT(SPLIT(cur_page_ele,':')[0],':',SPLIT(cur_page_ele,':')[1],':',SPLIT(cur_page_ele,':')[2])
      WHEN array_contains(${hivevar:eid2_arr},msg_event_case_id)
      THEN CONCAT(msg_event_case_id,':',feature_name,':',cur_page_ele,':',mso)
      WHEN array_contains(${hivevar:eid3_arr},msg_event_case_id)
      THEN CONCAT(msg_event_case_id,':',feature_type,':',feature_step_name)
      ELSE msg_event_case_id END AS action_string
  , COUNT(*) AS action_cnt
  , app_api_response_code
  , SPLIT(cur_page_ele,':')[3] AS error_msg
  , wifi_customer_type
FROM asp_scp_portals_action p
WHERE data_utc_dt = '${env:START_DATE}'
AND ((array_contains(${hivevar:final_confirm},msg_event_case_id)
      AND array_contains(${hivevar:confirm_arr},CONCAT(SPLIT(cur_page_ele,':')[0],':',SPLIT(cur_page_ele,':')[1],':',SPLIT(cur_page_ele,':')[2])))
    OR NOT(array_contains(${hivevar:eid_arr},msg_event_case_id)))
GROUP BY
  acct_number_enc
  , sys_enc
  , prin_enc
  , agent_enc
  , CASE
      WHEN array_contains(${hivevar:final_confirm},msg_event_case_id)
      THEN CONCAT(SPLIT(cur_page_ele,':')[0],':',SPLIT(cur_page_ele,':')[1],':',SPLIT(cur_page_ele,':')[2])
      WHEN array_contains(${hivevar:eid2_arr},msg_event_case_id)
      THEN CONCAT(msg_event_case_id,':',feature_name,':',cur_page_ele,':',mso)
      WHEN array_contains(${hivevar:eid3_arr},msg_event_case_id)
      THEN CONCAT(msg_event_case_id,':',feature_type,':',feature_step_name)
      ELSE msg_event_case_id END
  , msa_scp_flag
  , app_api_response_code
  , SPLIT(cur_page_ele,':')[3]
  , wifi_customer_type
;
