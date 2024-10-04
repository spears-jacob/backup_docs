SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;
SET fs.s3a.multiobjectdelete.enable=false;
-- the above was added to address the following issue: com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.model.MultiObjectDeleteException: One or more objects could not be deleted (Service: null; Status Code: 200; Error Code: null

USE ${env:DASP_db};

SET hivevar:start_actions_arr=array('mySpectrum_selectAction_equipment_deviceManagement_swipedToSchedulePause', 'mySpectrum_selectAction_equipment_deviceManagement_createPauseSchedule', 'mySpectrum_selectAction_equipment_deviceManagement_pauseActionSheet_createPauseSchedule');
SET hivevar:save_schedule_pause_true='SCP:Create Pause Schedule Save:true';
SET hivevar:save_schedule_pause_failed='SCP:Create Pause Schedule Save:false';
SET hivevar:save_schedule_cancel='mySpectrum_selectAction_equipment_deviceManagement_scheduledPauseCancel';
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE asp_scp_portals_pause_schedule_action_agg PARTITION(data_utc_dt = '${env:START_DATE}')
  SELECT
    metric_name
    , metric_value
    , wifi_customer_type
    , grain
  FROM (
    SELECT
      MAP(
        'start_total', start_total,
        'saved_total', saved_total,
        'failed_total', failed_total,
        'cancelled_total', cancelled_total,
        'start_acct_cnt', start_acct_cnt,
        'saved_acct_cnt', saved_acct_cnt,
        'failed_acct_cnt', failed_acct_cnt,
        'cancelled_acct_cnt', cancelled_acct_cnt
      ) actions
      , wifi_customer_type
      , grain
    FROM (
      SELECT
        SUM(IF(ARRAY_CONTAINS(${hivevar:start_actions_arr},action_string), action_cnt, 0)) AS start_total
        , SUM(IF(action_string = ${hivevar:save_schedule_pause_true}, action_cnt, 0)) AS saved_total
        , SUM(IF(action_string = ${hivevar:save_schedule_pause_failed}, action_cnt, 0)) AS failed_total
        , SUM(IF(action_string = ${hivevar:save_schedule_cancel}, action_cnt, 0)) AS cancelled_total
        , COUNT(DISTINCT IF(ARRAY_CONTAINS(${hivevar:start_actions_arr},action_string), CONCAT(acct_number_enc, sys_enc, prin_enc, agent_enc), NULL)) AS start_acct_cnt
        , COUNT(DISTINCT IF(action_string = ${hivevar:save_schedule_pause_true}, CONCAT(acct_number_enc, sys_enc, prin_enc, agent_enc), NULL)) AS saved_acct_cnt
        , COUNT(DISTINCT IF(action_string = ${hivevar:save_schedule_pause_failed}, CONCAT(acct_number_enc, sys_enc, prin_enc, agent_enc), NULL)) AS failed_acct_cnt
        , COUNT(DISTINCT IF(action_string = ${hivevar:save_schedule_cancel}, CONCAT(acct_number_enc, sys_enc, prin_enc, agent_enc), NULL)) AS cancelled_acct_cnt
        , wifi_customer_type
        , 'daily' grain
      FROM `${env:ACCAGG}`
      WHERE data_utc_dt = '${env:START_DATE}'
      GROUP BY wifi_customer_type
    ) x
  ) y
  LATERAL VIEW OUTER EXPLODE(actions) explode_table AS metric_name, metric_value
  DISTRIBUTE BY '${env:START_DATE}'
;