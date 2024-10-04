USE ${env:DASP_db};

SET hive.vectorized.execution.enabled=false;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.size.per.task=2048000000;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- prepare temp tables for all-local processing

-- table 1 data pulled from BAES table

DROP TABLE IF EXISTS  ${env:TMP_db}.scp_pa_period_${env:CLUSTER}_${env:STEP};
CREATE TABLE          ${env:TMP_db}.scp_pa_period_${env:CLUSTER}_${env:STEP} AS
  SELECT
      encrypted_account_key_256 AS account_key
    , MAX(is_scp_router) as scp_flag
    , MAX(has_wifi) as wifi_flag
    , MIN(is_future_connect_acct) as future_connect_flag
    , MIN(customer_disconnect_count) as disconnect_count
    , MIN(equipment_status) as equipment_status
    , CASE WHEN wifi_customer_type IS NULL AND partition_date <='2022-09-28' THEN 'AHW'
           ELSE wifi_customer_type
           END AS wifi_customer_type
    ,CASE
    WHEN UPPER(wifi_customer_type) = 'AHW' THEN '0-AHW'
    WHEN UPPER(wifi_customer_type) = 'ABW' THEN '1-ABW'
    WHEN UPPER(wifi_customer_type) = 'ACW' THEN '2-ACW'
    WHEN UPPER(wifi_customer_type) = 'RESI' THEN '3-RESI'
    WHEN UPPER(wifi_customer_type) = 'SCS' THEN '4-SCS'
    WHEN UPPER(wifi_customer_type) = 'SMB' THEN '5-SMB'
    WHEN UPPER(wifi_customer_type) = 'OTHER' THEN '6-OTHER'
    ELSE '7-UNKNOWN' END AS wifi_customer_type_rank
    ,ROW_NUMBER() OVER (
    PARTITION BY encrypted_account_key_256 ORDER BY
    CASE
    WHEN UPPER(wifi_customer_type) = 'AHW' THEN '0-AHW'
    WHEN UPPER(wifi_customer_type) = 'ABW' THEN '1-ABW'
    WHEN UPPER(wifi_customer_type) = 'ACW' THEN '2-ACW'
    WHEN UPPER(wifi_customer_type) = 'RESI' THEN '3-RESI'
    WHEN UPPER(wifi_customer_type) = 'SCS' THEN '4-SCS'
    WHEN UPPER(wifi_customer_type) = 'SMB' THEN '5-SMB'
    WHEN UPPER(wifi_customer_type) = 'OTHER' THEN '6-OTHER'
    ELSE '7-UNKNOWN' END ASC) AS account_rank
  FROM `${env:BAES}`
  WHERE (partition_date >= '${env:START_DATE}'
     AND partition_date <  '${env:END_DATE}'  )
  GROUP BY
      encrypted_account_key_256
    , CASE WHEN wifi_customer_type IS NULL AND partition_date <='2022-09-28' THEN 'AHW'
           ELSE wifi_customer_type
           END
    ,CASE
    WHEN UPPER(wifi_customer_type) = 'AHW' THEN '0-AHW'
    WHEN UPPER(wifi_customer_type) = 'ABW' THEN '1-ABW'
    WHEN UPPER(wifi_customer_type) = 'ACW' THEN '2-ACW'
    WHEN UPPER(wifi_customer_type) = 'RESI' THEN '3-RESI'
    WHEN UPPER(wifi_customer_type) = 'SCS' THEN '4-SCS'
    WHEN UPPER(wifi_customer_type) = 'SMB' THEN '5-SMB'
    WHEN UPPER(wifi_customer_type) = 'OTHER' THEN '6-OTHER'
    ELSE '7-UNKNOWN' END
;

-- table 2 aggregation of table 1. could possibly be converted to a subquery

DROP TABLE IF EXISTS  ${env:TMP_db}.scp_pa_billing_${env:CLUSTER}_${env:STEP};
CREATE TABLE          ${env:TMP_db}.scp_pa_billing_${env:CLUSTER}_${env:STEP} AS
  SELECT
    scp_flag,
    wifi_customer_type,
    SUM(CASE WHEN disconnect_count = 0 THEN 1 ELSE 0 END) AS total_hhs
  FROM ${env:TMP_db}.scp_pa_period_${env:CLUSTER}_${env:STEP}
  WHERE NOT (scp_flag AND LOWER(equipment_status) != 'connected')
    AND wifi_flag
    AND not future_connect_flag
    AND account_rank = 1
  GROUP BY
    scp_flag,
    wifi_customer_type
;

-- table 3 pulled for login table

DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_portals_${env:CLUSTER}_${env:STEP};
CREATE TABLE         ${env:TMP_db}.scp_pa_portals_${env:CLUSTER}_${env:STEP} AS
  SELECT
    scp_flag,
    wifi_customer_type,
    COUNT(DISTINCT(account_number)) as hhs,
    SUM(CASE WHEN event_type = 'portals_start_pause_device' THEN instances ELSE 0 END) as pause_events,
    SUM(CASE WHEN event_type = 'portals_start_unpause_device' THEN instances ELSE 0 END) as unpause_events,
    SUM(CASE WHEN event_type = 'portals_equipment_confirm_edit_ssid_select_action' THEN instances ELSE 0 END) as ssid_change_events,
    SUM(CASE WHEN event_type = 'portals_confirm_remove_device' THEN instances ELSE 0 END) as remove_device_events,
    SUM(CASE WHEN event_type = 'portals_save_edit_device_nickname' THEN instances ELSE 0 END) as edit_device_nickname_events,
    SUM(CASE WHEN event_type = 'portals_view_connected_devices' THEN instances ELSE 0 END) as connected_device_events,
    COUNT(DISTINCT(CASE WHEN event_type = 'portals_equipment_list_view' AND instances > 0 THEN account_number END)) as equipment_page_view_hhs,
    COUNT(DISTINCT(CASE WHEN event_type = 'portals_start_pause_device' AND instances > 0 THEN account_number END)) as pause_event_hhs
  FROM `${env:LOGIN}`
  WHERE (partition_date >= '${env:START_DATE}'
     AND partition_date <  '${env:END_DATE}'  )
    AND wifi_flag
    AND not future_connect_flag
  GROUP BY
    scp_flag,
    wifi_customer_type
;

-- run the actual enrichment
-- LABEL_DATE_DENVER is just the 28th of the month, the fiscal month end being cut by START_DATE and END_DATE

INSERT OVERWRITE TABLE asp_scp_portals_product PARTITION(partition_date='${env:LABEL_DATE_DENVER}')
SELECT
  b_agg.scp_flag,
  p_agg.hhs AS login_hhs,
  p_agg.pause_events AS pause_events,
  p_agg.unpause_events AS unpause_events,
  p_agg.ssid_change_events AS ssid_change_events,
  p_agg.remove_device_events AS remove_device_events,
  p_agg.edit_device_nickname_events AS edit_device_nickname_events,
  p_agg.connected_device_events AS connected_device_events,
  p_agg.hhs / b_agg.total_hhs AS session_rate,
  p_agg.equipment_page_view_hhs / p_agg.hhs AS equipment_page_view_hhs_per_hhs_rate,
  p_agg.pause_event_hhs / b_agg.total_hhs AS pause_event_hhs_per_total_hhs_rate,
  b_agg.wifi_customer_type,
  'fiscal_month' AS grain
FROM ${env:TMP_db}.scp_pa_billing_${env:CLUSTER}_${env:STEP} b_agg
INNER JOIN ${env:TMP_db}.scp_pa_portals_${env:CLUSTER}_${env:STEP} p_agg
  ON b_agg.scp_flag = p_agg.scp_flag AND b_agg.wifi_customer_type = p_agg.wifi_customer_type
DISTRIBUTE BY '${env:LABEL_DATE_DENVER}'
;

-- drop/delete temp tables
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_period_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_billing_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_portals_${env:CLUSTER}_${env:STEP};
