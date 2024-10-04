USE ${env:DASP_db};

SET hive.vectorized.execution.enabled=false;
SET hive.auto.convert.join=false;
SET hive.optimize.sort.dynamic.partition=false;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;


-- prepare temp tables for local processing
DROP TABLE IF EXISTS  ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP};
CREATE TABLE          ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP} AS
  SELECT
      legacy_company
    , encrypted_legacy_account_number_256 AS account_number
    , has_wifi AS wifi_flag
    , is_internet_customer AS internet_flag
    , is_future_connect_acct AS future_connect_flag
    , customer_type
    , account_type
    , is_scp_acct AS scp_flag
    , encrypted_account_key_256 AS account_key
    , CASE WHEN wifi_customer_type IS NULL AND partition_date <='2022-09-28' THEN 'AHW'
           ELSE wifi_customer_type
           END AS wifi_customer_type
    , partition_date
    , ROW_NUMBER() OVER (PARTITION BY encrypted_legacy_account_number_256 ORDER BY CASE WHEN subscriber_type = 'other' THEN 'zother' ELSE subscriber_type END ASC, is_internet_customer DESC, is_smb_internet_customer DESC) AS account_rank
  FROM `${env:BAES}`
  WHERE partition_date = '${env:START_DATE}'
  AND CASE
              WHEN is_scp_acct THEN LOWER(equipment_status) = 'connected'
              ELSE TRUE
            END
;



-- Run enrichment
INSERT OVERWRITE TABLE asp_scp_portals_snapshot PARTITION(partition_date = '${env:START_DATE}')
SELECT
  legacy_company
  , account_number
  , metric_name AS event_type
  , COALESCE(metric_value,0) AS instances
  , application_name
  , scp_flag
  , login_flag
  , wifi_flag
  , internet_flag
  , future_connect_flag
  , customer_type
  , account_type
  , account_key
  , wifi_customer_type
FROM (
  SELECT
    b_snapshot.legacy_company
    , b_snapshot.account_number
    , p_agg.application_name
    , b_snapshot.scp_flag
    , (p_agg.acct_id IS NOT NULL) AS login_flag
    , b_snapshot.wifi_flag
    , b_snapshot.internet_flag
    , b_snapshot.future_connect_flag
    , b_snapshot.customer_type
    , b_snapshot.account_type
    , b_snapshot.account_key
    , b_snapshot.wifi_customer_type
    , MAP
        (
          'portals_confirm_remove_device', SUM(p_agg.portals_confirm_remove_device),
          'portals_save_edit_device_nickname', SUM(p_agg.portals_save_edit_device_nickname),
          'portals_start_pause_device', SUM(p_agg.portals_start_pause_device),
          'portals_start_unpause_device', SUM(p_agg.portals_start_unpause_device),
          'portals_equipment_confirm_edit_ssid_select_action', SUM(p_agg.portals_equipment_confirm_edit_ssid_select_action),
          'portals_view_connected_devices', SUM(p_agg.portals_view_connected_devices),
          'portals_equipment_list_view', SUM(p_agg.portals_equipment_list_view)
        ) feature
  FROM (SELECT* FROM ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP} WHERE account_rank = 1 AND account_number is NOT NULL) b_snapshot
  LEFT JOIN
    (
      SELECT
        *
      FROM `${env:QMAP}`
      WHERE denver_date = '${env:START_DATE}'
    ) p_agg
    ON b_snapshot.account_number = p_agg.acct_id
    AND b_snapshot.partition_date = p_agg.denver_date
  GROUP BY
    b_snapshot.legacy_company
    , b_snapshot.account_number
    , p_agg.application_name
    , b_snapshot.scp_flag
    , (p_agg.acct_id is NOT NULL)
    , b_snapshot.wifi_flag
    , b_snapshot.internet_flag
    , b_snapshot.future_connect_flag
    , b_snapshot.customer_type
    , b_snapshot.account_type
    , b_snapshot.account_key
    , b_snapshot.wifi_customer_type
) a
LATERAL VIEW OUTER EXPLODE(feature) explode_table AS metric_name, metric_value
DISTRIBUTE BY '${env:START_DATE}'
;

-- drop/delete temp tables
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_baes_${env:CLUSTER}_${env:STEP};