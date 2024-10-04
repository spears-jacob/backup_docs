USE ${env:DASP_db};

set hive.auto.convert.join=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.support.concurrency=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- {unit_type} metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.quantum_set_agg_portals_stage_{unit_type}_${hiveconf:execid}_${hiveconf:stepid}
SELECT
  CASE WHEN (grouping_id & 65536) = 0 THEN mso ELSE 'All Companies' END AS mso,
  CASE WHEN (grouping_id & 32768) = 0 THEN application_type ELSE 'All Applications' END AS application_type,
  CASE WHEN (grouping_id & 16384) = 0 THEN device_type ELSE 'All Devices' END AS device_type,
  CASE WHEN (grouping_id & 8192) = 0 THEN connection_type ELSE 'All Connections' END AS connection_type,
  CASE WHEN (grouping_id & 4096) = 0 THEN network_status ELSE 'All Network Statuses' END AS network_status,
  CASE WHEN (grouping_id & 2048) = 0 THEN playback_type ELSE 'All Playback Types' END AS playback_type,
  CASE WHEN (grouping_id & 1024) = 0 THEN cust_type ELSE 'All Customer Types' END AS cust_type,
  CASE WHEN (grouping_id & 512) = 0 THEN application_group_type ELSE 'All Application Group Types' END AS application_group_type,
  CASE WHEN (grouping_id & 256) = 0 THEN app_version ELSE 'All App Versions' END AS app_version,
  grouping_id,
  metric_name,
  metric_value,
  CASE WHEN (grouping_id & 128) = 0 THEN logged_in_status ELSE 'All Logged In Statuses' END AS logged_in_status,
  CASE WHEN (grouping_id & 64) = 0 THEN application_name ELSE 'All App Versions' END AS application_name,
  CASE WHEN (grouping_id & 32) = 0 THEN os_name ELSE 'All OS Names' END AS os_name,
  CASE WHEN (grouping_id & 16) = 0 THEN os_version ELSE 'All OS Versions' END AS os_version,
  CASE WHEN (grouping_id & 8) = 0 THEN browser_name ELSE 'All Browser Names' END AS browser_name,
  CASE WHEN (grouping_id & 4) = 0 THEN browser_version ELSE 'All Browser Versions' END AS browser_version,
  CASE WHEN (grouping_id & 2) = 0 THEN form_factor ELSE 'All Form Factors' END AS form_factor,
  '${hiveconf:ProcessTimestamp}' as process_date_time_denver,
  '${hiveconf:ProcessUser}' AS process_identity,
  unit_type,
  IF('{unit_type}'='visits',cast(metric_calls as string),cast(NULL as string)) as call_count_24h,
  label_date_denver,
  '${hiveconf:grain}' AS grain
FROM
  (
  SELECT
    '${hiveconf:label_date_denver}' AS label_date_denver,
    mso,
    application_type,
    device_type,
    connection_type,
    network_status,
    playback_type,
    cust_type,
    application_group_type,
    app_version,
    logged_in_status,
    application_name,
    os_name,
    os_version,
    browser_name,
    browser_version,
    unit_type,
    form_factor,
    grouping_id,
      MAP(

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
