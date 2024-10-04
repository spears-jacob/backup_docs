-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
--  Cadence parameterized Portals set aggregation HQL
--
--  '${env:grain}' AS grain
--  '${env:label_date_denver}',
--  "${env:START_DATE}" "${env:END_DATE}"
--  "${env:ProcessTimestamp}"  "${env:ProcessUser}"
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- {unit_type} metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

INSERT INTO ${env:TMP_db}.venona_set_agg_idm_stage_{unit_type}_${env:execid}
SELECT

page_name,
app_section,
user_role,
device_id,
visit_id,
application_type,
device_type,
app_version,
logged_in,
application_name,
os_name,
operating_system,
browser_name,
browser_version,
browser_size_breakpoint,
form_factor,
referrer_link,
grouping_id,
metric_name,
metric_value,
'${env:ProcessTimestamp}' as process_date_time_denver,
'${env:ProcessUser}' AS process_identity,
'{unit_type}' AS unit_type,
'${env:grain}' AS grain,
label_date_denver
FROM
  (
  SELECT
    '${env:label_date_denver}' AS label_date_denver,
    page_name,
    app_section,
    user_role,
    device_id,
    visit_id,
    application_type,
    device_type,
    app_version,
    logged_in,
    application_name,
    os_name,
    operating_system,
    browser_name,
    browser_version,
    browser_size_breakpoint,
    form_factor,
    referrer_link,
    CAST(grouping__id AS INT) AS grouping_id,
      MAP(

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
