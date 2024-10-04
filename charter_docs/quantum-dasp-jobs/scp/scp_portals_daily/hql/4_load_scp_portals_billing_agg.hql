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

INSERT OVERWRITE TABLE asp_scp_portals_billing_agg PARTITION(partition_date = '${env:START_DATE}')
SELECT
  scp_flag,
  wifi_flag,
  internet_flag,
  future_connect_flag,
  CASE WHEN grouping(account_type) = 0 THEN account_type ELSE 'All account_type' END AS account_type,
  CASE WHEN grouping(legacy_company) = 0 THEN legacy_company ELSE 'All legacy_company' END AS legacy_company,
  CASE WHEN grouping(customer_type) = 0 THEN customer_type ELSE 'All customer_type' END AS customer_type,
  SUM(total_hhs) AS total_hhs,
  CAST(grouping__id AS int) AS grouping_id,
  CONCAT_WS('|',
    IF(grouping(legacy_company) = 0, 'legacy_company', NULL),
    IF(grouping(customer_type) = 0, 'customer_type', NULL),
    IF(grouping(scp_flag) = 0, 'scp_flag', NULL),
    IF(grouping(wifi_flag) = 0, 'wifi_flag', NULL),
    IF(grouping(internet_flag) = 0, 'internet_flag', NULL),
    IF(grouping(future_connect_flag) = 0, 'future_connect_flag', NULL),
    IF(grouping(account_type) = 0, 'account_type', NULL),
    IF(grouping(grain) = 0, 'grain', NULL),
    IF(grouping(wifi_customer_type) = 0, 'wifi_customer_type', NULL)
  ) as grouping_set,
  wifi_customer_type,
  CASE WHEN grouping(grain) = 0 THEN grain ELSE 'All grain' END AS grain
FROM
(
  SELECT
    grain,
    scp_flag,
    account_type,
    wifi_flag,
    internet_flag,
    legacy_company,
    customer_type,
    future_connect_flag,
    SUM(total_hhs) AS total_hhs,
    wifi_customer_type
  FROM `${env:BSA}`
  WHERE partition_date = '${env:START_DATE}' -- use 9/29
    AND (CASE WHEN scp_flag THEN LOWER(equipment_status) = 'connected' ELSE true END)
  GROUP BY
    scp_flag,
    wifi_flag,
    internet_flag,
    future_connect_flag,
    account_type,
    legacy_company,
    customer_type,
    grain,
    wifi_customer_type
  ) b_agg
  GROUP BY
    legacy_company,
    customer_type,
    scp_flag,
    wifi_flag,
    internet_flag,
    future_connect_flag,
    account_type,
    grain,
    wifi_customer_type
  GROUPING SETS
  (
    (grain, scp_flag, wifi_flag, future_connect_flag, wifi_customer_type),
    (grain, scp_flag, wifi_flag, future_connect_flag, legacy_company, wifi_customer_type),
    (grain, scp_flag, wifi_flag, future_connect_flag, account_type, wifi_customer_type),
    (grain, scp_flag, wifi_flag, future_connect_flag, customer_type, wifi_customer_type),
    (grain, scp_flag, wifi_flag, future_connect_flag, internet_flag, wifi_customer_type),
    (grain, scp_flag, wifi_flag, future_connect_flag, legacy_company, account_type, customer_type, wifi_customer_type),
    (grain, scp_flag, wifi_flag, future_connect_flag, internet_flag, legacy_company, account_type, customer_type, wifi_customer_type)
  )
;