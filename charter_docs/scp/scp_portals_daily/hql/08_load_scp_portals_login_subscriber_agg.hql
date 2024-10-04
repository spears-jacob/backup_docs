USE ${env:DASP_db};

SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;

DROP TABLE IF EXISTS ${env:TMP_db}.scp_lsa_${env:CLUSTER}_${env:STEP};
create table ${env:TMP_db}.scp_lsa_${env:CLUSTER}_${env:STEP} as
SELECT  'daily' as grain,
        legacy_company,
        customer_type,
        scp_flag,
        wifi_flag,
        internet_flag,
        future_connect_flag,
        account_type,
        account_number,
        event_type,
        instances,
        wifi_customer_type
FROM `${env:LOGIN}`
WHERE partition_date = '${env:START_DATE}';

INSERT OVERWRITE TABLE asp_scp_portals_login_subscriber_agg PARTITION(partition_date = '${env:START_DATE}')
SELECT
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
  CASE WHEN grouping(legacy_company) = 0 THEN legacy_company ELSE 'All legacy_company' END AS legacy_company,
  CASE WHEN grouping(customer_type) = 0 THEN customer_type ELSE 'All customer_type' END AS customer_type,
  scp_flag,
  wifi_flag,
  internet_flag,
  future_connect_flag,
  CASE WHEN grouping(account_type) = 0 THEN account_type ELSE 'All account_type' END AS account_type,
  COUNT(DISTINCT(account_number)) as hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_equipment_list_view' AND instances > 0 THEN account_number END)) as equipment_page_view_hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_start_pause_device' AND instances > 0 THEN account_number END)) as pause_event_hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_start_unpause_device' AND instances > 0 THEN account_number END)) as unpause_event_hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_equipment_confirm_edit_ssid_select_action' AND instances > 0 THEN account_number END)) as ssid_change_hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_confirm_remove_device' AND instances > 0 THEN account_number END)) as remove_device_hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_save_edit_device_nickname' AND instances > 0 THEN account_number END)) as edit_device_nickname_hhs,
  COUNT(DISTINCT(CASE WHEN event_type = 'portals_view_connected_devices' AND instances > 0 THEN account_number END)) as connected_device_hhs,
  SUM(CASE WHEN event_type = 'portals_equipment_list_view' THEN instances ELSE 0 END) as equipment_page_view_events,
  SUM(CASE WHEN event_type = 'portals_start_pause_device' THEN instances ELSE 0 END) as pause_events,
  SUM(CASE WHEN event_type = 'portals_start_unpause_device' THEN instances ELSE 0 END) as unpause_events,
  SUM(CASE WHEN event_type = 'portals_equipment_confirm_edit_ssid_select_action' THEN instances ELSE 0 END) as ssid_change_events,
  SUM(CASE WHEN event_type = 'portals_confirm_remove_device' THEN instances ELSE 0 END) as remove_device_events,
  SUM(CASE WHEN event_type = 'portals_save_edit_device_nickname' THEN instances ELSE 0 END) as edit_device_nickname_events,
  SUM(CASE WHEN event_type = 'portals_view_connected_devices' THEN instances ELSE 0 END) as connected_device_events,
  CASE WHEN grouping(wifi_customer_type) = 0 THEN wifi_customer_type ELSE 'All wifi_customer_type' END AS wifi_customer_type,
  CASE WHEN grouping(grain) = 0 THEN grain ELSE 'All grain' END AS grain
FROM (SELECT * FROM ${env:TMP_db}.scp_lsa_${env:CLUSTER}_${env:STEP}) final
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
  (grain, scp_flag, future_connect_flag, wifi_flag, wifi_customer_type),
  (grain, scp_flag, future_connect_flag, wifi_flag, legacy_company, wifi_customer_type),
  (grain, scp_flag, future_connect_flag, wifi_flag, account_type, wifi_customer_type),
  (grain, scp_flag, future_connect_flag, wifi_flag, customer_type, wifi_customer_type),
  (grain, scp_flag, future_connect_flag, wifi_flag, internet_flag, wifi_customer_type),
  (grain, scp_flag, future_connect_flag, wifi_flag, legacy_company, account_type, customer_type, wifi_customer_type),
  (grain, scp_flag, future_connect_flag, wifi_flag, internet_flag, legacy_company, account_type, customer_type, wifi_customer_type)
)
DISTRIBUTE BY '${env:START_DATE}'
;

-- drop/delete temp tables
DROP TABLE IF EXISTS ${env:TMP_db}.scp_lsa_${env:CLUSTER}_${env:STEP};