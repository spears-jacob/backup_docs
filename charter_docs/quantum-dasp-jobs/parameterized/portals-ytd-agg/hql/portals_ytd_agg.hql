--XG19913
USE ${env:DASP_db};
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite table ${env:DASP_db}.asp_portals_ytd_agg PARTITION (label_date_denver, grain)
SELECT
   application_name,
   application_type,
   'devices'                         AS unit_type,
   metric_name,
   metric_value,
   '${hiveconf:LABEL_DATE_DENVER}'   AS label_date_denver,
   'ytd'                             AS grain
  FROM
  (
--  Distinct device counts
   SELECT
     application_name,
     application_type,
     MAP(
     'portals_download_wifi_profile_success', SIZE(COLLECT_SET(IF(portals_download_wifi_profile_success > 0, device_id, NULL))),
     'portals_download_wifi_profile_button_click', SIZE(COLLECT_SET(IF(portals_download_wifi_profile_button_click > 0, device_id, NULL)))
        ) AS tmp_map
      FROM ${env:DASP_db}.quantum_metric_agg_portals
     WHERE denver_date            >= '${hiveconf:START_DATE}'
       AND denver_date            <  '${hiveconf:END_DATE}'
       AND application_name       =  'myspectrum'
    GROUP BY
        application_name,
        application_type
      ) getcounts
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;
