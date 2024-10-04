USE ${env:DASP_db};

-- TODO: find suitable parameters for tez. current split.maxsize and split.minsize parameters seem like are not used.
-- SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
set hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
-- default logic is ignored and Tez tries to group splits into the specified number of groups. Change that parameter carefully.
set tez.grouping.split-count=1200;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

SELECT "\n\nFor: 1 Core Start\n\n";
----Get all HH app name lvl metrics from portals set agg except for SpecMobile
--- insert OVERWRITE first time to clear partition
INSERT OVERWRITE TABLE asp_quality_kpi_core PARTITION(denver_date)
SELECT
  'total' as timeframe,
  grouping_id,
  application_name,
  application_version,
  metric_name,
  metric_value,
  denver_date
FROM
(
  SELECT
    grouping__id as grouping_id,
    application_name,
    application_version,
    denver_date,
    MAP(
      'portals_all_equipment_reset_flow_successes_hh', SUM(portals_all_equipment_reset_flow_successes),
      'portals_all_equipment_reset_flow_failures_hh', SUM(portals_all_equipment_reset_flow_failures),
      'portals_one_time_payment_successes_hh', SUM(portals_one_time_payment_successes),
      'portals_one_time_payment_failures_hh', SUM(portals_one_time_payment_failures),
      'portals_site_unique_hh', SUM(portals_site_unique)
    ) tmp_map
  FROM
    (
      SELECT
        portals_unique_acct_key,
        application_name,
        app_version as application_version,
        denver_date,
        IF(SUM(portals_all_equipment_reset_flow_successes) > 0, 1, 0) AS portals_all_equipment_reset_flow_successes,
        IF(SUM(portals_all_equipment_reset_flow_failures) > 0, 1, 0) AS portals_all_equipment_reset_flow_failures,
        IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
        IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
        IF(SUM(portals_site_unique) > 0, 1, 0) AS portals_site_unique
      FROM
        quantum_metric_agg_portals
      WHERE denver_date >= '${hiveconf:START_DATE}'
        AND denver_date <  '${hiveconf:END_DATE}'
        AND application_name is not null
      GROUP BY
        portals_unique_acct_key,
        application_name,
        app_version,
        denver_date
    ) sub
  GROUP BY
    denver_date,
    application_name,
    application_version
    grouping sets(
    (denver_date),
    (denver_date,application_name),
    (denver_date,application_name,application_version)
    )
) map_query
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

SELECT "\n\nFor: 2 Active HHs (All)\n\n";
----Active HHs (All)
INSERT INTO asp_quality_kpi_core PARTITION(denver_date)
SELECT
  'total' as timeframe,
  3 as grouping_id,
  'All Applications' application_name,
  'All Versions' application_version,
  metric_name,
  metric_value,
  denver_date
FROM
(
  SELECT
    denver_date,
    MAP(
      'portals_site_unique_hh', SUM(portals_site_unique)
    ) tmp_map
  FROM
    (
      SELECT
        portals_unique_acct_key,
        denver_date,
        IF(SUM(portals_site_unique) > 0, 1, 0) AS portals_site_unique
      FROM
        quantum_metric_agg_portals
      WHERE denver_date >= '${hiveconf:START_DATE}'
        AND denver_date <  '${hiveconf:END_DATE}'
        AND application_name is not null
      GROUP BY
        portals_unique_acct_key,
        denver_date
    ) sub
    GROUP BY
      denver_date
) map_query
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

SELECT "\n\nFor: 3 Device log in for all except SpecMobile\n\n";
--Device log in for all except SpecMobile
INSERT INTO asp_quality_kpi_core PARTITION(denver_date)
SELECT
  'total' as timeframe,
  grouping_id,
  application_name,
  application_version,
  metric_name,
  metric_value,
  denver_date
FROM
(
  SELECT
    grouping__id as grouping_id,
    application_name,
    application_version,
    denver_date,
    MAP(
      'portals_login_attempts_devices', SUM(portals_login_attempts),
      'portals_login_failures_devices', SUM(portals_login_failures),
      'portals_site_unique_auth_devices', SUM(portals_site_unique_auth)
    ) tmp_map
  FROM
    (
      SELECT
        device_id,
        application_name,
        app_version as application_version,
        denver_date,
        IF(SUM(portals_login_attempts) > 0, 1, 0) AS portals_login_attempts,
        IF(SUM(portals_login_failures) > 0, 1, 0) AS portals_login_failures,
        IF(SUM(portals_site_unique_auth) > 0, 1, 0) AS portals_site_unique_auth
      FROM
          quantum_metric_agg_portals
      WHERE denver_date >= '${hiveconf:START_DATE}'
        AND denver_date <  '${hiveconf:END_DATE}'
        AND application_name is not null
      GROUP BY
        device_id,
        application_name,
        app_version,
        denver_date
    ) sub
  GROUP BY
    denver_date,
    application_name,
    application_version
    grouping sets(
    (denver_date),
    (denver_date,application_name),
    (denver_date,application_name,application_version)
    )
) map_query
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

SELECT "\n\nFor: 4 Active HHs (SpecMobile)\n\n";
--Active HHs (SpecMobile)
INSERT INTO asp_quality_kpi_core PARTITION(denver_date)
SELECT
  'total' timeframe,
  grouping_id,
  LOWER(application_name) AS application_name,
  application_version,
  metric_name,
  metric_value,
  denver_date
FROM(
  SELECT
    grouping__id as grouping_id,
    denver_date,
    application_name,
    application_version,
    MAP(
      count(distinct portals_unique_acct_key), 'portals_site_unique_hh'
    ) tmp_map
  FROM
    (
      SELECT DISTINCT
        portals_unique_acct_key,
        application_name,
        application_version,
        SUBSTRING(epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10) denver_date
      FROM
        ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
      WHERE application_name in ('SpecMobile')
    ) A
  GROUP BY
    denver_date,
    application_name,
    application_version
    grouping sets(
    (denver_date),
    (denver_date,application_name),
    (denver_date,application_name,application_version)
    )
) sub
LATERAL VIEW EXPLODE(tmp_map) tmp_map AS metric_value, metric_name
;

SELECT "\n\nFor: 5 Devices Login (SpecMobile)\n\n";
--Devices Login (SpecMobile)
INSERT INTO asp_quality_kpi_core PARTITION(denver_date)
SELECT
  'total' timeframe,
  grouping_id,
  LOWER(application_name) AS application_name,
  application_version,
  metric_name,
  metric_value,
  denver_date
FROM(
  SELECT
    grouping__id as grouping_id,
    SUBSTRING(epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10) AS denver_date,
    application_name,
    application_version,
    MAP(
      COUNT(DISTINCT(if(message__name = 'loginStop' and operation__success = false,visit__device__uuid,null))), 'portals_login_failures_devices',
      COUNT(DISTINCT(if(message__name = 'loginStart',visit__device__uuid,null))), 'portals_login_attempts_devices'
    ) as tmp_map
  FROM
    ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER}
  WHERE application_name in ('SpecMobile')
  GROUP BY
    SUBSTRING(epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10),
    application_name,
    application_version
    grouping sets(
    (SUBSTRING(epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10)),
    (SUBSTRING(epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10),application_name),
    (SUBSTRING(epoch_timestamp(received__timestamp, 'America/Denver'), 0, 10),application_name,application_version)
    )
) sub
LATERAL VIEW EXPLODE(tmp_map) tmp_map AS metric_value, metric_name
;
SELECT "\n\nFor: 6 Core End\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_core_events_oneday_${env:CLUSTER} PURGE;
