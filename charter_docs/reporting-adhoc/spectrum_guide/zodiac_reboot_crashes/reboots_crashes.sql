-- ============
-- == STEP 0: RESEARCH
-- ============

SELECT event_start_time, session_activity_id, reason,*
FROM prod.zodiac_events_utc
WHERE partition_date_utc = '2018-03-20'
AND session_activity_id IN  ('REBOOT', 'CRASH'
AND mac_id = 'UXerIN29AQevUeaAD588Aw=='
ORDER BY event_start_time



-- ============
-- == STEP 0: SET VARIABLES
-- ============
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET hive.support.concurrency=false;


SET BEGIN_DATE = '2018-01-01';
SET END_DATE = '2018-03-25';

-- ============
-- == STEP 1: CREATE INITIAL TABLE FOR ZODIAC EVENTS
-- ============
DROP TABLE IF EXISTS test.zodiac_view_testing;
CREATE TABLE IF NOT EXISTS test.zodiac_view_testing(
  MESSAGE_TYPE  string,
  mac_id      string,
  event_start_time     string,
  session_activity_id     string,
  reason string,
partition_date_denver      string )

PARTITIONED BY (partition_date_utc string);

-- ============
-- == STEP 1: LOAD ZODIAC RAW DATA
-- ============

INSERT OVERWRITE TABLE test.zodiac_view_testing PARTITION(partition_date_utc)
SELECT
MESSAGE_TYPE,
mac_id,
event_start_time,
session_activity_id,
reason,
CASE
    WHEN cast(REGEXP_REPLACE(substring(event_start_time,0,19),'T',' ') AS TIMESTAMP) IS NOT NULL
    THEN to_date(from_utc_timestamp(REGEXP_REPLACE(substring(event_start_time,0,19),'T',' '),'America/Denver'))
  ELSE partition_date_utc
  END
    AS partition_date_denver,
partition_date_utc


FROM prod.zodiac_events_utc
WHERE partition_date_utc BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
AND MESSAGE_TYPE IN ('activity') AND session_activity_id IN ('REBOOT','CRASH')
;


-- ============
-- == STEP 2: DERIVE AGG
-- ============
DROP TABLE IF EXISTS test_tmp.SG_P2_SUCCESS_RATES_DAILY PURGE;
CREATE TABLE test_tmp.SG_P2_SUCCESS_RATES_DAILY AS
SELECT
       b.run_date,
       -- b.account__type AS account_type,
       b.system__kma_desc as system__kma_desc,
       -- b.sg_deployed_type as sg_deployed_type,
       -- b.service__category AS service__category,
       -- b.customer__type as customer__type,
       -- b.account__category as account__category,
       b.system__controller_name as system__controller_name,
       'reboot' AS metric_category,
       CASE
              WHEN TRIM(a.reason) IN
              ('DVBS_OEM_REBOOT_SAM_FACTORY_RESET',
              'DVBS_REBOOT_CDS_COMPONENT_FAILURE_POWERUP',
              'DVBS_REBOOT_SIGNAL_SIGSEGV',
              'DVBS_OEM_REBOOT_SAM_CD_COMPLETED',
              'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_DONWLOAD_FIRMWARE_CODE_IMAGE',
              'DVBS_REBOOT_CMD2K_REBOOT_NOW',
              'DVBS_REBOOT_CDS_CRITICAL_COMPONENT_CANT_BE_LOADED',
              'DVBS_REBOOT_SIGNAL_SIGABRT',
              'DVBS_REBOOT_FROM_FRONT_PANEL',
              'DVBS_REBOOT_DIAG',
              'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_FLASH_FIRMWARE_CODE_IMAGE') THEN 'other'
              WHEN a.reason IS NOT NULL THEN  a.reason
              ELSE 'unknown'
              END AS reason,
       SUM(IF(session_activity_id = 'REBOOT',1,0)) AS count,
       size(collect_set(if(session_activity_id = 'REBOOT',b.account__number_aes256,NULL))) AS hhs,
       size(collect_set(if(session_activity_id = 'REBOOT',b.account__mac_id_aes256,NULL))) AS stbs
FROM test.zodiac_view_testing a
JOIN prod_lkp.sg_phaseii_deployed_macs_all_history b
ON a.mac_id=b.account__mac_id_aes256
AND a.partition_date_denver = b.run_date


WHERE b.run_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
AND b.account__type = 'SUBSCRIBER'
AND b.customer__type = 'Residential'
AND session_activity_id = 'REBOOT'
AND to_date(from_unixtime(b.sg_account_deployed_timestamp)) <= b.run_date
AND (b.record_inactive_date is NULL or b.record_inactive_date >= b.run_date)

GROUP BY
       b.run_date,
       -- b.account__type,
       b.system__kma_desc,
       -- b.sg_deployed_type ,
       -- b.service__category,
       -- b.customer__type ,
       -- b.account__category,
       b.system__controller_name,
       CASE
              WHEN TRIM(a.reason) IN
              ('DVBS_OEM_REBOOT_SAM_FACTORY_RESET',
              'DVBS_REBOOT_CDS_COMPONENT_FAILURE_POWERUP',
              'DVBS_REBOOT_SIGNAL_SIGSEGV',
              'DVBS_OEM_REBOOT_SAM_CD_COMPLETED',
              'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_DONWLOAD_FIRMWARE_CODE_IMAGE',
              'DVBS_REBOOT_CMD2K_REBOOT_NOW',
              'DVBS_REBOOT_CDS_CRITICAL_COMPONENT_CANT_BE_LOADED',
              'DVBS_REBOOT_SIGNAL_SIGABRT',
              'DVBS_REBOOT_FROM_FRONT_PANEL',
              'DVBS_REBOOT_DIAG',
              'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_FLASH_FIRMWARE_CODE_IMAGE') THEN 'other'
              WHEN a.reason IS NOT NULL THEN  a.reason
              ELSE 'unknown'
              END


UNION ALL

SELECT
       b.run_date,
       -- b.account__type AS account_type,
       b.system__kma_desc as system__kma_desc,
       -- b.sg_deployed_type as sg_deployed_type,
       -- b.service__category AS service__category,
       -- b.customer__type as customer__type,
       -- b.account__category as account__category,
       b.system__controller_name as system__controller_name,
       'reboot' AS metric_category,
       'all' AS reason,
       SUM(IF(session_activity_id = 'REBOOT',1,0)) AS count,
       size(collect_set(if(session_activity_id = 'REBOOT',b.account__number_aes256,NULL))) AS hhs,
       size(collect_set(if(session_activity_id = 'REBOOT',b.account__mac_id_aes256,NULL))) AS stbs
FROM test.zodiac_view_testing a
JOIN prod_lkp.sg_phaseii_deployed_macs_all_history b
ON a.mac_id=b.account__mac_id_aes256
AND a.partition_date_denver = b.run_date


WHERE b.run_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
AND b.account__type = 'SUBSCRIBER'
AND b.customer__type = 'Residential'
AND session_activity_id = 'REBOOT'
AND to_date(from_unixtime(b.sg_account_deployed_timestamp)) <= b.run_date
AND (b.record_inactive_date is NULL or b.record_inactive_date >= b.run_date)

GROUP BY
       b.run_date,
       -- b.account__type,
       b.system__kma_desc,
       -- b.sg_deployed_type ,
       -- b.service__category,
       -- b.customer__type ,
       -- b.account__category,
       b.system__controller_name

UNION ALL

SELECT
      b.run_date,
      -- b.account__type AS account_type,
      b.system__kma_desc as system__kma_desc,
      -- b.sg_deployed_type as sg_deployed_type,
      -- b.service__category AS service__category,
      -- b.customer__type as customer__type,
      -- b.account__category as account__category,
      b.system__controller_name as system__controller_name,
      'crash' AS metric_category,
      CASE
             WHEN TRIM(a.reason) IN
             ('DVBS_OEM_REBOOT_SAM_FACTORY_RESET',
             'DVBS_REBOOT_CDS_COMPONENT_FAILURE_POWERUP',
             'DVBS_REBOOT_SIGNAL_SIGSEGV',
             'DVBS_OEM_REBOOT_SAM_CD_COMPLETED',
             'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_DONWLOAD_FIRMWARE_CODE_IMAGE',
             'DVBS_REBOOT_CMD2K_REBOOT_NOW',
             'DVBS_REBOOT_CDS_CRITICAL_COMPONENT_CANT_BE_LOADED',
             'DVBS_REBOOT_SIGNAL_SIGABRT',
             'DVBS_REBOOT_FROM_FRONT_PANEL',
             'DVBS_REBOOT_DIAG',
             'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_FLASH_FIRMWARE_CODE_IMAGE') THEN 'other'
             WHEN a.reason  IS NOT NULL THEN  a.reason
             ELSE 'unknown'
             END AS reason,
      SUM(IF(session_activity_id = 'CRASH',1,0 )) AS count,
      size(collect_set(if(session_activity_id = 'CRASH',b.account__number_aes256,NULL))) AS hhs,
      size(collect_set(if(session_activity_id = 'CRASH',b.account__mac_id_aes256,NULL))) AS stbs
FROM test.zodiac_view_testing a
JOIN prod_lkp.sg_phaseii_deployed_macs_all_history b
ON a.mac_id=b.account__mac_id_aes256
AND a.partition_date_denver = b.run_date


WHERE b.run_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
AND b.account__type = 'SUBSCRIBER'
AND b.customer__type = 'Residential'
AND session_activity_id = 'CRASH'
AND to_date(from_unixtime(b.sg_account_deployed_timestamp)) <= b.run_date
AND (b.record_inactive_date is NULL or b.record_inactive_date >= b.run_date)

GROUP BY
      b.run_date,
      -- b.account__type,
      b.system__kma_desc,
      -- b.sg_deployed_type ,
      -- b.service__category,
      -- b.customer__type ,
      -- b.account__category,
      b.system__controller_name,
      CASE
             WHEN TRIM(a.reason) IN
             ('DVBS_OEM_REBOOT_SAM_FACTORY_RESET',
             'DVBS_REBOOT_CDS_COMPONENT_FAILURE_POWERUP',
             'DVBS_REBOOT_SIGNAL_SIGSEGV',
             'DVBS_OEM_REBOOT_SAM_CD_COMPLETED',
             'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_DONWLOAD_FIRMWARE_CODE_IMAGE',
             'DVBS_REBOOT_CMD2K_REBOOT_NOW',
             'DVBS_REBOOT_CDS_CRITICAL_COMPONENT_CANT_BE_LOADED',
             'DVBS_REBOOT_SIGNAL_SIGABRT',
             'DVBS_REBOOT_FROM_FRONT_PANEL',
             'DVBS_REBOOT_DIAG',
             'DVBS_OEM_REBOOT_SAM_CDL_FAILED_TO_FLASH_FIRMWARE_CODE_IMAGE') THEN 'other'
             WHEN a.reason  IS NOT NULL THEN  a.reason
             ELSE 'unknown'
             END

;



-- ============
-- == STEP 3: DERIVE AGG + ADD DEPLOYED COUNTS
-- ============
DROP TABLE IF EXISTS test.zodiac_reboot_crash_final;
CREATE TABLE test.zodiac_reboot_crash_final AS

SELECT
a.*,
sg_usage.bi_hhs AS deployed_hhs,
sg_usage.bi_stbs AS deployed_stbs

FROM test_tmp.SG_P2_SUCCESS_RATES_DAILY a
LEFT JOIN
  (SELECT
                partition_date_denver,
                system__kma_desc,
                system__controller_name,
                -- account__category,
                -- sg_deployed_type,
                -- service__category,
                -- customer__type,
                -- user_type AS account_type,
                SUM(current_deployed_household_count) AS bi_hhs,
                SUM(current_deployed_mac_count) AS bi_stbs,
                SUM(venona_sg_active_today_hh_count) AS active_venona_hh,
                SUM(venona_sg_active_today_mac_count) AS active_venona_stbs,
                SUM(zodiac_active_today_hh_count) AS active_zodiac_hh,
                SUM(zodiac_active_today_mac_count) AS active_zodiac_stb
          FROM prod.spectrum_guide_usage
          WHERE user_type = 'SUBSCRIBER'
          AND customer__type = 'Residential'
          AND partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
          GROUP BY
                partition_date_denver,
                system__kma_desc,
                system__controller_name
                -- account__category,
                -- sg_deployed_type,
                -- service__category,
                -- customer__type,
                -- user_type
              ) sg_usage
       ON

                 a.run_date = sg_usage.partition_date_denver --> ok
             AND a.system__kma_desc = sg_usage.system__kma_desc --> ok
             AND a.system__controller_name = sg_usage.system__controller_name --> ok
             -- AND a.account__category = sg_usage.account__category--> ok
             -- AND a.sg_deployed_type = sg_usage.sg_deployed_type --> ok
             -- AND a.service__category = sg_usage.service__category --> ok
             --
             -- AND a.customer__type = sg_usage.customer__type--> ok
             -- AND a.account_type = sg_usage.account_type--> ok

             ;
