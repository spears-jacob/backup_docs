USE ${env:ENVIRONMENT};

SET hive.auto.convert.join=false;

SET RollingLag=8;

SET TabPrefix=asp_sbnet_;

SET Domain=sb;

SET Company=L-CHTR;

SET TabView=asp_v_sbnet_events;

SET PageName=Login;

SET PageAlias=Login;

-----------------------------------------------------
--STEP 1
--Drop and rebuild temp tables
-----------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids PURGE
;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids
(
  visit__visit_id STRING
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial PURGE
;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial
(
  visit__visit_id STRING,
  message__timestamp BIGINT,
  min_sequence_number BIGINT,
  min_partition_date_hour_utc STRING
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events PURGE
;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
(
  visit__visit_id STRING,
  message__category STRING,
  message__name STRING,
  message__timestamp BIGINT,
  message__sequence_number BIGINT,
  page_name STRING,
  partition_date_utc STRING,
  partition_date_hour_utc STRING
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp PURGE
;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp
(
  count_distinct_entries_visits INT,
  message__category STRING,
  message__name STRING,
  page_name STRING,
  date_denver STRING
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits PURGE
;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits
(
  visit_id STRING,
  ct_bounced_visits INT
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page PURGE
;
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page
(
  page_name STRING,
  count_distinct_bounce_visits INT,
  date_denver STRING
)
;

SELECT '

*****-- End STEP 1: Drop and rebuild temp tables --*****

';

-----------------------------------------------------
--STEP 2
--Get Distinct event history for rolling 7 days
-----------------------------------------------------

INSERT OVERWRITE TABLE  ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids
SELECT DISTINCT visit__visit_id
FROM ${hiveconf:TabView}
WHERE (partition_date_hour_utc >= CONCAT(DATE_SUB("${env:START_DATE}",${hiveconf:RollingLag}),'_00')
  AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
;

SELECT '

*****-- End STEP 2: Compile Visit IDs that exist within range --*****

';

-----------------------------------------------------
--STEP 3
--Find Minimum timestamp and sequence for previously identified visit IDs
-----------------------------------------------------

INSERT OVERWRITE TABLE ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial
SELECT  ee.visit__visit_id,
        MIN(sbe.message__timestamp)       AS message__timestamp,
        MIN(sbe.message__sequence_number) AS min_sequence_number,
        MIN(sbe.partition_date_hour_utc)  AS min_partition_date_hour_utc
FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids ee
JOIN ${hiveconf:TabView} sbe
  ON ee.visit__visit_id = sbe.visit__visit_id
WHERE (sbe.partition_date_hour_utc >= CONCAT(DATE_SUB("${env:START_DATE}",2*${hiveconf:RollingLag}),'_00')
    AND sbe.partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ee.visit__visit_id
;

SELECT '

*****-- End STEP 3: Filter out Visit IDs outside of range --*****

';

-----------------------------------------------------
--STEP 4
--Pull only visit IDs that BEGIN within the rolling 7-day timeframe
-----------------------------------------------------

INSERT OVERWRITE TABLE ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
SELECT  eei.visit__visit_id,
        sbe2.message__category,
        sbe2.message__name,
        sbe2.message__timestamp,
        sbe2.message__sequence_number,
        sbe2.state__view__current_page__page_name AS page_name,
        sbe2.partition_date_utc,
        sbe2.partition_date_hour_utc
FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial eei
JOIN ${hiveconf:TabView} sbe2
  ON eei.visit__visit_id = sbe2.visit__visit_id
WHERE (sbe2.partition_date_hour_utc >= ("${env:START_DATE_TZ}")
      AND  sbe2.partition_date_hour_utc < ("${env:END_DATE_TZ}"))
  AND eei.min_partition_date_hour_utc>= ("${env:START_DATE_TZ}")
;

SELECT '

*****-- End STEP 4: Build entry_events source table --*****

';

-----------------------------------------------------
--STEP 5
--Identify distinct entries
-----------------------------------------------------

INSERT OVERWRITE TABLE ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp
SELECT  COUNT(DISTINCT me.visit_id) AS count_distinct_entries_visits,
        sbne.message__category,
        sbne.message__name,
        sbne.page_name,
        ${env:ape} AS date_denver
FROM
  ( SELECT visit__visit_id AS visit_id,
            MIN(message__timestamp) AS min_timestamp,
            MIN(message__sequence_number) AS min_sequence_number
    FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
    WHERE page_name IS NOT NULL
    GROUP BY visit__visit_id
  ) me
LEFT JOIN  ${env:TMP_db}.${hiveconf:TabPrefix}entry_events sbne
  ON me.visit_id = sbne.visit__visit_id
  AND me.min_timestamp = sbne.message__timestamp
  AND me.min_sequence_number = sbne.message__sequence_number
GROUP BY
  sbne.message__category,
  sbne.message__name,
  sbne.page_name,
  ${env:ape}
;

SELECT '

*****-- End STEP 5: L-CHTR Entry Calculations --*****

';

-----------------------------------------------------
--STEP 6
--Identify bounced visits
-----------------------------------------------------

INSERT OVERWRITE TABLE ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits
SELECT  visit__visit_id AS visit_id,
        COUNT(*) AS ct_bounced_visits
FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
GROUP BY  visit__visit_id
HAVING COUNT(*) = 1
;

INSERT OVERWRITE TABLE ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page
SELECT  sbne.page_name,
        COUNT(DISTINCT sbne.visit__visit_id) AS count_distinct_bounce_visits,
        ${env:ape} AS date_denver
FROM ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits bv
LEFT JOIN ${env:TMP_db}.${hiveconf:TabPrefix}entry_events sbne
  ON bv.visit_id = sbne.visit__visit_id
GROUP BY  sbne.page_name,
          ${env:ape}
;

SELECT '

*****-- End STEP 6: L-CHTR Bounce Calculations --*****

';

-----------------------------------------------------
--STEP 7
--Insert into agg table -- FINAL STEP
-----------------------------------------------------

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
         SELECT  'bounces_${hiveconf:PageAlias}' AS metric,
                  SUM(bo.count_distinct_bounce_visits) AS value,
                  'visits' AS unit,
                  '${hiveconf:Domain}' AS domain,
                  '${hiveconf:Company}' AS company,
                  ${env:apl} AS ${env:pf}
        FROM ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page bo
        LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm ON bo.date_denver = fm.partition_date
            WHERE (bo.date_denver >= ("${env:START_DATE}") AND bo.date_denver < ("${env:END_DATE}"))
            AND page_name = '${hiveconf:PageName}'
        GROUP BY
        ${env:apl},
        '${hiveconf:Company}'
        ) q
;


INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
         SELECT  'entries_${hiveconf:PageAlias}' AS metric,
                  SUM(count_distinct_entries_visits) AS value,
                  'visits' AS unit,
                  '${hiveconf:Domain}' AS domain,
                  '${hiveconf:Company}' AS company,
                  ${env:apl} AS ${env:pf}
          FROM ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp emt
          LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm ON emt.date_denver = fm.partition_date
            WHERE (emt.date_denver >= ("${env:START_DATE}") AND emt.date_denver < ("${env:END_DATE}"))
              AND page_name = '${hiveconf:PageName}'
          GROUP BY
          ${env:apl},
          '${hiveconf:Company}'
          ) q
;
SELECT '

*****-- End STEP 7: agg_raw table insert --*****

';

SELECT '

***** END BOUNCE & ENTRY CALCULATIONS *****

';

-----------------------------------------------------
---- ***** END BOUNCE & ENTRY CALCULATIONS ***** ----
-----------------------------------------------------
