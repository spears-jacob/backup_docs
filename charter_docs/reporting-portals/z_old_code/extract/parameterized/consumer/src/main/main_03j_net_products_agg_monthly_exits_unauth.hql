USE ${env:ENVIRONMENT};

SET hive.auto.convert.join=false;

SET RollingLag=8;

SET TabPrefix=asp_net_euauth;

SET Domain=resi;

SET Company=L-CHTR;

SET TabView=asp_v_net_events;

SET PageName=home-unauth;

SET PageAlias=exit_home-unauth;

SET MsgCategory=Page View;

-----------------------------------------------------
--STEP 1
--Drop and rebuild temp tables
--Remove   for debugging
-----------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids PURGE
;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids
(
  visit__visit_id STRING
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial PURGE
;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial
(
  visit__visit_id STRING,
  message__timestamp BIGINT,
  max_sequence_number BIGINT,
  max_partition_date STRING
  )
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events PURGE
;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
(
  visit__visit_id STRING,
  message__category STRING,
  message__name STRING,
  message__timestamp BIGINT,
  message__sequence_number BIGINT,
  page_name STRING,
  partition_date STRING
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}events_max_timestamp PURGE
;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}events_max_timestamp
(
  count_distinct_exit_visits  INT,
  message__category STRING,
  message__name STRING,
  page_name STRING,
  partition_date STRING,
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
WHERE partition_date >= "${env:START_DATE}"
  AND partition_date < DATE_ADD("${env:END_DATE}",${hiveconf:RollingLag})
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
        MAX(sbe.message__timestamp) AS message__timestamp,
        MAX(sbe.message__sequence_number) AS max_sequence_number,
        MAX(sbe.partition_date) AS max_partition_date
FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids ee
JOIN ${hiveconf:TabView} sbe
  ON ee.visit__visit_id = sbe.visit__visit_id
WHERE sbe.partition_date >= "${env:START_DATE}"
  AND sbe.partition_date < DATE_ADD("${env:END_DATE}",2*${hiveconf:RollingLag})
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
        sbe2.state__view__current_page__name AS page_name,
        sbe2.partition_date
FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_initial eei
JOIN ${hiveconf:TabView} sbe2
  ON eei.visit__visit_id = sbe2.visit__visit_id
WHERE (sbe2.partition_date >= ("${env:START_DATE}")
  AND  sbe2.partition_date < ("${env:END_DATE}"))
  AND eei.max_partition_date< ("${env:END_DATE}")
;

SELECT '

*****-- End STEP 4: Build entry_events source table --*****

';

-----------------------------------------------------
--STEP 5
--Identify distinct entries
-----------------------------------------------------

INSERT OVERWRITE TABLE ${env:TMP_db}.${hiveconf:TabPrefix}events_max_timestamp
SELECT  COUNT(DISTINCT me.visit_id) AS count_distinct_exit_visits ,
        ne.message__category,
        ne.message__name,
        ne.page_name,
        ne.partition_date ,
        ne.partition_date AS date_denver
FROM
  ( SELECT  visit__visit_id AS visit_id,
            MAX(message__timestamp) AS max_timestamp,
            MAX(message__sequence_number) AS max_sequence_number
    FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
    WHERE page_name IS NOT NULL
    GROUP BY visit__visit_id
  ) me
LEFT JOIN  ${env:TMP_db}.${hiveconf:TabPrefix}entry_events ne
  ON me.visit_id = ne.visit__visit_id
  AND me.max_timestamp = ne.message__timestamp
  AND me.max_sequence_number = ne.message__sequence_number
GROUP BY
  ne.message__category,
  ne.message__name,
  ne.page_name,
  ne.partition_date
;

SELECT '

*****-- End STEP 5: L-CHTR Entry Calculations --*****

';

-----------------------------------------------------
--STEP 6
--Insert into agg table -- FINAL STEP
-----------------------------------------------------


INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
         SELECT  'exits_${hiveconf:PageAlias}' AS metric,
                  SUM(count_distinct_exit_visits) AS value,
                  'visits' AS unit,
                  '${hiveconf:Domain}' AS domain,
                  '${hiveconf:Company}' AS company,
                  ${env:apl} AS ${env:pf}
        FROM (  SELECT page_name,
                       message__category,
                       count_distinct_exit_visits,
                       ${env:apl}
                FROM ${env:TMP_db}.${hiveconf:TabPrefix}events_max_timestamp ne
                LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm
                  ON date_denver = fm.partition_date
                WHERE (date_denver >= ("${env:START_DATE}") AND date_denver < ("${env:END_DATE}"))
                 AND message__name = '${hiveconf:PageName}'
                 AND message__category = '${hiveconf:MsgCategory}'
             ) ne
        GROUP BY ${env:apl},
                '${hiveconf:Company}'
        ) q
;


INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (
         SELECT  'entries_${hiveconf:PageAlias}' AS metric,
                  SUM(count_distinct_visits ) AS value,
                  'visits' AS unit,
                  '${hiveconf:Domain}' AS domain,
                  '${hiveconf:Company}' AS company,
                  ${env:apl} AS ${env:pf}
         FROM (   SELECt  count(distinct visit__visit_id) as count_distinct_visits,
                          ${env:apl}
                  FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events ne
                  LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm
                    ON ne.partition_date = fm.partition_date
                  WHERE (ne.partition_date >= ("${env:START_DATE}")
                   AND ne.partition_date < ("${env:END_DATE}"))
                   AND message__name = '${hiveconf:PageName}'
                   AND message__category = '${hiveconf:MsgCategory}'
                  group by ${env:apl}
               ) ne
        GROUP BY   ${env:apl},
                   '${hiveconf:Company}'
      ) q
;
SELECT '

*****-- End STEP 6: agg_raw table insert --*****

';

SELECT '

***** END EXIT & ENTRY CALCULATIONS *****

';

-----------------------------------------------------
---- ***** END EXIT & ENTRY CALCULATIONS ***** ----
-----------------------------------------------------
