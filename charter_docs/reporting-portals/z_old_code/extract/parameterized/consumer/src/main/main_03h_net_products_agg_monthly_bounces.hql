USE ${env:ENVIRONMENT};

SET hive.auto.convert.join=false;

SET RollingLag=8;

SET TabPrefix=asp_net_;

SET Domain=resi;

SET Company=L-CHTR;

SET TabView=asp_v_net_events;

SET PageName=home-unauth;

SET PageAlias=home-unauth;

-----------------------------------------------------
--STEP 1
--Drop and rebuild temp tables
--Remove TEMPORARY for debugging
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
  min_sequence_number BIGINT,
  min_partition_date STRING
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

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp PURGE
;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp
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
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits
(
  visit_id STRING,
  ct_bounced_visits INT
)
;
----

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page PURGE
;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page
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
WHERE (partition_date >= DATE_SUB("${env:START_DATE}",${hiveconf:RollingLag})
  AND partition_date < ("${env:END_DATE}"))
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
        MIN(sbe.message__timestamp) AS message__timestamp,
        MIN(sbe.message__sequence_number) AS min_sequence_number,
        MIN(sbe.partition_date) AS min_partition_date
FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events_visit_ids ee
JOIN ${hiveconf:TabView} sbe
  ON ee.visit__visit_id = sbe.visit__visit_id
WHERE (sbe.partition_date >= DATE_SUB("${env:START_DATE}",2*${hiveconf:RollingLag})
    AND sbe.partition_date < ("${env:END_DATE}"))
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
  AND eei.min_partition_date>= ("${env:START_DATE}")
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
        ne.message__category,
        ne.message__name,
        ne.page_name,
        ne.partition_date AS date_denver
FROM
  ( SELECT  visit__visit_id AS visit_id,
            MIN(message__timestamp) AS min_timestamp,
            MIN(message__sequence_number) AS min_sequence_number
    FROM ${env:TMP_db}.${hiveconf:TabPrefix}entry_events
    WHERE page_name IS NOT NULL
    GROUP BY visit__visit_id
  ) me
LEFT JOIN  ${env:TMP_db}.${hiveconf:TabPrefix}entry_events ne
  ON me.visit_id = ne.visit__visit_id
  AND me.min_timestamp = ne.message__timestamp
  AND me.min_sequence_number = ne.message__sequence_number
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
SELECT  ne.page_name,
        COUNT(DISTINCT ne.visit__visit_id) AS count_distinct_bounce_visits,
        ne.partition_date AS date_denver
FROM ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits bv
LEFT JOIN ${env:TMP_db}.${hiveconf:TabPrefix}entry_events ne
  ON bv.visit_id = ne.visit__visit_id
GROUP BY
  ne.page_name,
  ne.partition_date
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
                  SUM(ne.count_distinct_bounce_visits) AS value,
                  'visits' AS unit,
                  '${hiveconf:Domain}' AS domain,
                  '${hiveconf:Company}' AS company,
                  ${env:apl} AS ${env:pf}
        FROM (SELECT page_name,
                     count_distinct_bounce_visits,
                     date_denver,
                     date_denver as partition_date
              FROM ${env:TMP_db}.${hiveconf:TabPrefix}bounced_visits_by_page) ne
        LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm ON ne.date_denver = fm.partition_date
            WHERE (ne.date_denver >= ("${env:START_DATE}") AND ne.date_denver < ("${env:END_DATE}"))
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
          FROM (SELECt  count_distinct_entries_visits,
                        message__category ,
                        message__name ,
                        page_name ,
                        date_denver ,
                        date_denver as partition_date
                FROM ${env:TMP_db}.${hiveconf:TabPrefix}events_min_timestamp )ne
          LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm ON ne.date_denver = fm.partition_date
            WHERE (ne.date_denver >= ("${env:START_DATE}") AND ne.date_denver < ("${env:END_DATE}"))
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
