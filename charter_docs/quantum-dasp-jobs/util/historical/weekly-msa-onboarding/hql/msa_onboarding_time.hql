USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;

set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled = false;

set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';


INSERT OVERWRITE TABLE asp_msa_onboarding_time PARTITION(label_date_denver, grain)
select
       CASE
           WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
           WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
           WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
           WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
                 OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
           ELSE visit__account__details__mso
       END AS mso,
       visit__device__device_type AS device_type,
       'NA' as tutorial_completed,
       'page_time' as metric_type,
       state__view__previous_page__page_name as pagename,
       SUM(1) as number_of_item,
       AVG(state__view__previous_page__page_viewed_time_ms)/1000 as page_avg_sec,
       percentile(cast(state__view__previous_page__page_viewed_time_ms as BIGINT), 0.25)/1000 as page_25th_sec,
       percentile(cast(state__view__previous_page__page_viewed_time_ms as BIGINT), 0.50)/1000 as page_50th_sec,
       percentile(cast(state__view__previous_page__page_viewed_time_ms as BIGINT), 0.75)/1000 as page_75th_sec,
       '${hiveconf:ProcessTimestamp}' as process_date_time_denver,
       '${hiveconf:ProcessUser}' AS process_identity,
       '${hiveconf:label_date_denver}' AS label_date_denver,
       '${hiveconf:grain}' AS grain
  from asp_v_quantum_events_portals_msa
 WHERE (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}' and partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
   AND message__name ='pageView'
   AND state__view__previous_page__page_name is not null
   AND state__view__previous_page__page_viewed_time_ms > 0
 group by epoch_converter(received__timestamp, 'America/Denver'),
       CASE
          WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
          WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
          WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
          WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
                OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
          ELSE visit__account__details__mso
       END,
       visit__device__device_type,
       state__view__previous_page__page_name
;


DROP TABLE IF EXISTS ${env:TMP_db}.asp_msa_onboarding_flow_weekly;

CREATE TEMPORARY table if not exists ${env:TMP_db}.asp_msa_onboarding_flow_weekly
 ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION 'hdfs:///tmp/tmp_msa_onboarding/asp_msa_onboarding_flow_weekly'
 TBLPROPERTIES('orc.compress'='snappy') AS
select max(tutorialEnd) OVER (PARTITION BY visit_id) as endtutorial,
       max(tutorialEndTime) OVER (PARTITION BY visit_id) as endtutorialtime,
       min(message_timestamp) OVER (PARTITION BY visit_id) as visit_start,
       max(message_timestamp) OVER (PARTITION BY visit_id) as visit_end,
       *
  from
      (select
              epoch_converter(received__timestamp, 'America/Denver') as date_denver,
              CASE
                WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
                WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
                WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
                WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
                      OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
                ELSE visit__account__details__mso
              END AS mso,
              visit__device__device_type AS device_type,
              visit__account__enc_account_number as acct_id,
              visit__device__enc_uuid AS device_id,
              visit__visit_id as visit_id,
              message__sequence_number as visit_order,
              message__name as msg_name,
              message__timestamp as message_timestamp,
              state__view__previous_page__page_name as prev_pagename,
              state__view__previous_page__page_viewed_time_ms as prev_page_viewed_time_ms,
              CASE
                   when message__name = 'pageView'
                          AND state__view__current_page__page_name = 'onboardingTour'
                          and state__view__current_page__page_title = 'LoginOnboarding.json Page: 4'
                   then message__sequence_number
                   ELSE 0
              END AS tutorialEnd,
              CASE
                   when message__name = 'pageView'
                          AND state__view__current_page__page_name = 'onboardingTour'
                          and state__view__current_page__page_title = 'LoginOnboarding.json Page: 4'
                   then message__timestamp
                   ELSE 0
              END AS tutorialEndTime,
              state__view__current_page__page_name as pagename,
              state__view__current_page__page_title AS pagetitle
         from asp_v_quantum_events_portals_msa
        WHERE (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}' and partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
        Order by visit_id, visit_order) a;

INSERT INTO TABLE asp_msa_onboarding_time PARTITION(label_date_denver, grain)
select
       mso,
       device_type,
       CASE WHEN endtutorial > 0 THEN 1 ELSE 0 END as tutorial_completed,
       'session_time' as metric_type,
       'NA' as pagename,
       count(distinct visit_id) as number_of_item,
       AVG(visit_length2_sec) as visit_length2_avg_sec,
       percentile(cast(visit_length2_sec as BIGINT), 0.25) as visit_lenth2_25th_sec,
       percentile(cast(visit_length2_sec as BIGINT), 0.50) as visit_lenth2_50th_sec,
       percentile(cast(visit_length2_sec as BIGINT), 0.75) as visit_lenth2_75th_sec,
       '${hiveconf:ProcessTimestamp}' as process_date_time_denver,
       '${hiveconf:ProcessUser}' AS process_identity,
       '${hiveconf:label_date_denver}' AS label_date_denver,
       '${hiveconf:grain}' AS grain
  FROM
      (select date_denver,
              mso,
              device_type,
              visit_id,
              MAX(endtutorial) as endtutorial,
              sum(prev_page_viewed_time_ms)/1000 as visit_length_sec,
              SUM(CASE WHEN endtutorial > 0 and visit_order > endtutorial
                       THEN prev_page_viewed_time_ms
                       else 0
                  END)/1000 AS visit_after_tutorial_length_sec,
              MAX(visit_end-visit_start)/1000 as visit_length2_sec,
              MAX(CASE WHEN endtutorialtime > 0
                       THEN (visit_end-endtutorialtime)/1000
                       ELSE 0
              END) AS visit_after_tutorial_length2_sec
         from ${env:TMP_db}.asp_msa_onboarding_flow_weekly
        where msg_name='pageView'
        group by date_denver,
                 mso,
                 device_type,
                 visit_id) a
 group by date_denver,
          CASE WHEN endtutorial > 0 THEN 1 ELSE 0 END,
          mso,
          device_type;

INSERT INTO TABLE asp_msa_onboarding_time PARTITION(label_date_denver, grain)
select
     mso,
     device_type,
     CASE WHEN endtutorial > 0 THEN 1 ELSE 0 END as tutorial_completed,
     'after_tutorial_session_time' as metric_type,
     'NA' as pagename,
     count(distinct visit_id) as number_of_item,
     avg(visit_after_tutorial_length2_sec) as visit_after_tutorial_length2_avg_sec,
     percentile(cast(visit_after_tutorial_length2_sec as BIGINT), 0.25) as visit_after_tutorial_lenth2_25th_sec,
     percentile(cast(visit_after_tutorial_length2_sec as BIGINT), 0.50) as visit_after_tutorial_lenth2_50th_sec,
     percentile(cast(visit_after_tutorial_length2_sec as BIGINT), 0.75) as visit_after_tutorial_lenth2_75th_sec,
     '${hiveconf:ProcessTimestamp}' as process_date_time_denver,
     '${hiveconf:ProcessUser}' AS process_identity,
     '${hiveconf:label_date_denver}' AS label_date_denver,
     '${hiveconf:grain}' AS grain
FROM
    (select date_denver,
            mso,
            device_type,
            visit_id,
            MAX(endtutorial) as endtutorial,
            sum(prev_page_viewed_time_ms)/1000 as visit_length_sec,
            SUM(CASE WHEN endtutorial > 0 and visit_order > endtutorial
                     THEN prev_page_viewed_time_ms
                     else 0
                END)/1000 AS visit_after_tutorial_length_sec,
            MAX(visit_end-visit_start)/1000 as visit_length2_sec,
            MAX(CASE WHEN endtutorialtime > 0
                     THEN (visit_end-endtutorialtime)/1000
                     ELSE 0
            END) AS visit_after_tutorial_length2_sec
       from ${env:TMP_db}.asp_msa_onboarding_flow_weekly
      where msg_name='pageView'
      group by date_denver,
               mso,
               device_type,
               visit_id) a
group by date_denver,
         CASE WHEN endtutorial > 0 THEN 1 ELSE 0 END,
         mso,
         device_type;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_msa_onboarding_flow_weekly;
