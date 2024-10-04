use ${env:ENVIRONMENT};

SELECT "############################################################################################################################### ";
SELECT "### Running load_specguide_wb_netflix_daily.hql for load date: '${hiveconf:LOAD_DATE}' ";
SELECT "### Converting UTC to Denver time for this data.";

SELECT "### Alter settings before pulling Venona data ";

SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.tez.container.size=16000;


SELECT "### Step One - pull Netflix Events from Venona Events ";
SELECT "### Overwrite temp table with data for '${hiveconf:LOAD_DATE}' ";
SELECT "### First part in Step One - pull Netflix activity for App Lobby, Launches, and Upgrades ";

INSERT OVERWRITE TABLE ${env:TMP_db}.specguide_wb_netflix_events
SELECT
      v1.visit__visit_id,
      v1.visit__device__uuid,
      v1.account__mac_id_aes256_standard,
      v1.account__number_aes256,
      v1.message__category,
      v1.message__name,
      v1.message__sequence_number,
      v1.received__timestamp,
      v1.visit__application_details__app_version,
      v1.state__view__current_page__page_type,
      v1.state__view__current_page__elements__standardized_name,
      v1.state__view__current_page__elements__ui_name,
      v1.state__view__modal__name,
      v1.state__view__content__identifiers__tms_guide_id,
      v1.state__view__content__identifiers__tms_program_id,
      v1.source_application_name,
      IF (v1.upgrade_visit_id = 'X',NULL,v1.upgrade_visit_id),
      IF (v1.launch_visit_id = 'X',NULL,v1.launch_visit_id),
      IF (v1.failed_launch_visit_id = 'X',NULL,v1.failed_launch_visit_id),
      v1.source_criteria,
      v1.det_env,
      current_date() AS load_date,
      v1.partition_date_denver
FROM (
    SELECT
          ven.visit__visit_id,
          ven.visit__device__uuid,
          prod.aes_encrypt256(lower(prod.aes_decrypt(ven.visit__device__uuid))) AS account__mac_id_aes256_standard,
          prod.aes_encrypt256(prod.aes_decrypt(ven.visit__account__account_number)) AS account__number_aes256,
          ven.message__category,
          ven.message__name,
          ven.message__sequence_number,
          ven.received__timestamp,
          ven.visit__application_details__app_version,
          ven.state__view__current_page__page_type,
          ven.state__view__current_page__elements__standardized_name,
          ven.state__view__current_page__elements__ui_name,
          ven.state__view__modal__name,
          ven.state__view__content__identifiers__tms_guide_id,
          ven.state__view__content__identifiers__tms_program_id,
          ven.visit__application_details__application_name AS source_application_name,
          IF(ven.state__view__current_page__elements__ui_name = 'Upgrade', ven.visit__visit_id, 'X') as upgrade_visit_id,
          IF(ven.visit__application_details__app_version = '1.7.10' AND ven.message__name != 'error',ven.visit__visit_id,
                IF(ven.state__view__current_page__elements__ui_name = 'Launch App',ven.visit__visit_id,'X')
            ) AS launch_visit_id,
          IF(ven.visit__application_details__app_version = '1.7.10' AND ven.message__name = 'error', ven.visit__visit_id,'X') as failed_launch_visit_id,

          -- CASE Statement not working here - vertex errors, so switch to nested IF statements
          IF(ven.state__view__current_page__elements__ui_name = 'Upgrade','Upgrade UI Name',
              IF(ven.visit__application_details__app_version = '1.7.10' AND ven.message__name != 'error','Launch App Lobby',
                 IF(ven.state__view__current_page__elements__ui_name = 'Launch App','Launch UI Name',
                    IF(ven.visit__application_details__app_version = '1.7.10' AND ven.message__name = 'error','Failed Launch App Lobby',
              'Other')))) AS source_criteria,
          ven.visit__application_details__environment["environmentName"] as det_env,
          prod.epoch_converter(ven.received__timestamp ,'America/Denver') as partition_date_denver
    FROM core_v_venona_events ven
    WHERE
              ven.partition_date_utc BETWEEN '${hiveconf:LOAD_DATE}' AND DATE_ADD('${hiveconf:LOAD_DATE}',1)
          AND (prod.epoch_converter(ven.received__timestamp ,'America/Denver') = '${hiveconf:LOAD_DATE}' )
          AND ven.visit__application_details__application_name = 'Spectrum Guide'
          AND ven.visit__device__uuid IS NOT NULL
          AND
               (
                  (ven.state__view__current_page__page_type = 'networkDetailsScreen'
                          AND
                                (
                                  ven.state__view__current_page__elements__ui_name = 'Launch App'
                                OR
                                  ven.state__view__current_page__elements__ui_name = 'Upgrade'
                                )
                  )
                  OR
                  ven.visit__application_details__app_version = '1.7.10'
                )
  ) v1
;



SELECT "### Second part in Step One - Pull Netflix activity for Netflix ONLY if in last two events in visit - add to temp table";
SELECT "### Insert into the temp specguide_wb_netflix_events table with data for '${hiveconf:LOAD_DATE}' ";

INSERT INTO ${env:TMP_db}.specguide_wb_netflix_events
SELECT
      ven.visit__visit_id,
      ven.visit__device__uuid,
      prod.aes_encrypt256(lower(prod.aes_decrypt(ven.visit__device__uuid))) AS account__mac_id_aes256_standard,
      prod.aes_encrypt256(prod.aes_decrypt(ven.visit__account__account_number)) AS account__number_aes256,
      ven.message__category,
      ven.message__name,
      ven.message__sequence_number,
      ven.received__timestamp,
      ven.visit__application_details__app_version,
      ven.state__view__current_page__page_type,
      ven.state__view__current_page__elements__standardized_name,
      ven.state__view__current_page__elements__ui_name,
      ven.state__view__modal__name,
      ven.state__view__content__identifiers__tms_guide_id,
      ven.state__view__content__identifiers__tms_program_id,
      ven.visit__application_details__application_name AS source_application_name,
      NULL as upgrade_visit_id,
      ven.visit__visit_id AS launch_visit_id,
      NULL as failed_launch_visit_id,
      'Launch Netflix' AS source_criteria,
      ven.visit__application_details__environment["environmentName"],
      current_date() AS load_date,
      prod.epoch_converter(ven.received__timestamp ,'America/Denver') as partition_date_denver
FROM core_v_venona_events ven
WHERE
          (ven.partition_date_utc BETWEEN '${hiveconf:LOAD_DATE}' AND DATE_ADD('${hiveconf:LOAD_DATE}',1) )
      AND (prod.epoch_converter(ven.received__timestamp ,'America/Denver') = '${hiveconf:LOAD_DATE}' )
      AND ven.visit__application_details__application_name = 'Spectrum Guide'
      AND ven.state__view__current_page__elements__ui_name = 'Netflix'
      AND ven.visit__device__uuid IS NOT NULL
      AND ven.visit__visit_id IN
          (
          SELECT DISTINCT subQ.visit__visit_id
          FROM
                  (
                        SELECT
                              ven2.visit__visit_id, ven2.state__view__current_page__elements__ui_name,
                              row_number() over (partition by ven2.visit__visit_id ORDER BY ven2.message__sequence_number DESC) AS row_nbr
                        FROM core_v_venona_events ven2
                        WHERE
                                  (ven2.partition_date_utc BETWEEN '${hiveconf:LOAD_DATE}' AND DATE_ADD('${hiveconf:LOAD_DATE}',1) )
                              AND (prod.epoch_converter(ven2.received__timestamp ,'America/Denver') = '${hiveconf:LOAD_DATE}' )
                              AND ven2.visit__visit_id IN
                                          (
                                          SELECT DISTINCT v.visit__visit_id
                                          FROM core_v_venona_events v
                                          WHERE
                                                    (v.partition_date_utc BETWEEN '${hiveconf:LOAD_DATE}' AND DATE_ADD('${hiveconf:LOAD_DATE}',1) )
                                                AND (prod.epoch_converter(v.received__timestamp ,'America/Denver') = '${hiveconf:LOAD_DATE}' )
                                                AND  v.state__view__current_page__elements__ui_name = 'Netflix'
                                                AND  v.visit__application_details__application_name = 'Spectrum Guide'
                                          )
                  ) subQ
          WHERE subQ.row_nbr <= 2
          )
;

----


DROP TABLE IF EXISTS ${env:TMP_db}.specguide_deployed PURGE;
CREATE TABLE ${env:TMP_db}.specguide_deployed AS
SELECT
      run_date,
      account__number_aes256,
			account__mac_id_aes256,
      lower(prod.aes_decrypt256 (account__mac_id_aes256)) as mac_decr,
			MAX(account__type) AS account__type,
			MAX(system__kma_desc) as system__kma_desc,
			MAX(sg_deployed_type) AS sg_deployed_type,
			MAX(service__category) AS service__category,
			MAX(customer__type) AS customer__type,
			MAX(account__category) AS account__category,
			MAX(system__controller_name) AS system__controller_name,
			MAX(TO_DATE(FROM_UNIXTIME(sg_account_deployed_timestamp))) AS deployed_date
FROM
			prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
WHERE
					(RECORD_INACTIVE_DATE IS NULL OR RECORD_INACTIVE_DATE >= '${hiveconf:LOAD_DATE}')
			AND TO_DATE(FROM_UNIXTIME(SG_ACCOUNT_DEPLOYED_TIMESTAMP)) <= '${hiveconf:LOAD_DATE}'
			AND RUN_DATE ='${hiveconf:LOAD_DATE}'
      AND equipment__model RLIKE '^SP'   -- World Box
      AND service__category='DOCSIS'
GROUP BY
      run_date,
      account__number_aes256,
      account__mac_id_aes256
      ;


SELECT "### Third part in Step One - Move events from temp table to specguide_wb_netflix_events table";
SELECT "### Overwrite the partition in the specguide_wb_netflix_events table with data for '${hiveconf:LOAD_DATE}' ";


INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.specguide_wb_netflix_events PARTITION (partition_date_denver = '${hiveconf:LOAD_DATE}')
SELECT
  tmp_n.visit__visit_id,
  tmp_n.visit__device__uuid,
  tmp_n.account__mac_id_aes256_standard,
  -- tmp_n.account__number_aes256,
  COALESCE(sg2.account__number_aes256,tmp_n.account__number_aes256) AS account__number_aes256,
  tmp_n.message__category,
  tmp_n.message__name,
  tmp_n.message__sequence_number,
  tmp_n.received__timestamp,
  tmp_n.visit__application_details__app_version,
  tmp_n.state__view__current_page__page_type,
  tmp_n.state__view__current_page__elements__standardized_name,
  tmp_n.state__view__current_page__elements__ui_name,
  tmp_n.state__view__modal__name,
  tmp_n.state__view__content__identifiers__tms_guide_id,
  tmp_n.state__view__content__identifiers__tms_program_id,
  tmp_n.source_application_name,
  tmp_n.upgrade_visit_id,
  tmp_n.launch_visit_id,
  tmp_n.failed_launch_visit_id,
  tmp_n.source_criteria,
  tmp_n.visit__application_details__environment,
  tmp_n.load_date
FROM ${env:TMP_db}.specguide_wb_netflix_events tmp_n
 --> USE LEFT JOIN TO PICK UP TEST ACCOUNTS FROM VENONA THAT WILL NOT BE IN BI DATA --> FEEDS METRIC AGG
 --> USE INNER JOIN FOR AGG TABLES THAT FEED TABLEAU SUBSCRIBER ONLY AGGS
LEFT JOIN ( SELECT * FROM ${env:TMP_db}.specguide_deployed) AS sg2
ON lower(prod.aes_decrypt(tmp_n.visit__device__uuid)) = sg2.mac_decr
AND tmp_n.partition_date_denver = sg2.run_date
WHERE (tmp_n.partition_date_denver = '${hiveconf:LOAD_DATE}')
;


-------
--  World Box Daily Activity for Netflix and VOD
-------
SELECT "### Step Two - pull IP Activity (Netflix and VOD) ";
SELECT "### Part one of Step Two - Overwrite temp table with Netflix activity for '${hiveconf:LOAD_DATE}'";

-- Note Netflix events data has lower-cased mac ids, where SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY are upper-cased
-- Left join used because Netflix data has Venona test accounts that are not in deployed table
-- IT IS NOT EXPECTED TO SEE ALL VENONA TEST ACCOUNTS IN THE BI DATA
INSERT OVERWRITE TABLE ${env:TMP_db}.specguide_worldbox_activity
SELECT DISTINCT
      events.account__mac_id_aes256_standard AS account__mac_id_aes256,
      COALESCE(sg2.account__number_aes256,events.account__number_aes256) AS account__number_aes256,
      coalesce(sg2.account__type,'VENONA TEST'),
      sg2.customer__type,
      'Netflix' AS application,
      CASE TRUE
            WHEN events.launch_visit_id IS NOT NULL THEN 'Netflix Launch'
            WHEN events.upgrade_visit_id IS NOT NULL THEN 'Upgrade'
            WHEN events.failed_launch_visit_id IS NOT NULL THEN 'Failed Launch'
      ELSE 'Other' END AS application_subtype,
      'Spectrum Guide' AS source_application_name,
      events.message__name,
      'venona_events' AS data_source,
      '${hiveconf:LOAD_DATE}' AS partition_date_denver,
      current_date() AS load_date
FROM ${env:TMP_db}.specguide_wb_netflix_events events
--> USE LEFT JOIN TO PICK UP TEST ACCOUNTS FROM VENONA THAT WILL NOT BE IN BI DATA --> FEEDS METRIC AGG
--> USE INNER JOIN FOR AGG TABLES THAT FEED TABLEAU SUBSCRIBER ONLY AGGS
LEFT JOIN ( SELECT * FROM ${env:TMP_db}.specguide_deployed) AS sg2
ON lower(prod.aes_decrypt(events.visit__device__uuid)) = sg2.mac_decr
AND events.partition_date_denver = sg2.run_date
WHERE (events.partition_date_denver = '${hiveconf:LOAD_DATE}')
;

--STEVE NOTES:
--1) WE SHOULD BE ABLE TO USE THE VOD DAILY TABLE FOR THIS IF WE UPDATE THE VOD TABLE TO BE AT A MAC LEVEL
--2) FURTHER WE WOULD NEED A WORLDBOX IDENTIFIER AT AN ACCOUNT LEVEL AS WELL AS A STB LEVEL. CURRENTLY THE IDENTIFIER IS AT THE ACCOUNT LEVEL.

-- VOD QUESTIONS
--3) WE HAVE ALWAYS RELIED ON PARTITION_DATE_TIME FROM VOD. SHOULD WE BE USING THE TIMESTAMP INSTEAD?
    --LOGIC THAT DERIVES PARTITION_DATE_TIME: cast(to_date(from_unixtime(unix_timestamp(start_timestamp,"MM/dd/yyyy HH:mm:ss"))) as string) as partition_date_time

SELECT "### Part two of Step Two - Add WorldBox VOD activity to temp table for '${hiveconf:LOAD_DATE}' ";
-- Note that VOD mac ids are lowercase before encrypting, but SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY are uppercase
-- Join to deployed table to restrict to World Boxes
INSERT INTO TABLE ${env:TMP_db}.specguide_worldbox_activity
SELECT DISTINCT
   events.session__equipment_mac_id_aes AS account__mac_id_aes256,
   sg2.account__number_aes256,
   sg2.account__type,
   sg2.customer__type,
   'VOD' AS application,
   events.asset__class_code AS application_subtype,
   events.title__service_category_name AS source_application_name,
   NULL AS message__name,
   'vod_concurrent_stream_session_event' AS data_source,
   '${hiveconf:LOAD_DATE}' AS partition_date_denver,
   current_date() AS load_date
FROM prod.vod_concurrent_stream_session_event events
INNER JOIN ( SELECT * FROM ${env:TMP_db}.specguide_deployed) AS sg2
    ON lower(prod.aes_decrypt(events.session__equipment_mac_id_aes)) = sg2.mac_decr
    AND prod.epoch_converter((events.session__start_timestamp*1000) ,'America/Denver') = sg2.run_date
    AND prod.epoch_converter((events.session__start_timestamp*1000) ,'America/Denver') = sg2.run_date
WHERE (events.partition_date_time BETWEEN '${hiveconf:LOAD_DATE}' AND DATE_ADD('${hiveconf:LOAD_DATE}',1) )
   AND (prod.epoch_converter((events.session__start_timestamp*1000) ,'America/Denver') = '${hiveconf:LOAD_DATE}' )
   AND events.title__service_category_name != 'ADS'   -- exclude Ads
   AND events.asset__class_code = 'MOVIE'
   AND events.session__viewing_in_s >= 1
   AND events.session__viewing_in_s < 14400
   AND events.session__is_error = FALSE --> exclude VOD errors, specific error detail not available
   AND events.session__vod_lease_sid IS NOT NULL --> Unique identifier for VOD content
;





SELECT "### Part three of Step Two - Load '${hiveconf:LOAD_DATE}' data from temp table to specguide_worldbox_activity table ";

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.specguide_worldbox_activity PARTITION (partition_date_denver = '${hiveconf:LOAD_DATE}')
SELECT
  account__mac_id_aes256_standard,
  account__number_aes256,
  account__type,
  customer__type,
  application,
  application_subtype,
  source_application_name,
  message__name,
  data_source,
  load_date
FROM ${env:TMP_db}.specguide_worldbox_activity
;



SELECT "### ### ### ### ";
SELECT "### Done loading specguide World Box Daily Aggregates for '${hiveconf:LOAD_DATE}'";
SELECT "### ### ### ### ";
