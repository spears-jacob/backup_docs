USE ${env:DASP_db};

-- TODO: find suitable parameters for tez. current split.maxsize and split.minsize parameters seem like are not used.
-- SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';
CREATE TEMPORARY FUNCTION aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256';

--------------------------------------------------------------------------------
------------------ Getting support search quantum event data ------------------- 
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_support_quantum_events PARTITION (denver_date)
SELECT  application_name,
        visit_id,
        device_id,
        CASE WHEN SUBSTR(page_name, -1)    = '/'
             THEN SUBSTR(page_name, 1, LENGTH(page_name) - 1)
             ELSE page_name END            AS page_name,
        journey,
        sub_journey,
        visit_unique_id,
        account_number,
        received__timestamp,
        message__name,
        operation__operation_type,
        search_id,
        search_text,
        seq_num,
        CASE WHEN SUBSTR(element_url, -1)  = '/'
             THEN SUBSTR(element_url, 1, LENGTH(element_url) - 1)
             ELSE element_url
             END                           AS element_url,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'articleHelpful'
             THEN 1 ELSE 0 END             AS helpful_yes,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'articleNotHelpful'
             THEN 1 ELSE 0 END             AS helpful_no,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'sendFeedback'
                  AND elements__selected_options = 'wasnt_what_i_searched'
             THEN 1 ELSE 0 END             AS wasnt_what_i_searched,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'sendFeedback'
                  AND elements__selected_options = 'incorrect_info'
             THEN 1 ELSE 0 END             AS incorrect_info,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'sendFeedback'
                  AND elements__selected_options = 'confusing'
             THEN 1 ELSE 0 END             AS confusing,
        page_id,
        denver_date
FROM (
  SELECT  visit__application_details__application_name AS application_name,
          visit__visit_id                              AS visit_id,
          visit__device__enc_uuid                      AS device_id,
          concat_ws(',',state__view__current_page__user_journey) as journey,
          concat_ws(',',state__view__current_page__user_sub_journey) as sub_journey,
          coalesce(aes_decrypt256(state__view__current_page__enc_page_title, 'aes256'),
                   CASE WHEN state__view__current_page__page_name = 'supportArticle'
                         AND regexp_extract(aes_decrypt256(state__view__current_page__enc_page_url, 'aes256'),"(\/support[A-Za-z\-\/0-9]+)") = ''
                        THEN NULL
                        WHEN state__view__current_page__page_name = 'supportArticle'
                        THEN regexp_extract(aes_decrypt256(state__view__current_page__enc_page_url, 'aes256'),"(\/support[A-Za-z\-\/0-9]+)")
                        ELSE state__view__current_page__page_name
                        END)      AS page_name,
          CONCAT(visit__visit_id,'-',
              CASE WHEN (visit__account__enc_account_number
                         NOT IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
                         OR visit__account__enc_account_number IS NOT NULL
                        )
                    THEN visit__account__enc_account_number
                    ELSE NULL
                    END)                   AS visit_unique_id,
          COALESCE(
              CASE WHEN (visit__account__enc_account_number
                         NOT IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
                         OR visit__account__enc_account_number IS NOT NULL
                        )
                    THEN visit__account__enc_account_number
                    ELSE NULL
                    END,"UNKNOWN")         AS account_number,
          received__timestamp,
          message__name,
          operation__operation_type,
          state__search__search_id         AS search_id,
          state__search__text              AS search_text,
          message__sequence_number         AS seq_num,
          state__view__current_page__elements__element_url           AS element_url,
          state__view__current_page__elements__standardized_name     AS elements__standardized_name,
          state__view__current_page__elements__selected_options[0]   AS elements__selected_options,
          state__view__current_page__page_id                         AS page_id,
          epoch_converter(received__timestamp, 'America/Denver')     AS denver_date
    FROM `${env:PCQE}`
   WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
     AND partition_date_hour_utc < '${env:END_DATE_TZ}')
     AND visit__application_details__application_name IN ('MySpectrum', 'SMB', 'SpecNet')
     AND state__view__current_page__app_section = 'support'
     )    A
;

--------------------------------------------------------------------------------
-------------------------- Getting support call data --------------------------- 
--------------------------------------------------------------------------------

SET CallDataLag=3;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_calls_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_support_calls_${env:CLUSTER}
AS
  SELECT DISTINCT encrypted_account_number_256 AS account_number,
                  call_inbound_key,
                  call_start_timestamp_utc
    FROM `${env:ACCC}`
   WHERE (call_end_date_utc >= "${env:STARTDATE_3day}" AND call_end_date_utc < "${env:END_DATE_1day}")
     AND enhanced_account_number = 0
;

--------------------------------------------------------------------------------
-------------------------- Getting visits with a call -------------------------- 
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_vc_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_support_vc_${env:CLUSTER}      AS
  SELECT  application_name,
          denver_date,
          page_name,
          journey,
          sub_journey,
          page_id,
          COUNT(DISTINCT visit_unique_id)  AS distinct_visits_with_calls,
          COUNT(1)                         AS pageviews_with_calls
    FROM ${env:DASP_db}.asp_support_quantum_events           v
    JOIN ${env:TMP_db}.asp_support_calls_${env:CLUSTER}     c
      ON v.account_number                  = c.account_number
   WHERE (v.denver_date >= "${env:STARTDATE_3day}" AND v.denver_date < "${env:END_DATE_1day}")
     AND v.message__name = 'pageView'
     AND v.page_name IS NOT NULL
     AND ((c.call_start_timestamp_utc - v.received__timestamp) >= 0 AND (c.call_start_timestamp_utc - v.received__timestamp) < 86400000)
  GROUP BY  application_name,
            denver_date,
            page_name,
            page_id,
            journey,
            sub_journey
;

--------------------------------------------------------------------------------
------------------ Getting support page level aggregate data ------------------- 
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_vt_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_support_vt_${env:CLUSTER}
AS
  SELECT  application_name,
          denver_date,
          page_name,
          journey,
          sub_journey,
          page_id,
          COUNT(DISTINCT device_id)        AS visitors,
          COUNT(DISTINCT CASE WHEN account_number NOT IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
                              THEN device_id
                              END)         AS authenticated_visitors,
          COUNT(DISTINCT CASE WHEN account_number NOT IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
                              THEN account_number
                              END)         AS households,
          COUNT(DISTINCT visit_id)         AS visits,
          SUM(CASE WHEN message__name      = 'pageView'
                   THEN 1
                   ELSE 0
                   END)                    AS pageviews,
          SUM(helpful_yes)                 AS helpful_yes,
          SUM(helpful_no)                  AS helpful_no,
          SUM(wasnt_what_i_searched)       AS wasnt_what_i_searched,
          SUM(incorrect_info)              AS incorrect_info,
          SUM(confusing)                   AS confusing,
          COUNT(DISTINCT CASE WHEN message__name = 'pageView'
                              THEN visit_unique_id
                              END)         AS total_distinct_visits
    FROM ${env:DASP_db}.asp_support_quantum_events
   WHERE (denver_date >= "${env:STARTDATE_3day}" AND denver_date < "${env:END_DATE_1day}")
     AND page_name IS NOT NULL
  GROUP BY  application_name,
            denver_date,
            page_id,
            page_name,
            journey,
            sub_journey
;

--------------------------------------------------------------------------------
----------------------- Getting support search instances ----------------------- 
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_search_instances_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_support_search_instances_${env:CLUSTER}
AS
SELECT  application_name,
        denver_date,
        page_name,
        journey,
        sub_journey,
        page_id,
        search_text,
        COUNT(1)                           AS search_instances
FROM (SELECT  application_name,
              denver_date,
              element_url                  AS page_name,
              journey,
              sub_journey,
              page_id,
              search_text,
              ROW_NUMBER() OVER (PARTITION BY element_url, search_id ORDER BY seq_num DESC) rn
      FROM ${env:DASP_db}.asp_support_quantum_events
      WHERE (denver_date >= "${env:STARTDATE_3day}" AND denver_date < "${env:END_DATE_1day}")
        AND operation__operation_type = 'searchResultSelected'
        AND search_text IS NOT NULL
        AND element_url IS NOT NULL
        AND search_id   IS NOT NULL
) A
WHERE rn = 1
GROUP BY  application_name,
          denver_date,
          page_name,
          page_id,
          search_text,
          journey,
          sub_journey
;

--------------------------------------------------------------------------------
---------------- Inserting data into support_content_agg table ----------------- 
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_content_agg_tableset${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_support_content_agg_tableset${env:CLUSTER}
AS
SELECT  COALESCE(vc.application_name,vt.application_name) AS application_name,
        COALESCE(vc.page_name,vt.page_name)               AS page_name,
        COALESCE(vc.journey,vt.journey)                   AS journey,
        COALESCE(vc.sub_journey,vt.sub_journey)           AS sub_journey,
        COALESCE(vc.page_id,vt.page_id)                   AS page_id,
        COALESCE(vc.pageviews_with_calls,0)               AS pageviews_with_calls,
        vt.visitors,
        vt.authenticated_visitors,
        vt.households,
        vt.visits,
        vt.pageviews,
        vt.helpful_yes,
        vt.helpful_no,
        vt.wasnt_what_i_searched,
        vt.incorrect_info,
        vt.confusing,
        COALESCE(vc.distinct_visits_with_calls,0)         AS distinct_visits_with_calls,
        vt.total_distinct_visits,
        CAST(distinct_visits_with_calls/total_distinct_visits AS decimal(12,4)) AS call_in_rate,
        CAST(NULL AS STRING) AS search_text,
        CAST(NULL AS STRING) AS search_instances,
        COALESCE(vc.denver_date, vt.denver_date)          AS denver_date
  FROM ${env:TMP_db}.asp_support_vc_${env:CLUSTER}                   vc
  FULL OUTER JOIN ${env:TMP_db}.asp_support_vt_${env:CLUSTER}        vt
   ON vc.application_name       = vt.application_name
   AND vc.page_name             = vt.page_name
   AND vc.page_id               = vt.page_id
   AND vc.denver_date           = vt.denver_date
;

INSERT INTO ${env:TMP_db}.asp_support_content_agg_tableset${env:CLUSTER}
SELECT  application_name,
        page_name,
        journey,
        sub_journey,
        page_id,
        NULL pageviews_with_calls,
        NULL visitors,
        NULL authenticated_visitors,
        NULL households,
        NULL visits,
        NULL pageviews,
        NULL helpful_yes,
        NULL helpful_no,
        NULL wasnt_what_i_searched,
        NULL incorrect_info,
        NULL confusing,
        NULL distinct_visits_with_calls,
        NULL total_distinct_visits,
        NULL call_in_rate,
        search_text,
        search_instances,
        denver_date
FROM ${env:TMP_db}.asp_support_search_instances_${env:CLUSTER}
;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_support_content_agg PARTITION (denver_date)
SELECT  
     application_name,
     page_name,
     journey,
     sub_journey,
     page_id,
     pageviews_with_calls,
     visitors,
     authenticated_visitors,
     households,
     visits,
     pageviews,
     helpful_yes,
     helpful_no,
     wasnt_what_i_searched,
     incorrect_info,
     confusing,
     distinct_visits_with_calls,
     total_distinct_visits,
     call_in_rate,
     search_text,
     search_instances,
     denver_date
FROM ${env:TMP_db}.asp_support_content_agg_tableset${env:CLUSTER}
;
--adding comment for commit
--------------------------------------------------------------------------------
---------------------- ***** DROP TEMPORARY TABLES ***** -----------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_calls_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_vc_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_vt_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_search_instances_${env:CLUSTER} PURGE;

--------------------------------------------------------------------------------
------------------------------- ***** END ***** --------------------------------
--------------------------------------------------------------------------------
