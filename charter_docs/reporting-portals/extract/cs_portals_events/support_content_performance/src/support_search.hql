USE ${env:ENVIRONMENT};

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT '***** getting support search quantum event data ******';
;

INSERT OVERWRITE TABLE asp_support_quantum_events PARTITION (denver_date)
SELECT  application_name,
        visit_id,
        device_id,
        CASE WHEN SUBSTR(page_name, -1)    = '/'
             THEN SUBSTR(page_name, 1, LENGTH(page_name) - 1)
             ELSE page_name END            AS page_name,
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
        denver_date
FROM (
  SELECT  visit__application_details__application_name AS application_name,
          visit__visit_id                  AS visit_id,
          visit__device__enc_uuid          AS device_id,
          coalesce(state__view__current_page__page_title,
                   CASE WHEN state__view__current_page__page_name = 'supportArticle'
                         AND regexp_extract(state__view__current_page__page_url,"(\/support[A-Za-z\-\/0-9]+)") = ''
                        THEN NULL
                        WHEN state__view__current_page__page_name = 'supportArticle'
                        THEN regexp_extract(state__view__current_page__page_url,"(\/support[A-Za-z\-\/0-9]+)")
                        ELSE state__view__current_page__page_name
                        END,
                   state__view__current_page__page_name)      AS page_name,
          CONCAT(visit__visit_id,'-',
               CASE WHEN prod.aes_decrypt256(visit__account__enc_account_number) IS NULL
                    THEN NULL
                    ELSE visit__account__enc_account_number
                    END)                   AS visit_unique_id,
          COALESCE(
               CASE WHEN prod.aes_decrypt256(visit__account__enc_account_number) IS NULL
                    THEN NULL
                    ELSE visit__account__enc_account_number
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
          epoch_converter(received__timestamp, 'America/Denver')     AS denver_date
    FROM core_quantum_events_portals_v
   WHERE partition_date_hour_utc           >= '${env:START_DATE_TZ}'
     AND partition_date_hour_utc           <  '${env:END_DATE_TZ}'
     AND state__view__current_page__app_section = 'support'    )    A
;

SET CallDataLag=3;
SELECT '***** getting support call data ******';
-- similar to cs_chr_calls
DROP TABLE IF EXISTS asp_support_calls PURGE;
CREATE TEMPORARY TABLE asp_support_calls   AS
  SELECT DISTINCT encrypted_account_number_256 AS account_number,
                  call_inbound_key,
                  call_start_timestamp_utc
    FROM red_cs_call_care_data_v
   WHERE ( call_end_date_utc               >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
     AND call_end_date_utc                 <  DATE_ADD("${env:END_DATE}",1) )
     AND enhanced_account_number           = 0                -- don't include enhanced accounts
;

SELECT  '***** getting visits with a call ******';
-- similar to cs_chr_quantum_events AND cs_pageviews_and_calls AND cs_vc
DROP TABLE IF EXISTS asp_support_vc PURGE;
CREATE TEMPORARY TABLE asp_support_vc      AS
  SELECT  application_name,
          denver_date,
          page_name,
          COUNT(DISTINCT visit_unique_id)  AS distinct_visits_with_calls,
          COUNT(1)                         AS pageviews_with_calls
    FROM asp_support_quantum_events           v
    JOIN asp_support_calls                    c
      ON v.account_number                  = c.account_number
   WHERE (v.denver_date                    >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
     AND v.denver_date                     <  "${env:END_DATE}" )
     AND v.message__name                   = 'pageView'
     AND v.page_name IS NOT NULL
     AND (c.call_start_timestamp_utc - v.received__timestamp) BETWEEN 0 AND 86400000
  GROUP BY  application_name,
            denver_date,
            page_name
;

SELECT '***** getting support page level aggregate data  ******';
-- similar to cs_vt
DROP TABLE IF EXISTS asp_support_vt PURGE;
CREATE TEMPORARY TABLE asp_support_vt      AS
  SELECT  application_name,
          denver_date,
          page_name,
          COUNT(DISTINCT device_id)        AS visitors,
          COUNT(DISTINCT CASE WHEN account_number != 'UNKNOWN'
                              THEN device_id
                              END)         AS authenticated_visitors,
          COUNT(DISTINCT CASE WHEN account_number != 'UNKNOWN'
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
    FROM asp_support_quantum_events
   WHERE (denver_date                      >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
     AND denver_date                       <  "${env:END_DATE}")
     AND page_name IS NOT NULL
  GROUP BY  application_name,
            denver_date,
            page_name
;

SELECT '***** getting support search instances ******';
DROP TABLE IF EXISTS asp_support_search_instances PURGE;
CREATE TEMPORARY TABLE asp_support_search_instances AS
SELECT  application_name,
        denver_date,
        page_name,
        search_text,
        COUNT(1)                           AS search_instances
FROM (SELECT  application_name,
              denver_date,
              element_url                  AS page_name,
              search_text,
              ROW_NUMBER() OVER (PARTITION BY element_url, search_id ORDER BY seq_num DESC) rn
      FROM asp_support_quantum_events
      WHERE (denver_date                   >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
        AND denver_date                    <  "${env:END_DATE}" )
        AND operation__operation_type      = 'searchResultSelected'
        AND search_text IS NOT NULL
        AND element_url IS NOT NULL
        AND search_id   IS NOT NULL
) A
WHERE rn = 1
GROUP BY  application_name,
          denver_date,
          page_name,
          search_text
;

SELECT '***** inserting data into support_content_agg table ******';

-- similar to cs_pageview_call_in_rate with additional aggregations
INSERT OVERWRITE TABLE asp_support_content_agg PARTITION (denver_date)
-- page level data
SELECT  COALESCE(vc.application_name,vt.application_name) AS application_name,
        COALESCE(vc.page_name,vt.page_name)               AS page_name,
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
        NULL search_text,
        NULL search_instances,
        COALESCE(vc.denver_date, vt.denver_date)          AS denver_date
  FROM asp_support_vc                   vc
  FULL OUTER JOIN asp_support_vt        vt
    ON vc.application_name      = vt.application_name
   AND vc.page_name             = vt.page_name
   AND vc.denver_date           = vt.denver_date
UNION ALL
SELECT  application_name,
        page_name,
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
FROM asp_support_search_instances
;

SELECT '***** data insert into support_content_agg complete *****';
