USE ${env:ENVIRONMENT};

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT '***** getting support search content base data ******'
;
-- similar to cs_venona_events
INSERT OVERWRITE TABLE asp_support_venona_events PARTITION (partition_date_utc)
SELECT  application_name,
        visit_id,
        device_id,
        CASE WHEN SUBSTR(page_name, -1) = '/' THEN SUBSTR(page_name, 1, LENGTH(page_name) - 1) ELSE page_name END AS page_name,
        visit_unique_id,
        account_number,
        received__timestamp,
        message__name,
        operation__operation_type,
        search_id,
        search_text,
        seq_num,
        CASE WHEN SUBSTR(element_url, -1) = '/' THEN SUBSTR(element_url, 1, LENGTH(element_url) - 1) ELSE element_url END AS element_url,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'articleHelpful' THEN 1 ELSE 0 END AS helpful_yes,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'articleNotHelpful' THEN 1 ELSE 0 END AS helpful_no,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'sendFeedback' AND elements__selected_options = 'wasnt_what_i_searched' THEN 1 ELSE 0 END AS wasnt_what_i_searched,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'sendFeedback' AND elements__selected_options = 'incorrect_info' THEN 1 ELSE 0 END AS incorrect_info,
        CASE WHEN message__name = 'selectAction' AND elements__standardized_name = 'sendFeedback' AND elements__selected_options = 'confusing' THEN 1 ELSE 0 END AS confusing,
        partition_date_utc
FROM (
  SELECT  UPPER(visit__application_details__application_name) application_name,
          visit__device__enc_uuid AS device_id,
          visit__visit_id AS visit_id,
          partition_date_utc,
          CASE WHEN (state__view__current_page__page_name = 'supportArticle' OR state__view__current_page__page_name = 'SupportArticle')
            THEN regexp_extract(state__view__current_page__page_url,"(\/support[A-Za-z\-\/0-9]+)")
            ELSE state__view__current_page__page_name END as page_name,
            CONCAT(visit__visit_id,'-',
              CASE WHEN (visit__account__enc_account_number
                         NOT IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
                         OR visit__account__enc_account_number IS NOT NULL
                        )
                   THEN visit__account__enc_account_number -- if decrypted account number is pending or a blank string set it null
                   ELSE NULL
              END
              ) visit_unique_id,
          COALESCE(
                CASE WHEN (visit__account__enc_account_number
                           NOT IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
                           OR visit__account__enc_account_number IS NOT NULL
                          )
                     THEN visit__account__enc_account_number -- if decrypted account number is pending or a blank string set it null
                     ELSE NULL
                END
              ,"UNKNOWN") AS account_number,
          received__timestamp,
          message__name,
          operation__operation_type,
          state__search__search_id AS search_id,
          state__search__text AS search_text,
          message__sequence_number AS seq_num,
          state__view__current_page__elements__element_url AS element_url,
          state__view__current_page__elements__standardized_name AS elements__standardized_name,
          state__view__current_page__elements__selected_options[0] AS elements__selected_options
  FROM asp_v_venona_events_portals
  WHERE partition_date_utc  >= '${env:START_DATE}'
    AND partition_date_utc  <  '${env:END_DATE}'
  AND (visit__application_details__application_name = 'specnet' OR
       visit__application_details__application_name = 'SpecNet')
  AND state__view__current_page__app_section = 'support'
) A
;

SET CallDataLag=3;
SELECT '***** getting support call data ******'
;
-- similar to cs_chr_calls
DROP TABLE IF EXISTS asp_support_calls PURGE;
CREATE TEMPORARY TABLE asp_support_calls AS
  SELECT DISTINCT encrypted_account_number_256 account_number,
                  call_inbound_key,
                  call_start_timestamp_utc
  FROM red_cs_call_care_data_v
  where ( call_end_date_utc >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
      AND call_end_date_utc <  "${env:END_DATE}" )
  AND enhanced_account_number = 0  -- don't include enhanced accounts
;

SELECT  '***** getting visits with a call ******'
;
-- similar to cs_chr_venona_events AND cs_pageviews_and_calls AND cs_visits_calls
DROP TABLE IF EXISTS asp_support_visits_calls PURGE;
CREATE TEMPORARY TABLE asp_support_visits_calls AS
SELECT  application_name,
        partition_date_utc,
        page_name,
        COUNT(DISTINCT visit_unique_id) AS distinct_visits_with_calls,
        COUNT(1) AS pageviews_with_calls
FROM asp_support_venona_events v
INNER JOIN prod.red_atom_snapshot_accounts_v ac
  ON v.account_number = encrypted_legacy_account_number_256
INNER JOIN asp_support_calls c
  ON v.account_number = c.account_number
WHERE ( v.partition_date_utc >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
    AND v.partition_date_utc <  "${env:END_DATE}" )
 AND v.message__name = 'pageView'
 AND page_name IS NOT NULL
 AND (call_start_timestamp_utc - received__timestamp) BETWEEN 0 AND 86400000
GROUP BY  application_name,
          partition_date_utc,
          page_name
;

SELECT '***** getting support page level aggregate data  ******'
;
-- similar to cs_VISITS_TOTAL
DROP TABLE IF EXISTS asp_support_visits_total PURGE;
CREATE TEMPORARY TABLE asp_support_visits_total AS
SELECT  application_name,
        partition_date_utc,
        page_name,
        COUNT(DISTINCT device_id) AS visitors,
        COUNT(DISTINCT CASE WHEN account_number != 'UNKNOWN' THEN device_id END) AS authenticated_visitors,
        COUNT(DISTINCT CASE WHEN account_number != 'UNKNOWN' THEN account_number END) AS households,
        COUNT(DISTINCT visit_id) AS visits,
        SUM(CASE WHEN message__name = 'pageView' THEN 1 ELSE 0 END) As pageviews,
        SUM(helpful_yes) AS helpful_yes,
        SUM(helpful_no) AS helpful_no,
        SUM(wasnt_what_i_searched) AS wasnt_what_i_searched,
        SUM(incorrect_info) AS incorrect_info,
        SUM(confusing) AS confusing,
        COUNT(DISTINCT CASE WHEN message__name = 'pageView' THEN visit_unique_id END) AS total_distinct_visits
FROM asp_support_venona_events
WHERE ( partition_date_utc >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
    AND partition_date_utc <  "${env:END_DATE}" )
AND page_name IS NOT NULL
GROUP BY  application_name,
          partition_date_utc,
          page_name
;

SELECT '***** getting support search instances ******'
;
DROP TABLE IF EXISTS asp_support_search_instances PURGE;
CREATE TEMPORARY TABLE asp_support_search_instances AS
SELECT  application_name,
        partition_date_utc,
        page_name,
        search_text,
        COUNT(1) AS search_instances
FROM (SELECT  application_name,
              partition_date_utc,
              element_url AS page_name,
              search_text,
              ROW_NUMBER() OVER (PARTITION BY element_url, search_id ORDER BY seq_num DESC) rn
      FROM asp_support_venona_events
      WHERE ( partition_date_utc >= DATE_SUB("${env:START_DATE}",${hiveconf:CallDataLag})
          AND partition_date_utc <  "${env:END_DATE}" )
       AND operation__operation_type = 'searchResultSelected'
       AND search_text IS NOT NULL
       AND element_url IS NOT NULL
       AND search_id IS NOT NULL
) A
WHERE rn = 1
GROUP BY  application_name,
          partition_date_utc,
          page_name,
          search_text
;

SELECT '***** inserting data into support_content_performance table ******'
;

-- similar to cs_pageview_call_in_rate with additional aggregations
INSERT OVERWRITE TABLE asp_support_content_performance PARTITION (partition_date_utc)
-- page level data
SELECT  COALESCE(visits_calls.application_name,visits_total.application_name) application_name,
        COALESCE(visits_calls.page_name,visits_total.page_name) page_name,
        COALESCE(visits_calls.pageviews_with_calls,0) pageviews_with_calls,
        visits_total.visitors,
        visits_total.authenticated_visitors,
        visits_total.households,
        visits_total.visits,
        visits_total.pageviews,
        visits_total.helpful_yes,
        visits_total.helpful_no,
        visits_total.wasnt_what_i_searched,
        visits_total.incorrect_info,
        visits_total.confusing,
        COALESCE(visits_calls.distinct_visits_with_calls,0) distinct_visits_with_calls,
        visits_total.total_distinct_visits,
        cast(distinct_visits_with_calls/total_distinct_visits as decimal(12,4)) call_in_rate,
        NULL search_text,
        NULL search_instances,
        COALESCE(visits_calls.partition_date_utc,visits_total.partition_date_utc) partition_date_utc
FROM asp_support_visits_calls visits_calls
FULL OUTER JOIN asp_support_visits_total visits_total
        ON visits_calls.application_name = visits_total.application_name
        AND visits_calls.page_name = visits_total.page_name
        AND visits_calls.partition_date_utc = visits_total.partition_date_utc
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
        partition_date_utc
FROM asp_support_search_instances
;

SELECT '***** data insert into support_content_performance complete ******'
;
