--From Quantum
set hive.exec.orc.split.strategy=BI;

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_venona_events PARTITION (source_system, application_name, partition_date_utc)
SELECT --prod.aes_decrypt(visit__account__account_number) account_number
--max(prod.aes_decrypt(visit__account__account_number)) OVER (PARTITION BY visit__account__account_number) account_number_new
CONCAT(visit__visit_id,'-',
  coalesce(regexp_replace(${env:ENVIRONMENT}.aes_decrypt(
    CASE WHEN visit__account__account_number IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') THEN NULL -- if decrypted account number is pending or a blank string set it null
    ELSE visit__account__account_number END
  ),"-",""),"UNKNOWN")) visit_unique_id
,coalesce(regexp_replace(${env:ENVIRONMENT}.aes_decrypt(
  CASE WHEN visit__account__account_number IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') THEN NULL -- if decrypted account number is pending or a blank string set it null
  ELSE visit__account__account_number END
),"-",""),"UNKNOWN")  account_number
,visit__account__details__mso company
,visit__visit_id visit_id
,received__timestamp received_timestamp
,${env:ENVIRONMENT}.epoch_timestamp(received__timestamp) current_page_datetime
,CASE WHEN lower(state__view__current_page__page_name) = 'supportarticle'
        and lower(message__name) = 'pageview'
        THEN regexp_extract(state__view__current_page__page_url,"(\/[A-Za-z\-\/0-9]+)")
        ELSE state__view__current_page__page_name END as current_page_name
,state__view__current_page__app_section current_app_section
,state__view__modal__name current_modal_name
,visit__visit_start_timestamp visit_start_timestamp
,${env:ENVIRONMENT}.epoch_timestamp(visit__visit_start_timestamp) visit_start_datetime
,message__name message_name
,'QUANTUM' source_system
,UPPER(visit__application_details__application_name) application_name
,partition_date_utc
FROM prod.asp_v_venona_events_portals v
WHERE 1=1
AND partition_date_utc >= '${env:START_DATE}'
AND partition_date_utc < '${env:END_DATE}';


--Joining to prod.account_current to ONLY grab Charter records
INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_chr_venona_events PARTITION (partition_date_utc)
  SELECT VE.visit_unique_id, VE.account_number, VE.company, VE.visit_id, VE.current_page_received_timestamp, VE.current_page_datetime, VE.current_page_name
  ,VE.current_app_section ,VE.current_modal_name, VE.visit_start_timestamp, VE.visit_start_datetime, VE.message_name, VE.source_system, VE.application_name
  ,ve.partition_date_utc
  FROM ${env:ENVIRONMENT}.CS_VENONA_EVENTS VE
    --INNER JOIN ${env:ENVIRONMENT}.account_current ac   -- implemented as part of XGANALYTIC-14664
      --ON VE.account_number = PROD.AES_DECRYPT256(ACCOUNT__NUMBER_AES256)   -- implemented as part of XGANALYTIC-14664
WHERE 1=1
--AND VE.company = 'CHARTER'
AND lower(VE.message_name) = 'pageview'
AND partition_date_utc >= '${env:START_DATE}'
AND partition_date_utc < '${env:END_DATE}'
AND LOWER(VE.account_number) <> 'unknown';  -- implemented as part of XGANALYTIC-14664

DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_chr_calls PURGE;
CREATE TABLE ${env:ENVIRONMENT}.cs_chr_calls as
  SELECT ${env:ENVIRONMENT}.aes_decrypt256(account_number) account_number, call_inbound_key, call_start_timestamp_utc
  ,CASE WHEN SUM(cast(segment_handled_flag as int)) > 0 THEN 1 ELSE 0 END call_contains_handled_segment
  FROM ${env:ENVIRONMENT}.cs_call_data
  WHERE enhanced_account_number = 0 -- don't include enhanced accounts
  GROUP BY ${env:ENVIRONMENT}.aes_decrypt256(account_number), call_inbound_key, call_start_timestamp_utc
  --WHERE COMPANY_CODE = 'CHR'
;

--set hive.auto.convert.join=false;
DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_pageviews_and_calls PURGE;
create table ${env:ENVIRONMENT}.cs_pageviews_and_calls as
SELECT application_name
,venona.account_number
,current_page_name
,current_app_section
,visit_unique_id
,call_inbound_key
,call_contains_handled_segment
,call_start_timestamp_utc
,current_page_received_timestamp
,source_system
,partition_date_utc
,CASE
    WHEN venona.company IN ('CHARTER', '"CHTR"') THEN 'L-CHTR'
    WHEN venona.company IN ('BH', '"BHN"') THEN 'L-BHN'
    WHEN venona.company IN ('TWC','"TWC"')  THEN 'L-TWC'
  ELSE 'UNKNOWN' END AS mso
FROM ${env:ENVIRONMENT}.cs_chr_venona_events venona
  LEFT JOIN ${env:ENVIRONMENT}.cs_chr_calls calls
    ON venona.account_number = CALLS.account_number
;


DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_visits_calls PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_visits_calls AS
SELECT UPPER(application_name) APPLICATION_NAME
,CURRENT_PAGE_NAME
,CURRENT_APP_SECTION
,PARTITION_DATE_UTC
,source_system
,COUNT(1) count_of_pageviews_with_calls
,COUNT(distinct visit_unique_id) count_of_distinct_visits_with_calls
,mso
FROM ${env:ENVIRONMENT}.cs_pageviews_and_calls
WHERE call_start_timestamp_utc IS NOT NULL
AND current_page_name IS NOT NULL
AND (CALL_START_TIMESTAMP_UTC - CURRENT_PAGE_RECEIVED_TIMESTAMP) BETWEEN 0 AND 86400000
GROUP BY UPPER(APPLICATION_NAME)
,CURRENT_PAGE_NAME
,CURRENT_APP_SECTION
,PARTITION_DATE_UTC
,source_system
,mso
;


DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_VISITS_TOTAL PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_VISITS_TOTAL AS
SELECT UPPER(application_name) APPLICATION_NAME, CURRENT_PAGE_NAME, CURRENT_APP_SECTION, PARTITION_DATE_UTC, source_system
,COUNT(1) total_pageviews
,COUNT(distinct visit_unique_id) total_distinct_visits
,CASE
    WHEN company IN ('CHARTER', '"CHTR"') THEN 'L-CHTR'
    WHEN company IN ('BH', '"BHN"') THEN 'L-BHN'
    WHEN company IN ('TWC','"TWC"')  THEN 'L-TWC'
  ELSE 'UNKNOWN' END AS mso
FROM ${env:ENVIRONMENT}.CS_VENONA_EVENTS
GROUP BY UPPER(application_name), CURRENT_PAGE_NAME, CURRENT_APP_SECTION, PARTITION_DATE_UTC, source_system,
CASE
    WHEN company IN ('CHARTER', '"CHTR"') THEN 'L-CHTR'
    WHEN company IN ('BH', '"BHN"') THEN 'L-BHN'
    WHEN company IN ('TWC','"TWC"')  THEN 'L-TWC'
  ELSE 'UNKNOWN' END
;




INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_pageview_call_in_rate PARTITION (source_system, partition_date_utc)
SELECT COALESCE(VISITS_CALLS.application_name,VISITS_TOTAL.APPLICATION_NAME) APPLICATION_NAME
,COALESCE(visits_calls.current_page_name,visits_total.current_page_name) CURRENT_PAGE_NAME
,COALESCE(visits_calls.current_app_section,visits_total.current_app_section) CURRENT_APP_SECTION
,COALESCE(visits_calls.count_of_pageviews_with_calls,0) count_of_pageviews_with_calls
,VISITS_TOTAL.total_pageviews
,COALESCE(visits_calls.count_of_distinct_visits_with_calls,0) count_of_distinct_visits_with_calls
,visits_total.total_distinct_visits
,cast(count_of_distinct_visits_with_calls/total_distinct_visits as decimal(12,4)) call_in_rate
,visits_total.mso                     -- implemented as part of XGANALYTIC-14664
,COALESCE(visits_calls.source_system, visits_total.source_system)
,COALESCE(visits_calls.PARTITION_DATE_UTC,VISITS_TOTAL.PARTITION_DATE_UTC) PARTITION_DATE_UTC
FROM ${env:ENVIRONMENT}.cs_visits_calls visits_calls
FULL OUTER JOIN ${env:ENVIRONMENT}.cs_visits_total visits_total
ON VISITS_CALLS.APPLICATION_NAME = VISITS_TOTAL.APPLICATION_NAME
AND VISITS_CALLS.CURRENT_PAGE_NAME = VISITS_TOTAL.CURRENT_PAGE_NAME
AND VISITS_CALLS.PARTITION_DATE_UTC = VISITS_TOTAL.PARTITION_DATE_UTC
AND VISITS_CALLS.mso = VISITS_TOTAL.mso
;