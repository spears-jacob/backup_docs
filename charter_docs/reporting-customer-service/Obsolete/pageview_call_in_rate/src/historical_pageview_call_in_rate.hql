--TRUNCATE TABLE DEV.cs_venona_events;


--set hive.auto.convert.join=false;
--SET mapreduce.job.reduces=1000;

--create temporary table dev.cs_partition_date AS
--set var = select to_date(PROD.EPOCH_timestamp((UNIX_TIMESTAMP()-(86400*31))*1000,'America/Denver')) partition_date;


SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

--From Adobe tables - SPECNET
INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_venona_events PARTITION (source_system, application_name, partition_date_utc)
SELECT CONCAT(visit__visit_id,'-',coalesce(regexp_replace(${env:ENVIRONMENT}.aes_decrypt(
  CASE WHEN visit__account__account_number IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') THEN NULL -- if decrypted account number is pending or a blank string set it null
  ELSE visit__account__account_number END
),"-",""),"UNKNOWN")) visit_unique_id
,COALESCE(regexp_replace(${env:ENVIRONMENT}.aes_decrypt(
  CASE WHEN visit__account__account_number IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') THEN NULL -- if decrypted account number is pending or a blank string set it null
  ELSE visit__account__account_number END
),"-",""),"UNKNOWN")  account_number
,'CHR' company
,visit__visit_id visit_id
,message__timestamp*1000
,${env:ENVIRONMENT}.epoch_timestamp(message__timestamp*1000) current_page_datetime
,CASE WHEN lower(state__view__current_page__name) rlike '.*email.*' or lower(state__view__current_page__page_id) rlike '.*mail2' THEN 'email'
     ELSE lower(state__view__current_page__name) END AS current_page_name
,state__view__modal__name current_modal_name
,visit__visit_start_timestamp*1000 visit_start_timestamp
,${env:ENVIRONMENT}.epoch_timestamp(visit__visit_start_timestamp*1000) visit_start_datetime
,lower(regexp_replace(message__category," ",""))  message_name
,'ADOBE' source_system
,'SPECNET' application_name
,partition_date
FROM prod.asp_v_net_events
WHERE partition_date <= '2018-08-22'
AND partition_date >= '2017-12-21'

UNION ALL

--From Adobe tables - SBNET
SELECT CONCAT(visit__visit_id,'-',coalesce(regexp_replace(${env:ENVIRONMENT}.aes_decrypt(
  CASE WHEN visit__account__account_number IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') THEN NULL -- if decrypted account number is pending or a blank string set it null
  ELSE visit__account__account_number END
),"-",""),"UNKNOWN")) visit_unique_id
,COALESCE(regexp_replace(${env:ENVIRONMENT}.aes_decrypt(
  CASE WHEN visit__account__account_number IN ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==') THEN NULL -- if decrypted account number is pending or a blank string set it null
  ELSE visit__account__account_number END
),"-",""),"UNKNOWN")  account_number
,'CHR' company
,visit__visit_id visit_id
,message__timestamp*1000
,${env:ENVIRONMENT}.epoch_timestamp(message__timestamp*1000) current_page_datetime
,CASE WHEN lower(state__view__current_page__page_name) rlike '.*email.*' or lower(state__view__current_page__page_id) rlike '.*mail2' THEN 'email'
     ELSE lower(state__view__current_page__page_name) END AS current_page_name
,state__view__modal__name current_modal_name
,visit__visit_start_timestamp*1000 visit_start_timestamp
,${env:ENVIRONMENT}.epoch_timestamp(visit__visit_start_timestamp*1000) visit_start_datetime
,lower(regexp_replace(message__category," ","")) message_name
,'ADOBE' source_system
,'SMB' application_name
,partition_date_utc
FROM prod.asp_v_sbnet_events
WHERE partition_date_utc <= '2018-08-29'
AND partition_date_utc >= '2017-12-21'
;


--Joining to prod.account_current to ONLY grab Charter records
set hive.auto.convert.join=false;

INSERT INTO ${env:ENVIRONMENT}.cs_chr_venona_events PARTITION (partition_date_utc)
  SELECT VE.visit_unique_id, VE.account_number, VE.company, VE.visit_id, VE.current_page_received_timestamp,
        VE.current_page_datetime, VE.current_page_name, VE.current_modal_name, VE.visit_start_timestamp,
        VE.visit_start_datetime, VE.message_name, VE.source_system, VE.application_name, VE.partition_date_utc
  FROM ${env:ENVIRONMENT}.CS_VENONA_EVENTS VE
    INNER JOIN prod.account_current ac
      ON VE.account_number = ${env:ENVIRONMENT}.AES_DECRYPT256(ACCOUNT__NUMBER_AES256)
WHERE 1=1
--AND VE.company = 'CHARTER'
AND lower(VE.message_name) = 'pageview'
;

DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_chr_calls PURGE;
create table ${env:ENVIRONMENT}.cs_chr_calls as
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
,visit_unique_id
,call_inbound_key
,call_contains_handled_segment
,call_start_timestamp_utc
,current_page_received_timestamp
,source_system
,partition_date_utc
FROM ${env:ENVIRONMENT}.cs_chr_venona_events venona
  LEFT JOIN ${env:ENVIRONMENT}.cs_chr_calls calls
    ON venona.account_number = CALLS.account_number
;


DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_visits_calls PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_visits_calls AS
SELECT UPPER(application_name) APPLICATION_NAME
,CURRENT_PAGE_NAME
,PARTITION_DATE_UTC
,source_system
,COUNT(1) count_of_pageviews_with_calls
,COUNT(distinct visit_unique_id) count_of_distinct_visits_with_calls
FROM ${env:ENVIRONMENT}.cs_pageviews_and_calls
WHERE call_start_timestamp_utc IS NOT NULL
AND current_page_name IS NOT NULL
AND (CALL_START_TIMESTAMP_UTC - CURRENT_PAGE_RECEIVED_TIMESTAMP) BETWEEN 0 AND 86400000
GROUP BY UPPER(APPLICATION_NAME)
,CURRENT_PAGE_NAME
,PARTITION_DATE_UTC
,source_system
;


DROP TABLE IF EXISTS ${env:ENVIRONMENT}.cs_VISITS_TOTAL PURGE;
CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_VISITS_TOTAL AS
SELECT UPPER(application_name) APPLICATION_NAME, CURRENT_PAGE_NAME, PARTITION_DATE_UTC, source_system
,COUNT(1) total_pageviews
,COUNT(distinct visit_unique_id) total_distinct_visits
FROM ${env:ENVIRONMENT}.CS_VENONA_EVENTS
GROUP BY UPPER(application_name), CURRENT_PAGE_NAME, PARTITION_DATE_UTC, source_system
;


INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_pageview_call_in_rate PARTITION (source_system, partition_date_utc)
SELECT COALESCE(VISITS_CALLS.application_name,VISITS_TOTAL.APPLICATION_NAME) APPLICATION_NAME
,COALESCE(visits_calls.current_page_name,visits_total.current_page_name) CURRENT_PAGE_NAME
,COALESCE(visits_calls.count_of_pageviews_with_calls,0) count_of_pageviews_with_calls
,VISITS_TOTAL.total_pageviews
,COALESCE(visits_calls.count_of_distinct_visits_with_calls,0) count_of_distinct_visits_with_calls
,visits_total.total_distinct_visits
,cast(count_of_distinct_visits_with_calls/total_distinct_visits as decimal(12,4)) call_in_rate
,COALESCE(visits_calls.source_system, visits_total.source_system)
,COALESCE(visits_calls.PARTITION_DATE_UTC,VISITS_TOTAL.PARTITION_DATE_UTC) PARTITION_DATE_UTC
FROM ${env:ENVIRONMENT}.cs_visits_calls visits_calls
FULL OUTER JOIN ${env:ENVIRONMENT}.cs_visits_total visits_total
		ON VISITS_CALLS.APPLICATION_NAME = VISITS_TOTAL.APPLICATION_NAME
    AND VISITS_CALLS.CURRENT_PAGE_NAME = VISITS_TOTAL.CURRENT_PAGE_NAME
		AND VISITS_CALLS.PARTITION_DATE_UTC = VISITS_TOTAL.PARTITION_DATE_UTC
;
