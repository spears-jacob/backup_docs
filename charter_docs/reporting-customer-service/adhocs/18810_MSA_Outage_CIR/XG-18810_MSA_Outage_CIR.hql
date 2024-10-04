DESCRIBE prod.core_quantum_events_portals_v;
DESCRIBE prod.red_cs_call_care_data_v;

--User clicks on Service Outage Reported Banner
DROP TABLE IF EXISTS dev_tmp.xp_outage_clicks;
CREATE TABLE dev_tmp.xp_outage_clicks
AS
SELECT DISTINCT
visit__visit_id as visit_id
,visit__account__enc_account_number as account_number
,CASE WHEN visit__account__details__mso is null then 'UNMAPPED' ELSE visit__account__details__mso END as MSO
,MAX(CAST(received__timestamp/1000 as BIGINT)) OVER (PARTITION BY visit__visit_id) as received__timestamp
,partition_date_utc
FROM prod.core_quantum_events_portals_v
WHERE
visit__application_details__application_name = 'MySpectrum'
and state__view__current_Page__elements__standardized_Name = 'outageReportedBanner'
and operation__operation_Type = 'buttonClick'
and message__category = 'navigation'
and message__triggered_By = 'user'
and visit__account__enc_account_number is not null
and visit__account__enc_account_number != 'Qn+T/sa8AB7Gnxhi4Wx2Xg=='
and visit__account__enc_account_number != 'dZJho5z9MUSD35BuytdIhg=='
and partition_date_utc >= '2020-03-01'
;

--Checking table
SELECT 
partition_date_utc
,count(1) as clicks
,count(distinct visit_id) as visits
,count(account_number) as accounts
FROM dev_tmp.xp_outage_clicks
GROUP BY partition_date_utc
;

--Outage banner displayed
DROP TABLE IF EXISTS dev_tmp.xp_outage_displayed;
CREATE TABLE dev_tmp.xp_outage_displayed
AS
SELECT DISTINCT
visit__visit_id as visit_id
,visit__account__enc_account_number as account_number
,CASE WHEN visit__account__details__mso is null then 'UNMAPPED' ELSE visit__account__details__mso END as MSO
,MAX(CAST(received__timestamp/1000 as BIGINT)) OVER (PARTITION BY visit__visit_id) as received__timestamp
,partition_date_utc
FROM prod.core_quantum_events_portals_v
WHERE
visit__application_details__application_name = 'MySpectrum'
and message__name = 'applicationActivity'
and operation__operation_Type = 'serviceOutage'
and  message__category in ('application', 'navigation')
and visit__account__enc_account_number is not null
and partition_date_utc >= '2020-03-01'
;

--Checking table
SELECT 
partition_date_utc
,count(1) as views
,count(distinct visit_id) as visits
,count(distinct account_number) as accounts
FROM dev_tmp.xp_outage_displayed
GROUP BY partition_date_utc
;

--Grab calls for month to date
DROP TABLE IF EXISTS dev_tmp.xp_march_calls;
CREATE TABLE dev_tmp.xp_march_calls
AS
SELECT DISTINCT
encrypted_account_number_256 as account
,(call_start_timestamp_utc/1000) as call_start_time
,call_end_date_utc
,call_inbound_key
FROM prod.red_cs_call_care_data_v                                             -- Table containing the spectrum.net, SB.net, and myspectrum app visits for the latest extract timeframe
WHERE 
call_end_date_utc >= '2020-03-01'
and segment_handled_flag = 1
and encrypted_account_number_256 is not null
;

--Checking table
SELECT 
call_end_date_utc
,count(1) as calls
,count(distinct call_inbound_key) as unique_calls
,count(distinct account) as accounts
FROM dev_tmp.xp_march_calls
GROUP BY call_end_date_utc
;

--Combine views and clicks
DROP TABLE IF EXISTS dev_tmp.xp_banner_views_clicks_by_day;
CREATE TABLE dev_tmp.xp_banner_views_clicks_by_day
AS
SELECT 
CASE WHEN a.partition_date_utc is null then b.partition_date_utc ELSE a.partition_date_utc END as datec
,CASE WHEN a.MSO is null then b.MSO ELSE a.MSO END as MSO
,count(a.visit_id) as views
,count(b.visit_id) as clicks
FROM dev_tmp.xp_outage_displayed a
  FULL JOIN dev_tmp.xp_outage_clicks b on a.visit_id = b.visit_id
GROUP BY CASE WHEN a.partition_date_utc is null then b.partition_date_utc ELSE a.partition_date_utc END, CASE WHEN a.MSO is null then b.MSO ELSE a.MSO END
;

SELECT * FROM dev_tmp.xp_banner_views_clicks_by_day;

--Create banner displayed with calls table
CREATE TABLE dev_tmp.xp_banner_outage_with_call
AS
SELECT
a.partition_date_utc
,a.MSO
,count(distinct a.visit_id) as banner_view_with_call
FROM dev_tmp.xp_outage_displayed a
LEFT JOIN dev_tmp.xp_march_calls b on a.account_number = account
WHERE
(call_start_time - received__timestamp) <= 86400
and (call_start_time - received__timestamp) >= 0
GROUP BY a.partition_date_utc ,a.MSO
;

SELECT * FROM dev_tmp.xp_banner_outage_with_call;

--Create clicks with calls table
CREATE TABLE dev_tmp.xp_banner_click_with_call
AS
SELECT
a.partition_date_utc
,a.MSO
,count(distinct a.visit_id) as banner_click_with_call
FROM dev_tmp.xp_outage_clicks a
LEFT JOIN dev_tmp.xp_march_calls b on a.account_number = account
WHERE
(call_start_time - received__timestamp) <= 86400
and (call_start_time - received__timestamp) >= 0
GROUP BY a.partition_date_utc ,a.MSO
;

SELECT * FROM dev_tmp.xp_banner_click_with_call;

--Create large table with all the data for Tableau
CREATE TABLE dev_tmp.xp_msa_outage_cir_adhoc
AS
SELECT
a.*
,b.banner_view_with_call
,c.banner_click_with_call
FROM dev_tmp.xp_banner_views_clicks_by_day a
  LEFT JOIN dev_tmp.xp_banner_outage_with_call b on a.datec = b.partition_date_utc AND a.mso = b.mso
  LEFT JOIN dev_tmp.xp_banner_click_with_call c on a.datec = c.partition_date_utc AND c.mso = b.mso
;
