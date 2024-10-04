
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
--ADD jar hdfs:///udf/brickhouse-0.7.1.jar;
--CREATE FUNCTION array_intersect AS "brickhouse.udf.collect.ArrayIntersectUDF";
-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};
--SET hive.cbo.enable=true;
--SET hive.compute.query.using.stats=true;
--SET hive.stats.fetch.partition.stats=true;
--SET hive.tez.container.size=8192;
--SET hive.tez.java.opts=-Xmx6554m;

-- Monthly net aggregate extract tables for pages and custom links 
-- 1) net_hhs_pvs_links_monthly
-- 2) net_hhs_sections_monthly
-- 3) net_return_frequency_monthly
-- 4) net_visit_metrics_monthly
-- 5) net_hh_total_monthly

-- Monthly Table : 1) net_hhs_pvs_links_monthly
-- Columns : year_month- run month and year constant for all metrics AS the reference year-month
--   hhs_month - Unique Account Numbers per month
--   message_name  - Unique message names per month
--   message_category  - message category
--   page_name - Unique page names per month
--   page_section  - Unique page sections per month
--   operation_type- Unique operation types per month
--   company - hard-code source company for each data source, 'L-CHTR' = legacy charter, 'L-TWC' = legacy bright house, etc.


-- Begin inserts into table: net_hh_account_monthly --

-- Household account list to get counts of Intersections
INSERT INTO TABLE net_hh_account_monthly 
SELECT
'${env:YEAR_MONTH}' AS year_month,
COUNT(DISTINCT visit__account__enc_account_number) AS total_unique_hhs,
collect_set(visit__account__enc_account_number) AS account_number_list
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

-- 6 month account retention table
--SET hive.auto.convert.join=false;

SELECT '--***** End inserts into table: net_hh_account_monthly *****--'
;

-- End inserts into table: net_hh_account_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin calculations for table: net_accounts_monthly_report --

DROP TABLE IF EXISTS net_accounts_monthly_report; --drop table

CREATE TABLE net_accounts_monthly_report --create table for 6 month period
AS
SELECT 
left_year_month, 
right_year_month, 
left_total_unique_hhs, 
right_total_unique_hhs, 
size(array_intersect(left_account_number_list, right_account_number_list)) AS Intersect_total_unique_hhs
from 
(
SELECT
year_month AS left_year_month,
total_unique_hhs AS left_total_unique_hhs,
account_number_list AS left_account_number_list
FROM 
net_hh_account_monthly
ORDER BY left_year_month DESC LIMIT 6
)a
JOIN 
(
SELECT
year_month AS right_year_month,
total_unique_hhs AS right_total_unique_hhs,
account_number_list AS right_account_number_list
FROM 
net_hh_account_monthly
ORDER BY right_year_month DESC LIMIT 6
) b
WHERE left_year_month <= right_year_month;

SELECT '--***** End calculations for table: net_accounts_monthly_report *****--'
;

-- End calculations for table: net_accounts_monthly_report --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

-- --


-- Begin Calculations for table net_hhs_pvs_links_monthly --

INSERT INTO TABLE net_hhs_pvs_links_monthly
SELECT 
'${env:YEAR_MONTH}' AS year_month,
SIZE(COLLECT_SET(visit__account__enc_account_number)) AS hhs_month,
message__name AS message_name,
message__category AS message_category,
state__view__current_page__name AS page_name,
state__view__current_page__section AS page_section,
operation__type AS operation_type,
'L-CHTR' AS company
FROM
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__category in ('Page View' , 'Custom Link')
GROUP BY 
state__view__current_page__name, state__view__current_page__section, operation__type, message__category, message__name
;

SELECT '--***** End Calculations for table net_hhs_pvs_links_monthly *****--'
;

-- End Calculations for table net_hhs_pvs_links_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for table net_hhs_sections_monthly --

---- Monthly Table : 2) net_hhs_sections_monthly
---- Columns : year_month- run month and year constant for all metrics AS the reference year-month
----   message_category  - message category
----   hhs_month - Unique Account Numbers per month
----   page_section  - Unique page sections per month

INSERT INTO TABLE net_hhs_sections_monthly
SELECT 
'${env:YEAR_MONTH}' AS year_month,
MIN(message__category) AS message_category,
SIZE(COLLECT_SET(visit__account__enc_account_number)) AS hhs_month,
state__view__current_page__section AS page_section,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__category = "Page View"
GROUP BY 
state__view__current_page__section
;

SELECT '--***** End Calculations for table net_hhs_sections_monthly *****--'
;

-- End Calculations for table net_hhs_sections_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for table net_return_frequency_monthly --

INSERT INTO TABLE net_return_frequency_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
LOCATION.ZIP_CODE AS ZIP_CODE,
SIZE(COLLECT_SET(VISITOR_ID)) AS UNIQUE_MONTHS,
'L-CHTR' AS company
FROM NET_VISITS_AGG A
WHERE 
EXISTS(
SELECT
VISITOR_ID 
FROM 
NET_VISITS_AGG B
WHERE 
TO_DATE(start_timestamp) = '${env:MONTH_END_DATE}'
AND A.VISITOR_ID = B.VISITOR_ID) 
AND TO_DATE(A.start_timestamp) BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
GROUP BY 
LOCATION.ZIP_CODE
;

SELECT '--***** End Calculations for table net_return_frequency_monthly *****--'
;

-- End Calculations for table net_return_frequency_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Created User ID' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Created User ID' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('my-account.create-id.final.','my-account.create-id-final.bam','my-account.create-id-final.btm','my-account.create-id-final.nbtm')
;

SELECT '--***** End Calculations for metric name Created User ID *****--'
;

-- End Calculations for metric name 'Created User ID' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Created In-Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Created On Network' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('my-account.create-id-final.bam','my-account.create-id-final.btm')
;

SELECT '--***** End Calculations for metric name Created In-Home *****--'
;

-- End Calculations for metric name 'Created In-Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Created Out-Of-Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Created Off Network' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name = 'my-account.create-id-final.nbtm'
;

SELECT '--***** End Calculations for metric name Created Out-Of-Home *****--'
;

-- End Calculations for metric name 'Created Out-Of-Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Username Recovery In-Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Username Recovery In-Home' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='recover-id-final.btm'
;

SELECT '--***** End Calculations for metric name Username Recovery In-Home *****--'
;

-- End Calculations for metric name 'Username Recovery In-Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Username Initiation In-Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Username Initiation In-Home' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='recover-id-1.btm'
;

SELECT '--***** End Calculations for metric name Username Initiation In-Home *****--'
;

-- End Calculations for metric name 'Username Initiation In-Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Username Recovery Out-Of-Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Username Recovery Out-Of-Home' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='recover-id-final.nbtm'
;

SELECT '--***** End Calculations for metric name Username Initiation Out-Of-Home *****--'
;

-- End Calculations for metric name 'Username Initiation Out-Of-Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Username Recovery Out-Of-Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Username Initiation Out-Of-Home' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='recover-id-1.nbtm'
;

SELECT '--***** End Calculations for metric name Username Recovery Out-Of-Home *****--'
;

-- End Calculations for metric name 'Username Recovery Out-Of-Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Password Reset Initiation' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Password Reset Initiation' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('reset-password.1', 'reset-password.1a', 'reset-password.1b')
;

SELECT '--***** End Calculations for metric name Password Reset Initiation *****--'
;

-- End Calculations for metric name 'Password Reset Initiation' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Password Reset' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Password Reset' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='reset-password.3'
;

SELECT '--***** End Calculations for metric name Password Reset *****--'
;

-- End Calculations for metric name 'Password Reset' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Migrated Unique IDs' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Migrated Unique IDs' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='validation.3'
;

SELECT '--***** End Calculations for metric name Migrated Unique IDs *****--'
;

-- End Calculations for metric name 'Migrated Unique IDs' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Abandoned from Migration' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Abandoned from Migration' AS metric,
CAST(SIZE(COLLECT_SET(IF((lower(message__name)='validation.2'),visit__visit_id,null))) AS INT) - CAST(SIZE(COLLECT_SET(IF((lower(message__name)='validation.3'),visit__visit_id,null))) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND lower(message__name) IN ('validation.3','validation.2')
;

SELECT '--***** End Calculations for metric name Abandoned from Migration *****--'
;

-- End Calculations for metric name 'Abandoned from Migration' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'One-Time Payment' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'One-Time Payment' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('my-account.payment.one-time-debit-confirm','my-account.payment.one-time-credit-confirm','my-account.payment.one-time-eft-confirm')
;

SELECT '--***** End Calculations for metric name One-Time Payment *****--'
;

-- End Calculations for metric name 'One-Time Payment' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Auto Payment' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Auto Payment' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('my-account.payment.auto-pay-debit-confirm','my-account.payment.auto-pay-credit-confirm','my-account.payment.auto-pay-eft-confirm')
;

SELECT '--***** End Calculations for metric name Auto Payment *****--'
;

-- End Calculations for metric name 'Auto Payment' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'View Online Statement' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'View Online Statement' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='View Statement'
;

SELECT '--***** End Calculations for metric name View Online Statement *****--'
;

-- End Calculations for metric name 'View Online Statement' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Choose Online Billing Delivery Option' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Choose Online Billing Delivery Option' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='Paperless Billing'
;

SELECT '--***** End Calculations for metric name Choose Online Billing Delivery Option *****--'
;

-- End Calculations for metric name 'Choose Online Billing Delivery Option' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Number AskCharter requests' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'AskCharter requests' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('Ask Spectrum','Ask-Spectrum')
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'AskCharter requests' AS metric,
CAST(SUM(IF(state__search__text LIKE '%askamy%',1,0)) AS INT) AS visits_count,
'L-TWC' AS company
FROM 
twc_residential_global_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT '--***** End Calculations for metric name Number AskCharter requests *****--'
;

-- End Calculations for metric name 'Number AskCharter requests' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Refresh Digital Receivers' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Refresh Digital Receivers' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='support.category.tv.update-refresh-digital-receiver'
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Refresh Digital Receivers' AS metric,
CAST(SUM(IF(state__view__current_page__sub_section LIKE '%reauthorize%' OR state__view__current_page__sub_section LIKE '%reboot%',1,0)) AS INT) AS visits_count,
'L-TWC' AS company
FROM 
twc_residential_global_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Refresh Digital Receivers' AS metric,
CAST(SUM(IF(state__view__current_page__sub_section LIKE '%reauthorize%' OR state__view__current_page__sub_section LIKE '%reboot%',1,0)) AS INT) AS visits_count,
'L-BHN' AS company
FROM 
bhn_residential_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;
SELECT 'Line 451';

SELECT '--***** End Calculations for metric name Refresh Digital Receivers *****--'
;

-- End Calculations for metric name 'Refresh Digital Receivers' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Rescheduled Service Appointments' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Rescheduled Service Appointments' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='reschedule-appointment'
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Rescheduled Service Appointments' AS metric,
CAST(SUM(IF(state__view__current_page__sub_section = 'services : my services : appointment manager : reschedule submitted',1,0)) AS INT) AS visits_count,
'L-TWC' AS company 
FROM 
twc_residential_global_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Rescheduled Service Appointments' AS metric,
CAST(SUM(IF(state__view__current_page__sub_section LIKE '%reschedule%',1,0)) AS INT) AS visits_count,
'L-BHN' AS company
FROM 
bhn_residential_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT '--***** End Calculations for metric name Rescheduled Service Appointments *****--'
;

-- End Calculations for metric name 'Rescheduled Service Appointments' --
-------------------------------------------------------------------------------
------------------------------------------------------------------------------

 -- Begin Calculations for metric name 'Cancelled Service Appointments' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Cancelled Service Appointments' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='cancel-appointment'
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Cancelled Service Appointments' AS metric,
CAST(SUM(IF(state__view__current_page__sub_section = 'my services > appointment manager > cancel submitted',1,0)) AS INT) AS visits_count,
'L-TWC' AS company
FROM 
twc_residential_global_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Cancelled Service Appointments' AS metric,
CAST(SUM(IF(state__view__current_page__sub_section LIKE '%cancel%',1,0)) AS INT) AS visits_count,
'L-BHN' AS company
FROM 
bhn_residential_events
WHERE
(partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
AND 
epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT '--***** End Calculations for metric name Cancelled Service Appointments *****--'
;

-- End Calculations for metric name 'Cancelled Service Appointments' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'TV OnDemand' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'TV OnDemand' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('tv.on-demand','tv.on-demand.watch-online','tv.on-demand.watch-on-tv')
;

SELECT '--***** End Calculations for metric name TV OnDemand *****--'
;

-- End Calculations for metric name 'TV OnDemand' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Live TV' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Live TV' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='tv.live'
;

SELECT '--***** End Calculations for metric name Live TV *****--'
;

-- End Calculations for metric name 'Live TV' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'TV Guide' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'TV Guide' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='tv.guide'
;

SELECT '--***** End Calculations for metric name TV Guide *****--'
;

-- End Calculations for metric name 'TV Guide' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Send to TV' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Send to TV' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('Send to TV','Send To TV')
;

SELECT '--***** End Calculations for metric name Send to TV *****--'
;

-- End Calculations for metric name 'Send to TV' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Record' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Record' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='Record'
;

SELECT '--***** End Calculations for metric name Record *****--'
;

-- End Calculations for metric name 'Record' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Internet Security Suite Downloads' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Internet Security Suite Downloads' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND state__view__current_page__name='my-security-suite' 
AND message__name='Install'
;

SELECT '--***** End Calculations for metric name Internet Security Suite Downloads *****--'
;

-- End Calculations for metric name 'Internet Security Suite Downloads' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Voice Online Manager' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Voice Online Manager' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='Voice Online Manager Save Confirm'
;

SELECT '--***** End Calculations for metric name Voice Online Manager *****--'
;

-- End Calculations for metric name 'Voice Online Manager' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Internet Security Suite Visits' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Internet Security Suite Visits' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('support.internet-services.security-suite','support.internet-services.security-suite-0')
;

SELECT '--***** End Calculations for metric name Internet Security Suite Visits *****--'
;

-- End Calculations for metric name 'Internet Security Suite Visits' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Charter Business' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Charter Business' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name IN ('Charter-Business','CharterBusiness')
;

SELECT '--***** End Calculations for metric name Charter Business *****--'
;

-- End Calculations for metric name 'Charter Business' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Home' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Home' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='home'
;

SELECT '--***** End Calculations for metric name Home *****--'
;

-- End Calculations for metric name 'Home' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Login' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Login' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='login'
;

SELECT '--***** End Calculations for metric name Login *****--'
;

-- End Calculations for metric name 'Login' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'My Account' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'My Account' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='my-account'
;

SELECT '--***** End Calculations for metric name My Account *****--'
;

-- End Calculations for metric name 'My Account' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Payment Options' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Payment Options' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='payment.options'
;

SELECT '--***** End Calculations for metric name Payment Options *****--'
;

-- End Calculations for metric name 'Payment Options' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Billing and Transactions' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Billing and Transactions' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='billing-and-transactions'
;

SELECT '--***** End Calculations for metric name Billing and Transactions *****--'
;

-- End Calculations for metric name 'Billing and Transactions' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Support' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Support' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='support'
;

SELECT '--***** End Calculations for metric name Support *****--'
;

-- End Calculations for metric name 'Support' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'Reset Password Front Page' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'Reset Password Front Page' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='reset-password.1'
;

SELECT '--***** End Calculations for metric name Reset Password Front Page *****--'
;

-- End Calculations for metric name 'Reset Password Front Page' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'One-Time Debit Verification' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'One-Time Debit Verification' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='my-account.payment.one-time-verify-debit'
;

SELECT '--***** End Calculations for metric name One-Time Debit Verification *****--'
;

-- End Calculations for metric name 'One-Time Debit Verification' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'VOM Summary' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'VOM Summary' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='voice.summary'
;

SELECT '--***** End Calculations for metric name VOM Summary *****--'
;

-- End Calculations for metric name 'VOM Summary' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'VOM Feature' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'VOM Feature' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='voice.features'
;

SELECT '--***** End Calculations for metric name VOM Feature *****--'
;

-- End Calculations for metric name 'VOM Feature' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'VOM Voicemail' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'VOM Voicemail' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='voice.voicemail'
;

SELECT '--***** End Calculations for metric name VOM Voicemail *****--'
;

-- End Calculations for metric name 'VOM Voicemail' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'VOM Call Logs' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'VOM Call Logs' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='voice.calllogs'
;

SELECT '--***** End Calculations for metric name VOM Call Logs *****--'
;

-- End Calculations for metric name 'VOM Call Logs' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'VOM My Contacts' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'VOM My Contacts' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='voice.mycontacts'
;

SELECT '--***** End Calculations for metric name VOM My Contacts *****--'
;

-- End Calculations for metric name 'VOM My Contacts' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Calculations for metric name 'TV' --

INSERT INTO TABLE net_visit_metrics_monthly
SELECT 
'${env:YEAR_MONTH}' AS YEAR_MONTH,
'TV' AS metric,
CAST(SIZE(COLLECT_SET(visit__visit_id)) AS INT) AS visits_count,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
AND message__name='tv'
;

SELECT '--***** End Calculations for metric name TV *****--'
;

-- End Calculations for metric name 'TV' --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Ended Calculations for net_visit_metrics_monthly table
-------------------------------------------------------------------------------

--- Started calculations for net_hh_total_monthly

-- net_hh_total: A daily aggregate table that calculates unique counts of visits, visitors, households, 
-- and logged_in visitors (visitors that have a not-null visit_login_login_completed_timestamp)  visit__login__login_completed_timestamp 
-- The fields should be AS follows:
-- partition_date,
-- unique_visits_in_last_month,
-- unique_visitors_in_last_month,
-- unique_visitors_logged_in_last_month,
-- unique_email_visitors_in_last_month,
-- unique_hh_in_last_month

-------------------------------------------------------------------------------
-- Begin inserts into temp table: net_unique_visits_counts_monthly --

DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_visits_counts_monthly;
CREATE TABLE ${env:TMP_db}.net_unique_visits_counts_monthly(
visits_run_date String,
unique_visits_in_last_month INT,
company STRING 
);

----------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_unique_visits_counts_monthly
SELECT
-- run partition_date is constant for all metrics AS the reference partition_date
'${env:MONTH_END_DATE}' AS visits_run_date,
(SIZE(COLLECT_SET(visit__visit_id))) AS unique_visits_in_last_month,
'L-CHTR' AS company
FROM
net_events
WHERE
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_visits_counts_monthly
--SELECT
--'${env:MONTH_END_DATE}' AS visits_run_date,
--(SIZE(COLLECT_SET(visit__visit_id))) AS unique_visits_in_last_month,
--'L-BHN' AS company
--FROM
--bhn_residential_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_visits_counts_monthly
--SELECT
--'${env:MONTH_END_DATE}' AS visits_run_date,
--(SIZE(COLLECT_SET(visit__visit_id))) AS unique_visits_in_last_month,
--'L-TWC' AS company
--FROM
--twc_residential_global_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;
--
--SELECT '--***** End inserts into temp table: net_unique_visits_counts_monthly *****--'
--;

-- End inserts into temp table: net_unique_visits_counts_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin inserts into temp table: net_unique_visitor_counts_monthly --

DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_visitor_counts_monthly;
CREATE TABLE ${env:TMP_db}.net_unique_visitor_counts_monthly(
visitor_run_date String,
unique_visitors_in_last_month INT,
company STRING 
);

----------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_unique_visitor_counts_monthly
SELECT
-- run partition_date is constant for all metrics AS the reference partition_date
'${env:MONTH_END_DATE}' AS visitor_run_date,
(SIZE(COLLECT_SET(visit__device__enc_uuid))) AS unique_visitors_in_last_month,
'L-CHTR' AS company
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_visitor_counts_monthly
--SELECT
--'${env:MONTH_END_DATE}' AS visitor_run_date,
--(SIZE(COLLECT_SET(visit__device__enc_uuid))) AS unique_visitors_in_last_month,
--'L-BHN' AS company
--FROM 
--bhn_residential_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_visitor_counts_monthly
--SELECT
--'${env:MONTH_END_DATE}' AS visitor_run_date,
--(SIZE(COLLECT_SET(visit__device__enc_uuid))) AS unique_visitors_in_last_month,
--'L-TWC' AS company
--FROM 
--twc_residential_global_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;
--
SELECT '--***** End inserts into temp table: net_unique_visitor_counts_monthly *****--'
;

-- End inserts into temp table: net_unique_visitor_counts_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin inserts into temp table: net_unique_logged_visitors_counts_monthly --

DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_logged_visitors_counts_monthly;
CREATE TABLE ${env:TMP_db}.net_unique_logged_visitors_counts_monthly(
logged_run_date String,
unique_visitors_logged_in_last_month INT,
company STRING 
);

----------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_unique_logged_visitors_counts_monthly
SELECT 
-- run partition_date is constant for all metrics AS the reference partition_date
'${env:MONTH_END_DATE}' AS logged_run_date,
(SIZE(COLLECT_SET(
IF((visit__account__enc_account_number IS NOT NULL), visit__device__enc_uuid, NULL))
)) AS unique_visitors_logged_in_last_month,
'L-CHTR' AS company
FROM
net_events
WHERE
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_logged_visitors_counts_monthly
--SELECT 
---- run partition_date is constant for all metrics AS the reference partition_date
--'${env:MONTH_END_DATE}' AS logged_run_date,
--(SIZE(COLLECT_SET(
--IF((visit__account__enc_account_number IS NOT NULL), visit__device__enc_uuid, null))
--)) AS unique_visitors_logged_in_last_month,
--'L-BHN' AS company
--FROM
--bhn_residential_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_logged_visitors_counts_monthly
--SELECT 
---- run partition_date is constant for all metrics AS the reference partition_date
--'${env:MONTH_END_DATE}' AS logged_run_date,
--(SIZE(COLLECT_SET(
--IF((visit__account__enc_account_number IS NOT NULL), visit__device__enc_uuid, null))
--)) AS unique_visitors_logged_in_last_month,
--'L-TWC' AS company
--FROM
--twc_residential_global_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;
--
SELECT '--***** End inserts into temp table: net_unique_logged_visitors_counts_monthly *****--'
;

-- End inserts into temp table: net_unique_logged_visitors_counts_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin inserts into temp table: net_unique_email_visitors_counts_monthly --

DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_email_visitors_counts_monthly;
CREATE TABLE ${env:TMP_db}.net_unique_email_visitors_counts_monthly(
run_date String,
unique_email_visitors_in_last_month INT,
company STRING 
);

----------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_unique_email_visitors_counts_monthly
SELECT 
-- run partition_date is constant for all metrics AS the reference partition_date
'${env:MONTH_END_DATE}' AS run_date,
(SIZE(COLLECT_SET(
IF((message__name = 'Email' AND state__view__current_page__name = 'home'), visit__device__enc_uuid, null))
)) AS unique_email_visitors_in_last_month,
'L-CHTR' AS company
FROM
net_events
WHERE
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_email_visitors_counts_monthly
--SELECT 
--'${env:MONTH_END_DATE}' AS run_date,
--NULL AS unique_email_visitors_in_last_month,
--'L-BHN' AS company
--FROM
--bhn_residential_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_unique_email_visitors_counts_monthly
--SELECT 
--'${env:MONTH_END_DATE}' AS run_date,
--NULL AS unique_email_visitors_in_last_month,
--'L-TWC' AS company
--FROM
--twc_residential_global_events
--WHERE 
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;
--
SELECT '--***** End inserts into temp table: net_unique_email_visitors_counts_monthly *****--'
;

-- End inserts into temp table: net_unique_email_visitors_counts_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin inserts into temp table: net_hh_total_monthly --

DROP TABLE IF EXISTS ${env:TMP_db}.net_hh_total_monthly;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_hh_total_monthly(
partition_date STRING,
unique_hh INT,
company STRING 
);

----------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_hh_total_monthly
SELECT
'${env:MONTH_END_DATE}' AS partition_date,
SIZE(COLLECT_SET(visit__account__enc_account_number)) AS unique_hh,
'L-CHTR' AS company
FROM
net_events
WHERE
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_hh_total_monthly
--SELECT
--'${env:MONTH_END_DATE}' AS partition_date,
--NULL AS unique_hh,
--'L-BHN' AS company
--FROM
--bhn_residential_events
--WHERE
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;

----------------------------------------

--INSERT INTO TABLE ${env:TMP_db}.net_hh_total_monthly
--SELECT 
--'${env:MONTH_END_DATE}' AS partition_date,
--SIZE(COLLECT_SET(visit__account__enc_account_number)) AS unique_hh,
--'L-TWC' AS company
--FROM
--twc_residential_global_events
--WHERE
--epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
--;

SELECT '--***** End inserts into temp table: net_hh_total_monthly *****--'
;

-- End inserts into temp table: net_hh_total_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin inserts into table: net_hh_total_monthly --

INSERT INTO TABLE net_hh_total_monthly
SELECT
-- run partition_date is constant for all metrics AS the reference partition_date
'${env:YEAR_MONTH}' AS YEAR_MONTH,
unique_visits_in_last_month AS unique_visits,
unique_visitors_in_last_month AS unique_visitors,
unique_visitors_logged_in_last_month AS unique_visitors_logged,
unique_email_visitors_in_last_month AS unique_email_visitors,
unique_hh,
hh.company
FROM
${env:TMP_db}.net_hh_total_monthly hh 
INNER JOIN ${env:TMP_db}.net_unique_visits_counts_monthly 
ON partition_date = visits_run_date
INNER JOIN ${env:TMP_db}.net_unique_visitor_counts_monthly 
ON partition_date = visitor_run_date
INNER JOIN ${env:TMP_db}.net_unique_email_visitors_counts_monthly 
ON partition_date = run_date
INNER JOIN ${env:TMP_db}.net_unique_logged_visitors_counts_monthly 
ON partition_date = logged_run_date
;

--Formerly dropped tables at end of job. Will keep for history now.
-- 
--DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_visits_counts_monthly;
--DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_email_visitors_counts_monthly;
--DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_logged_visitors_counts_monthly;
--DROP TABLE IF EXISTS ${env:TMP_db}.net_unique_visitor_counts_monthly;
--DROP TABLE IF EXISTS ${env:TMP_db}.net_hh_total_monthly;
--

SELECT '--***** End inserts into table: net_hh_total_monthly *****--'
;

-- End inserts into table: net_hh_total_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin inserts into table: net_hh_account_monthly --

-- Household account list to get counts of Intersections
INSERT INTO TABLE net_hh_account_monthly 
SELECT
'${env:YEAR_MONTH}' AS year_month,
COUNT(DISTINCT visit__account__enc_account_number) AS total_unique_hhs,
collect_set(visit__account__enc_account_number) AS account_number_list
FROM 
net_events
WHERE 
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

-- 6 month account retention table
--SET hive.auto.convert.join=false;

SELECT '--***** End inserts into table: net_hh_account_monthly *****--'
;

-- End inserts into table: net_hh_account_monthly --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin calculations for table: net_accounts_monthly_report --

DROP TABLE IF EXISTS net_accounts_monthly_report; --drop table

CREATE TABLE net_accounts_monthly_report --create table for 6 month period
AS
SELECT 
left_year_month, 
right_year_month, 
left_total_unique_hhs, 
right_total_unique_hhs, 
size(array_intersect(left_account_number_list, right_account_number_list)) AS Intersect_total_unique_hhs
from 
(
SELECT
year_month AS left_year_month,
total_unique_hhs AS left_total_unique_hhs,
account_number_list AS left_account_number_list
FROM 
net_hh_account_monthly
ORDER BY left_year_month DESC LIMIT 6
)a
JOIN 
(
SELECT
year_month AS right_year_month,
total_unique_hhs AS right_total_unique_hhs,
account_number_list AS right_account_number_list
FROM 
net_hh_account_monthly
ORDER BY right_year_month DESC LIMIT 6
) b
WHERE left_year_month <= right_year_month;
