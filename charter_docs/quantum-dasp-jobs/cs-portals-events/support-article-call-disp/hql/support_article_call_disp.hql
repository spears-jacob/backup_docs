USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
-- CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
-- CREATE TEMPORARY FUNCTION aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256'

--------------------------------------------------------------------------------
------------------ Create temporary tables for job data sets -------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.cqe_pt_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.cqe_pt_${env:CLUSTER} AS
SELECT DISTINCT
visit__visit_id,
visit__application_details__application_name,
visit__application_details__app_version,
--state__view__current_page__page_title,
-- aes_decrypt256(state__view__current_page__enc_page_title, 'aes256') AS state__view__current_page__page_title,
state__view__current_page__dec_page_title AS state__view__current_page__page_title,
partition_date_utc
FROM `${env:CQES}` cqe
WHERE (partition_date_utc >= '${env:START_DATE}'
  AND partition_date_utc < '${env:END_DATE}')
AND visit__application_details__application_name = 'SpecNet'
AND message__name = 'pageView'
AND visit__account__enc_account_number IS NOT NULL
AND state__view__current_page__page_name = 'supportArticle'
;

DROP TABLE IF EXISTS ${env:TMP_db}.calls1_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.calls1_${env:CLUSTER} AS
SELECT DISTINCT
visit_id,
call_inbound_key,
minutes_to_call
FROM `${env:CWPV}`
WHERE (call_date >= '${env:START_DATE}'
  AND call_date < '${env:ENDDATE}')
AND visit_type IN ('specnet')
;

DROP TABLE IF EXISTS ${env:TMP_db}.calls_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.calls_${env:CLUSTER} AS
SELECT
visit_id,
call_inbound_key,
minutes_to_call,
ROW_NUMBER() OVER(PARTITION BY call_inbound_key ORDER BY minutes_to_call) AS visit_n
FROM ${env:TMP_db}.calls1_${env:CLUSTER}
;

DROP TABLE IF EXISTS ${env:TMP_db}.atom_cc_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.atom_cc_${env:CLUSTER} AS
SELECT DISTINCT
call_inbound_key,
call_type,
product,
issue_description,
cause_description,
resolution_description
FROM `${env:CCCD}`
WHERE (call_end_date_utc >= '${env:START_DATE}'
  AND call_end_date_utc < '${env:ENDDATE}')
AND segment_handled_flag = 1
AND encrypted_account_number_256 IS NOT NULL
;

--------------------------------------------------------------------------------
------------ Join data sets for preparation of Sankey calculations -------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_support_article_call_prep
PARTITION (partition_date_utc)

SELECT
visit__visit_id,
visit__application_details__application_name,
b.call_inbound_key,
call_type,
product,
issue_group,
cause_group,
resolution_group,
state__view__current_page__page_title,
partition_date_utc
FROM ${env:TMP_db}.cqe_pt_${env:CLUSTER} a
  LEFT join ${env:TMP_db}.calls_${env:CLUSTER} b ON a.visit__visit_id = b.visit_id
  LEFT JOIN ${env:TMP_db}.atom_cc_${env:CLUSTER} c ON c.call_inbound_key = b.call_inbound_key
  LEFT JOIN ${env:DASP_db}.cs_dispositions_issue_groups ig
    ON c.issue_description = ig.issue_description
    AND ig.version = 'current'
  LEFT JOIN ${env:DASP_db}.cs_dispositions_cause_groups cg
    ON c.cause_description = cg.cause_description
    AND cg.version = 'current'
  LEFT JOIN ${env:DASP_db}.cs_dispositions_resolution_groups rg
    ON c.resolution_description = rg.resolution_description
    AND rg.version = 'current'
WHERE (a.partition_date_utc >= '${env:START_DATE}'
  AND a.partition_date_utc < '${env:END_DATE}')
;

--------------------------------------------------------------------------------
--------------------------- Build Sankey duplicates ----------------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_support_article_sankey_calc_${env:CLUSTER} PURGE;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_support_article_sankey_calc_${env:CLUSTER}   AS
SELECT
visit__visit_id,
visit__application_details__application_name,
call_inbound_key,
call_type,
product,
issue_group,
cause_group,
resolution_group,
state__view__current_page__page_title,
'sankey' AS table_name,
partition_date_utc
FROM ${env:DASP_db}.asp_support_article_call_prep
WHERE partition_date_utc >= '${env:START_DATE}'
  AND partition_date_utc < '${env:END_DATE}'
;

INSERT INTO ${env:TMP_db}.asp_support_article_sankey_calc_${env:CLUSTER}
SELECT
visit__visit_id,
visit__application_details__application_name,
call_inbound_key,
call_type,
product,
issue_group,
cause_group,
resolution_group,
state__view__current_page__page_title,
'sankey1' AS table_name,
partition_date_utc
FROM ${env:DASP_db}.asp_support_article_call_prep
WHERE partition_date_utc >= '${env:START_DATE}'
  AND partition_date_utc < '${env:END_DATE}'
;

--------------------------------------------------------------------------------
---------------------- Insert into Tableau feeder table ------------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_support_article_call_disposition PARTITION (partition_date_utc)
SELECT
visit__visit_id,
visit__application_details__application_name,
call_inbound_key,
call_type,
product,
issue_group,
cause_group,
resolution_group,
state__view__current_page__page_title,
table_name,
partition_date_utc
FROM ${env:TMP_db}.asp_support_article_sankey_calc_${env:CLUSTER}
;

DROP TABLE IF EXISTS ${env:TMP_db}.dasp_support_article_sankey_calc_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.cqe_pt_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.calls1_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.calls_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.atom_cc_${env:CLUSTER} PURGE;
