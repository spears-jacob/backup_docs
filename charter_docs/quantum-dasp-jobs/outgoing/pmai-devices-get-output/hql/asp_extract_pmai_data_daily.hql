USE ${env:DASP_db};

SET hive.auto.convert.join=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.size.per.task=2048000000;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.tezfiles=true;
SET hive.optimize.sort.dynamic.partition=false;
SET hive.stats.fetch.column.stats=false;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.vectorized.execution.enabled=false;
SET mapreduce.input.fileinputformat.split.maxsize=536870912;
SET mapreduce.input.fileinputformat.split.minsize=536870912;
SET orc.force.positional.evolution=true;

ADD JAR ${env:JARS_S3_LOCATION}/charter-hive-udf-0.1-SNAPSHOT.jar ;
ADD JAR ${env:JARS_S3_LOCATION}/epochtotimestamp-1.0-SNAPSHOT.jar ;
ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar ;
ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;

DROP FUNCTION IF EXISTS aes_encrypt256;
CREATE TEMPORARY FUNCTION aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256';
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';
CREATE TEMPORARY FUNCTION epoch_timestamp AS 'Epoch_Timestamp';

MSCK REPAIR TABLE ${env:SEC_db}.asp_pmai_sia_account_in;

select sia_id,
       aes_encrypt256(account_number,'aes256') as acct,
       aes_encrypt256(division_division_id, 'aes256') as division_division_id,
       aes_encrypt256(division_division, 'aes256') as division_division,
       aes_encrypt256(regexp_extract(division_division_id,'\.(\\d+)',1), 'aes256') as division,
       aes_encrypt256(UPPER(regexp_extract(division_division_id,'^([^\.]+)\.?',1)), 'aes256') as division_id,
       mso
from ${env:SEC_db}.asp_pmai_sia_account_in LIMIT 5;

--select sia_id,
--       ${env:GLOBAL_DB}.aes_encrypt256(account_number,'aes256') as acct,
--       ${env:GLOBAL_DB}.aes_encrypt256(division_division_id, 'aes256') as division_division_id,
--       ${env:GLOBAL_DB}.aes_encrypt256(division_division, 'aes256') as division_division,
--       ${env:GLOBAL_DB}.aes_encrypt256(regexp_extract(division_division_id,'\.(\\d+)',1), 'aes256') as division,
--       ${env:GLOBAL_DB}.aes_encrypt256(UPPER(regexp_extract(division_division_id,'^([^\.]+)\.?',1)), 'aes256') as division_id,
--       mso
--from ${env:SEC_db}.asp_pmai_sia_account_in LIMIT 5;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_pmai_sia_device_tmp_sspp_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_pmai_sia_device_tmp_sspp_${env:CLUSTER} AS
SELECT DISTINCT
    CASE
        WHEN visit__account__details__mso = 'BH' THEN 'BHN'
        WHEN visit__account__details__mso = 'CHARTER' THEN 'CHTR'
        ELSE visit__account__details__mso
    END AS mso,
    visit__application_details__application_name AS application_name,
    visit__application_details__platform_type AS application_platform,
    visit__account__enc_account_number AS acct_number,
    visit__account__enc_account_billing_id AS billing_id,
    visit__account__enc_account_billing_division AS billing_division,
    visit__account__enc_account_billing_division_id AS billing_division_id,
    visit__device__enc_uuid AS device_id,
    visit__visit_id AS visit_id,
    visit__device__manufacturer AS device_manufacturer,
    visit__device__model AS device_model,
    visit__device__operating_system AS device_os,
    visit__connection__network_cell_carrier AS cell_carrier,
    visit__connection__network_cell_network_type AS cell_network_type,
    epoch_converter(MAX(received__timestamp) OVER (PARTITION BY visit__account__enc_account_number, visit__visit_id, visit__application_details__platform_type),'America/Denver') as last_date_accessed_received
FROM
      ${env:ENVIRONMENT}.core_quantum_events_sspp AS quantum_data
WHERE (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
  AND visit__application_details__application_name in ('MySpectrum','SpecMobile')
;

INSERT overwrite table asp_pmai_sia_device_out PARTITION (run_date, application_name)
SELECT DISTINCT
        sia_id,
        application_platform,
        visit_id,
        device_id,
        device_manufacturer,
        device_model,
        device_os,
        cell_carrier,
        cell_network_type,
        last_date_accessed_received,
        '${hiveconf:RUN_DATE}' as RUN_DATE,
        application_name
  FROM (SELECT *
          FROM ${env:TMP_db}.asp_pmai_sia_device_tmp_sspp_${env:CLUSTER}
         WHERE application_name in ('MySpectrum','SpecMobile')
       ) AS quantum_data
  JOIN ${env:SEC_db}.asp_pmai_sia_account_in AS pmai_data
    ON quantum_data.billing_division = aes_encrypt256(regexp_extract(pmai_data.division_division_id,'\.(\\d+)',1), 'aes256')
   AND quantum_data.billing_division_id = aes_encrypt256(UPPER(regexp_extract(pmai_data.division_division_id,'^([^\.]+)\.?',1)), 'aes256')
   AND quantum_data.acct_number = aes_encrypt256(pmai_data.account_number, 'aes256')
;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_pmai_sia_device_tmp_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_pmai_sia_device_tmp_${env:CLUSTER} AS
SELECT DISTINCT
    CASE
        WHEN visit__account__details__mso = 'BH' THEN 'BHN'
        WHEN visit__account__details__mso = 'CHARTER' THEN 'CHTR'
        ELSE visit__account__details__mso
    END AS mso,
    visit__application_details__application_name AS application_name,
    visit__application_details__platform_type AS application_platform,
    visit__account__enc_account_number AS acct_number,
    visit__account__enc_account_billing_id AS billing_id,
    visit__account__enc_account_billing_division AS billing_division,
    visit__account__enc_account_billing_division_id AS billing_division_id,
    visit__device__enc_uuid AS device_id,
    visit__visit_id AS visit_id,
    visit__device__manufacturer AS device_manufacturer,
    visit__device__model AS device_model,
    visit__device__operating_system AS device_os,
    visit__connection__network_cell_carrier AS cell_carrier,
    visit__connection__network_cell_network_type AS cell_network_type,
    epoch_converter(MAX(received__timestamp) OVER (PARTITION BY visit__account__enc_account_number, visit__visit_id, visit__application_details__platform_type),'America/Denver') as last_date_accessed_received
FROM
      ${env:ENVIRONMENT}.core_quantum_events AS quantum_data
WHERE (partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
  AND visit__application_details__application_name in ('OneApp')
;

INSERT overwrite table asp_pmai_sia_device_out PARTITION (run_date, application_name)
SELECT DISTINCT
        sia_id,
        application_platform,
        visit_id,
        device_id,
        device_manufacturer,
        device_model,
        device_os,
        cell_carrier,
        cell_network_type,
        last_date_accessed_received,
        '${hiveconf:RUN_DATE}' as RUN_DATE,
        application_name
  FROM (SELECT *
          FROM ${env:TMP_db}.asp_pmai_sia_device_tmp_${env:CLUSTER}
        WHERE application_name = 'OneApp'
       ) AS quantum_data
  JOIN ${env:SEC_db}.asp_pmai_sia_account_in AS pmai_data
    ON quantum_data.billing_division =  aes_encrypt256(regexp_extract(pmai_data.division_division_id,'\.(\\d+)',1), 'aes256')
   AND quantum_data.billing_division_id =  aes_encrypt256(UPPER(regexp_extract(pmai_data.division_division_id,'^([^\.]+)\.?',1)), 'aes256')
   AND quantum_data.billing_id = aes_encrypt256(pmai_data.account_number, 'aes256')
;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_pmai_sia_device_tmp_sspp_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_pmai_sia_device_tmp_${env:CLUSTER} PURGE;
