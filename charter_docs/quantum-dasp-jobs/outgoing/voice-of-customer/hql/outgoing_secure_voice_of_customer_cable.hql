USE ${env:DASP_db};

set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=5120000000;
set hive.merge.size.per.task=5120000000;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.exec.max.dynamic.partitions.pernode=800;
set hive.cli.print.header=true;

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

INSERT OVERWRITE LOCAL DIRECTORY '${env:PWD}' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT DISTINCT
       aes_decrypt256(acct_number_enc, 'aes256') as account_number,
       aes_decrypt256(mobile_acct_enc, 'aes256') as mobile_account_number,
       visit_id,
       app,
       journey,
       sub_journey,
       transaction_timestamp,
       biller_type as billing_system,
       aes_decrypt256(division_id, 'aes256') as division_id,
       aes_decrypt256(site_sys, 'aes256') as sys,
       aes_decrypt256(prn, 'aes256') as prn,
       aes_decrypt256(agn, 'aes256') as agn,
       aes_decrypt256(division, 'aes256') as division,
       aes_decrypt256(acct_site_id, 'aes256') as acct_site_id,
       aes_decrypt256(acct_company, 'aes256') as acct_company,
       aes_decrypt256(acct_franchise, 'aes256') as acct_franchise,
       mso as legacy_company,
       event_type,
       denver_date
from asp_extract_voice_of_customer_cable
WHERE denver_date='${env:START_DATE}'
ORDER BY denver_date, visit_id, transaction_timestamp;
