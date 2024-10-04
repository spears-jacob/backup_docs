USE ${env:DASP_db};

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.size.per.task=1024000000;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.tezfiles=true;
SET mapreduce.reduce.java.opts=-Xmx5000m;
SET mapreduce.reduce.memory.mb=50000;
SET orc.force.positional.evolution=true;

--------------------------------------------------------------------------------
------------------------- ***** Load Encryption ***** --------------------------
--------------------------------------------------------------------------------

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

INSERT OVERWRITE LOCAL DIRECTORY '${env:PWD}' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' NULL DEFINED AS ''
SELECT distinct
       external_session_id,
       application_name,
       UPPER(aes_decrypt256(account_number)) as account_number,
       UPPER(biller_type) as biller_type,
       date_denver,
       UPPER(aes_decrypt256(division)) as division,
       UPPER(aes_decrypt256(division_id)) as division_id,
       UPPER(aes_decrypt256(sys)) as sys,
       UPPER(aes_decrypt256(prn)) as prn,
       UPPER(aes_decrypt256(agn)) as agn,
       UPPER(aes_decrypt256(acct_site_id)) as acct_site_id,
       UPPER(aes_decrypt256(acct_company)) as acct_company,
       UPPER(aes_decrypt256(acct_franchise)) as acct_franchise,
       UPPER(aes_decrypt256(mobile_acct_enc)) as mobile_acct_enc
FROM asp_extract_asapp_visitid_data_daily_pvt
WHERE date_denver = DATE_ADD("${env:START_DATE}",1)
;
