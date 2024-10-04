USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET mapreduce.reduce.memory.mb=50000;
SET mapreduce.reduce.java.opts=-Xmx5000m;

--------------------------------------------------------------------------------
------------------------- ***** Load Encryption ***** --------------------------
--------------------------------------------------------------------------------

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

--------------------------------------------------------------------------------
----------------------- ***** Declined Invitations ***** -----------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE LOCAL DIRECTORY '${env:PWD}' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' NULL DEFINED AS ''
SELECT Distinct
application_name,
aes_decrypt256(acct_number, 'aes256') as acct_number,
survey_action,
biller_type,
aes_decrypt256(division, 'aes256') AS division,
aes_decrypt256(division_id, 'aes256') AS division_id,
aes_decrypt256(sys, 'aes256') as sys,
aes_decrypt256(prin, 'aes256') as prin,
aes_decrypt256(agent, 'aes256') as agent,
aes_decrypt256(acct_site_id, 'aes256') as acct_site_id,
aes_decrypt256(account_company, 'aes256') as account_company,
aes_decrypt256(acct_franchise, 'aes256') as acct_franchise,
day_diff,
rpt_dt,
partition_date_utc
from ${env:DASP_db}.asp_medallia_interceptsurvey
WHERE partition_date_utc = '${env:START_DATE}'
;

--------------------------------------------------------------------------------
------------------------------- ***** END ***** --------------------------------
--------------------------------------------------------------------------------
