USE ${env:DASP_db};

SET hive.vectorized.execution.enabled = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.size.per.task=1024000000;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.cli.print.header=true;

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
DROP FUNCTION IF EXISTS aes_decrypt256;
CREATE FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

INSERT OVERWRITE LOCAL DIRECTORY '${env:PWD}' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT DISTINCT
       denver_date
      ,datetime_denver
      ,aes_decrypt256(account_number, 'aes256') as account_number
      ,aes_decrypt256(billing_id, 'aes256') as billing_id
      ,visit__visit_id
      ,visit__application_details__application_name
      ,state__view__current_page__user_journey
      ,state__view__current_page__user_sub_journey
      ,visit__account__details__mso
      ,aes_decrypt256(division, 'aes256') as division
      ,aes_decrypt256(divisionID, 'aes256') as divisionID
      ,aes_decrypt256(system, 'aes256') as system
      ,aes_decrypt256(prin, 'aes256') as prin
      ,aes_decrypt256(agent, 'aes256') as agent
from asp_extract_voice_of_customer_mobile_troubleshooting
WHERE denver_date='${env:START_DATE}'
ORDER BY denver_date, datetime_denver
;
