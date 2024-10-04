USE ${env:DASP_db};

set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.exec.max.dynamic.partitions.pernode=800;
set hive.cli.print.header=true;

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

INSERT OVERWRITE LOCAL DIRECTORY '${env:PWD}' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT DISTINCT
  denver_date,
  datetime_denver,
  aes_decrypt256(mobile_account_number_enc, 'aes256') as mobile_account_number,
  aes_decrypt256(mobile_device_number_enc, 'aes256') as mobile_device_number,
  final_supportPage
from asp_extract_voice_of_customer_mobile
WHERE denver_date='${env:START_DATE}'
ORDER BY denver_date, datetime_denver;
