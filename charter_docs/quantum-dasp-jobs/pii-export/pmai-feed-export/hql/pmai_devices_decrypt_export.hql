USE ${env:DASP_db};

SET mapreduce.input.fileinputformat.split.maxsize=68709120;
SET mapreduce.input.fileinputformat.split.minsize=68709120;
SET hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.cli.print.header=true;

create temporary function aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
USING JAR 's3://pi-global-${env:ENVIRONMENT}-udf-jars/hadoop-libs-hive-1.0.10.jar';

SET hive.exec.compress.output=true;

select
      distinct
      sia_id,
      application_name,
      application_platform,
      visit_id,
      aes_decrypt256(device_id,'aes256') as device_id,
      device_manufacturer,
      device_model,
      device_os,
      cell_carrier,
      cell_network_type,
      last_date_accessed_received
from ${env:DASP_db}.asp_pmai_sia_device_out
WHERE run_date='${hiveconf:RUN_DATE}'
;
