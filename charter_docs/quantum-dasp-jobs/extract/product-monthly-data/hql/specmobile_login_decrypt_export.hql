USE ${env:DASP_db};

SET mapreduce.input.fileinputformat.split.maxsize=68709120;
SET mapreduce.input.fileinputformat.split.minsize=68709120;
SET hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.cli.print.header=true;

create temporary function aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256'
USING JAR 's3://pi-global-${env:ENVIRONMENT}-udf-jars/hadoop-libs-hive-1.0.10.jar';

SET hive.exec.compress.output=true;

select distinct
       date_denver,
       login_timestamp,
       aes_decrypt256(account_number) as account_number,
       aes_decrypt256(division_id) as division_id,
       aes_decrypt256(division) as division,
       aes_decrypt256(site_sys) as sys,
       aes_decrypt256(prn) as prn,
       aes_decrypt256(agn) as agn
from ${env:DASP_db}.asp_specmobile_login
where date_denver >= '${hiveconf:START_DATE}'
and  date_denver < '${hiveconf:END_DATE}'
;
