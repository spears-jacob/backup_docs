USE ${env:DASP_db};

SET hive.merge.size.per.task=4096000000;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.tezfiles=true;
SET hive.optimize.sort.dynamic.partition.threshold=-1;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/json-serde-1.3.9-SNAPSHOT-jar-with-dependencies.jar;
ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;

DROP FUNCTION IF EXISTS aes_encrypt256;
CREATE TEMPORARY FUNCTION aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256';

INSERT OVERWRITE TABLE asp_skycreek_proactive_maintenance_notifications PARTITION (partition_date_utc)
SELECT  eventdetail.eventdatetime,
        aes_encrypt256(accountnumber,'aes256') as accountnumber_enc,
        companycd,
        aes_encrypt256(spcdivisionid,'aes256') as spcdivisionid_enc,
        eventdetail.channel,
        eventdetail.commid,
        eventdetail.eventtype,
        tokenid,
        partition_date_utc
FROM  ${env:SEC_db}.asp_sentmsgs_epid
WHERE partition_date_utc >= '${env:ROLLING_03_START_DATE}'
  AND partition_date_utc <  '${env:END_DATE}'
;


