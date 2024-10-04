USE ${env:SEC_db};

SET hive.merge.size.per.task=4096000000;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.tezfiles=true;
SET hive.optimize.sort.dynamic.partition.threshold=-1;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

SELECT "Inserting PII Data For asp_sentmsgs_hp (62-day lag, but lagging 9 days because it catches the majority of the records)";
INSERT OVERWRITE TABLE asp_sentmsgs_hp PARTITION (partition_date_utc)
SELECT
  header,
  payload,
  partition_date_utc
FROM ${env:TMP_db}.hp_${env:CLUSTER}_${env:STEP}
WHERE partition_date_utc >= '${env:ROLLING_09_START_DATE}'
  AND partition_date_utc <  '${env:END_DATE}'
;

DROP TABLE IF EXISTS ${env:TMP_db}.hp_${env:CLUSTER}_${env:STEP} PURGE;
