USE ${env:TMP_db};

SET hive.merge.tezfiles=true;

SELECT '***** Creating ReprocessDateTable ******'
;

DROP TABLE IF EXISTS ${env:ReprocessDateTable} PURGE;

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable}
(
    run_date string
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs:///tmp/tmp_asapp-visitid-data/${env:ReprocessDateTable}'
TBLPROPERTIES('orc.compress'='snappy');

DROP TABLE IF EXISTS values__tmp__table__1 PURGE;
-- This DROP flow added due to fails at the next INSERT INTO step.
-- Without this DROP we are getting
-- SemanticException [Error 10293]: Unable to create temp file for insert values AlreadyExistsException
-- (message:Table values__tmp__table__1 already exists.)

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');
