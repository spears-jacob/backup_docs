USE ${env:ENVIRONMENT};

SELECT "\n\nNow preparing temporary table for loading the defintions:\n\n${env:TMP_db}.asp_counts_definitions_raw\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_definitions_raw;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_definitions_raw
 (run          STRING,
  comments     STRING,
  hive_name    STRING,
  metric_name  STRING,
  sourceName   STRING,
  label4       STRING,
  label5       STRING,
  condition1   STRING,
  condition2   STRING,
  condition3   STRING,
  condition4   STRING,
  condition5   STRING,
  condition6   STRING
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1")
;
--skips first line of .csv file containing headers

SELECT "\n\nNow loading in ../bin/counts.tsv\n\n";

LOAD DATA LOCAL INPATH '../bin/counts.tsv' INTO TABLE ${env:TMP_db}.asp_counts_definitions_raw
;


INSERT OVERWRITE TABLE asp_counts_definitions PARTITION (partition_date_den)

select  run,
        comments,
        hive_name,
        metric_name,
        sourceName,
        label4,
        label5,
        condition1,
        condition2,
        condition3,
        condition4,
        condition5,
        condition6,
        to_utc_timestamp(current_timestamp,'UTC'),
        CURRENT_DATE
  from ${env:TMP_db}.asp_counts_definitions_raw
;
