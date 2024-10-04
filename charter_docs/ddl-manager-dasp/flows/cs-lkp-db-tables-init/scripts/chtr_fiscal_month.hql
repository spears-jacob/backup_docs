CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.chtr_fiscal_month
(
    partition_date string,
    fiscal_month   string
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION '${s3_location}';
