CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_dates
(
    calendar_date string,
    year          int,
    quarter       bigint,
    month         int,
    week_of_month string,
    week_of_year  string,
    day           int,
    day_of_week   string,
    day_of_week_s string,
    day_of_year   string,
    fiscal_month  string,
    fiscal_year   int,
    day_of_epoch  int,
    weekend       boolean,
    us_holiday    boolean
)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    LOCATION '${s3_location}';
