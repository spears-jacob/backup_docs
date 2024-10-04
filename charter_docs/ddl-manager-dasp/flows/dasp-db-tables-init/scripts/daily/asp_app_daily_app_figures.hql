CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_app_daily_app_figures
(
    metric string,
    value  bigint
)
    PARTITIONED BY (company string,date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
