CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_app_daily_app_figures_reviews
(
    company  string,
    platform string,
    stars    string,
    review   string
)
    PARTITIONED BY (date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
