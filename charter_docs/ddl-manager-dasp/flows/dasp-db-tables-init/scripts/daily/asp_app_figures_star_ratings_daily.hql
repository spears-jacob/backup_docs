CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_app_figures_star_ratings_daily
(
    one_star_to_date DECIMAL(10, 2),
    two_star_to_date DECIMAL(10, 2),
    thr_star_to_date DECIMAL(10, 2),
    fou_star_to_date DECIMAL(10, 2),
    fiv_star_to_date DECIMAL(10, 2),
    one_star_daily   DECIMAL(10, 2),
    two_star_daily   DECIMAL(10, 2),
    thr_star_daily   DECIMAL(10, 2),
    fou_star_daily   DECIMAL(10, 2),
    fiv_star_daily   DECIMAL(10, 2),
    product_name     STRING
)
    PARTITIONED BY (product_id STRING,platform STRING,version STRING,partition_date_denver STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
