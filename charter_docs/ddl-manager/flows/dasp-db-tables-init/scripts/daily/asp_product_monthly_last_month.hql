CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_product_monthly_last_month
(
    metric_name       string,
    application_name  string,
    application_order string,
    pct_less_than2    double,
    pct_between2_and4 double,
    pct_between4_and6 double,
    pct_larger_than6  double,
    run_date          date
)
    PARTITIONED BY (label_date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
