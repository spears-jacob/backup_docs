CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_product_monthly_time
(
    application_name  string,
    num_total         int,
    num_less_than2    int,
    num_between2_and4 int,
    num_between4_and6 iNT,
    num_larger_than6  INT,
    pct_less_than2    double,
    pct_between2_and4 double,
    pct_between4_and6 DOUBLE,
    pct_larger_than6  DOUBLE,
    run_date          DATE
)
    PARTITIONED BY (label_date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
