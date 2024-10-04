CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_billing_scorecard_portals_metrics
(
    application_name string,
    mso              string,
    grouping_id      int,
    metric_type      string,
    metric_name      string,
    metric_value     double,
    prev_1w_value    double,
    prev_2w_value    double,
    prev_3w_value    double,
    unit_type        string,
    grain            string
)
    PARTITIONED BY (label_date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
