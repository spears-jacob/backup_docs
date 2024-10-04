CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_billing_scorecard_callin_rate
(
    application_name string,
    mso string,
    metric_name string,
    calls_with_visit int,
    total_visits int,
    call_in_rate double
)
    PARTITIONED BY (label_date_denver string, grain string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
