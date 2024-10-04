CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_call_in_rate
(
    agent_mso          string,
    visit_type         string,
    customer_type      string,
    calls_with_visit   bigint,
    handled_acct_calls bigint,
    total_acct_calls   bigint,
    total_calls        bigint,
    total_acct_visits  bigint,
    total_visits       bigint
)
    PARTITIONED BY (`call_date` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
