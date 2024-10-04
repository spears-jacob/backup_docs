CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_call_in_rate_sps
(
    acct_agent_mso        string,
    visit_type            string,
    customer_type         string,
    calls_with_visit      bigint,
    validated_calls       bigint,
    distinct_call_accts   bigint,
    handled_calls         bigint,
    distinct_visit_accts  bigint,
    authenticated_visits  bigint,
    sps_flag              string
)
    PARTITIONED BY (`call_date` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY") 
