CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_prodmonthly_call_in_rate_source
(
    visit_type            string,
    app_type         string,
    form_factor           string,
    calls_with_visit      bigint,
    authenticated_visits  bigint
)
    PARTITIONED BY (`call_date` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
