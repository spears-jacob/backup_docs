CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_composite_score_kpi_agg_daily
(
	grouping_id                            INT,
	timeframe                              STRING,
	application_group                      STRING,
	application_version                    STRING,
	user_package                           STRING,
	metric_name                            STRING,
	metric_value                           DOUBLE,
	transaction_duration_filtered_ratio    DOUBLE,
	api_response_time_filtered_ratio       DOUBLE,
	transaction_duration_bucket            DOUBLE,
	api_response_time_bucket               DOUBLE,
	activation_both_derived                DOUBLE,
	activation_both                        DOUBLE,
	activation_success                     DOUBLE,
	activation_failure_not                 DOUBLE,
	number_uniq_visit                      INT,
	date_level                             STRING,
	avg_transaction_duration               DOUBLE,
	avg_api_response_time                  DOUBLE
)
	PARTITIONED BY (denver_date STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Aggregate - With PII (3 Years)")
