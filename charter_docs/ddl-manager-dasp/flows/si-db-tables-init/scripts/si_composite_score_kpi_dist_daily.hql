CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_composite_score_kpi_dist_daily
(
	grouping_id                  INT,
	application_group            STRING,
	application_version          STRING,
	user_package                 STRING,
	metric_name                  STRING,
	number_rows                  INT,
	number_visits                INT,
	pct_uniq_visits              DOUBLE,
	pct_duration                 DOUBLE,
	pct_api_response_time        DOUBLE,
	quality_score                DOUBLE,
	transaction_duration_bucket  DOUBLE,
	api_response_time_bucket     DOUBLE,
	all_true_yes                 INT,
	success_true_yes             INT,
	failure_true_not             INT,
	date_level                   STRING,
	avg_transaction_duration     DOUBLE,
	avg_api_response_time        DOUBLE
)
	PARTITIONED BY (denver_date STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Aggregate - With PII (3 Years)")
