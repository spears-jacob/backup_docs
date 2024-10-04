CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_summary_call_agg_master 
(
	visit_id                      STRING,
	device_id                     STRING,
	acct_id                       STRING,
	application_group             STRING,
	application_type              STRING,
	mso                           STRING,
	cause_description             STRING,
	issue_description             STRING,
	resolution_description        STRING,
	call_count                    INT,
	date_level                    STRING

)
	PARTITIONED BY (denver_date STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Aggregate - With PII (3 Years)")
