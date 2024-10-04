CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_cross_platform
(
	acct_id                                           STRING,
	mso                                               STRING,
	user_package                                      STRING,
	msa_first_initiated_self_installs_visit_id        STRING,
	msa_first_initiated_self_installs_timestamp       BIGINT,
	msa_complete_activation_self_installs_visit_id    STRING,
	msa_complete_activation_self_installs_timestamp   BIGINT,
	web_first_initiated_self_installs_visit_id        STRING,
	web_first_initiated_self_installs_timestamp       BIGINT,
	web_complete_activation_self_installs_visit_id    STRING,
	web_complete_activation_self_installs_timestamp   BIGINT,
	msa_initiated_self_install                        INT,
	web_initiated_self_install                        INT,
	msa_complete_self_install                         INT,
	web_complete_self_install                         INT,
	device_activated                                  INT
)
	PARTITIONED BY (denver_date STRING)
	STORED AS ORC
	LOCATION '${s3_location}'
	TBLPROPERTIES ("orc.compress" = "SNAPPY", "retention_policy" = "Event Level - With PII (1 Year)")
