	CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_quality_bucket_distribution
(
  application_name	string,
  application_version	string,
  score_type	string,
  component_name	string,
  component_bucket	int,
  visit_count	bigint,
  total_visits_component	bigint,
  percent_in_bucket	double,
  grouping_id	int
)
PARTITIONED BY (denver_date string)
  STORED AS ORC
  LOCATION '${s3_location}'
  TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
