CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quality_api_score(
  `porfolio_team` string, 
  `application_version` string, 
  `api_architecture` string, 
  `call_name` string, 
  `number_of_visits` int, 
  `avg_response_time` float, 
  `success_rate` float, 
  `continuous_response_threshold` float, 
  `q1` float, 
  `q2` float, 
  `q3` float, 
  `q4` float, 
  `q5` float, 
  `api_score` float, 
  `avg_response_time_component` float, 
  `success_rate_component` float, 
  `application_name` string, 
  `configuration_factor` string
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
