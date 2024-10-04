CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quality_component_distribution (
  `mso` string,
  `flow_group` string,
  `application_name` string,
  `application_type` string,
  `application_version` string,
  `elements_string_value` string,
  `feature_name` string,
  `billing_services` string,
  `target_application_name` string,
  `target_application_type` string,
  `target_application_version` string,
  `component_name` string,
  `component_bucket` tinyint,
  `visit_count` int,
  `total_visits_component` int,
  `total_visits_component_with_nulls` int,
  `percent_in_bucket` double,
  `percent_in_bucket_with_nulls` double,
  `grouping_id` bigint,
  `custom_grouping_id` string,
  `reporting_application` string,
  `reporting_application_version` string,
  `reporting_application_type` string,
  `has_idm_login` string,
  `accessibility_enabled` string,
  `corelegacybillingotp` string, 
  `deviceformfactor` string, 
  `configuration_factors` string, 
  `custom_grouping_id_decimal` bigint,
  `operating_system` string
)
PARTITIONED BY (denver_date STRING, source_job STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
