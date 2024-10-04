CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.daily_quantum_app_entry_metric_agg
(
  application_type    	string,
  operation_type      	string,
  number_of_visits    	bigint,
  has_login_success   	boolean,
  capabilities_api_failure	bigint,
  tdcs_api_failure    	bigint,
  blocking_api_failure	bigint,
  api_multiplier      	double,
  multiplier          	double,
  metric_name         	string,
  metric_value        	double,
  gid                 	bigint,
  mso                 	string,
  device_type         	string,
  connection_type     	string,
  network_status      	string,
  app_version         	string,
  cust_type           	string,
  peak_flag           	string
)
PARTITIONED BY (denver_date STRING)
STORED AS ORC
    LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
