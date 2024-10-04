CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.daily_quantum_visit_app_entry
(
      visit_id            	string,
      application_type    	string,
      operation_type      	string,
      operation__success  	boolean,
      loginstop_msn       	bigint,
      loginstop_timestamp 	string,
      loginstart_timestamp	string,
      end_timestamp       	string,
      app_launch_time_ms  	bigint,
      api_response_time_ms	bigint,
      capabilities_api_failures	bigint,
      location_api_failures	bigint,
      topology_api_failures	bigint,
      tdcs_api_failures   	bigint,
      navinit_api_failures	bigint,
      device_id           	string,
      billing_id          	string,
      billing_division    	string,
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
