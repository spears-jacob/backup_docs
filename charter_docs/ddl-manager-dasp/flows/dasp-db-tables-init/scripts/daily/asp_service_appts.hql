CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_service_appts
(
eid                 	string,
ft_name             	string,
ft_current_step     	int,
ft_number_of_steps  	int,
ft_featuretype      	string,
appt_scheduled      	boolean,
occurrences         	bigint,
app_name            	string
)
PARTITIONED BY (
partition_date_utc  	string
)
LOCATION '${s3_location}'
TBLPROPERTIES ('orc.compress'='SNAPPY')
;
