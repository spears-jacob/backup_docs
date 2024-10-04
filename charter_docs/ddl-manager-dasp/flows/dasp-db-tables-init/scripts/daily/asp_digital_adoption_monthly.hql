CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_digital_adoption_monthly
(
   customertype	string,
   customerjourney	string,
   totalcusts	bigint,
   supportseeking_hh	bigint,
   digitaltp_hh	bigint,
   digitalonlytp_hh	bigint,
   call_counts	bigint,
   digital_first_call_counts	bigint,
   digitalawarenessratio	double,
   digitaleffectivenessratio	double,
   callsperengagedhh	double,
   digitalfirstcallsperengagedhh	double,
   nondigitalfirstcallsperengagedhh	double,
   sharedigitalfirstcalls	double
)
PARTITIONED BY (year_month string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
