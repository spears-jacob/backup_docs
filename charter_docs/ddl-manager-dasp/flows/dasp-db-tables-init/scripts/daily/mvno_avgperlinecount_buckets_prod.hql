CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.mvno_avgperlinecount_buckets_prod (
  line_bucket                                                       string,
  rate_plan_code                                                    string,
  smb_ind                                                           string,
  line_count_end                                                    string,
  usage_gb                                                          string,
  acct_count                                                        string,
  metric_name                                                       string,
  actualdataaverage                                                 string
)
PARTITIONED BY
(partition_month string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
