CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_m2dot0_engagedHouseholds (
  visit__application_details__application_name      string,
  agg_visit__account__configuration_factors         string,
  first_visits                                      int,
  mvno_accounts                                     int,
  sum_src_sys_id                                    string,
  date_date                                         string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
