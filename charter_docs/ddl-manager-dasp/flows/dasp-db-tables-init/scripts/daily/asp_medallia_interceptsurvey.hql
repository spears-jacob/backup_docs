CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_medallia_interceptsurvey
(
  application_name        string,
  acct_number             string,
  survey_action           string,
  biller_type             string,
  division                string,
  division_id             string,
  sys                     string,
  prin                    string,
  agent                   string,
  acct_site_id            string,
  account_company         string,
  acct_franchise          string,
  day_diff                int,
  rpt_dt                  date
)
PARTITIONED BY (partition_date_utc string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
