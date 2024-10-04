CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_extract_voice_of_customer_troubleshooting (
  datetime_denver                                    string,
  visit__account__enc_account_number                 string,
  visit__account__enc_account_billing_id             string,
  visit__visit_id                                    string,
  visit__application_details__application_name       string,
  state__view__current_page__user_journey            string,
  state__view__current_page__user_sub_journey        string,
  visit__account__details__mso                       string,
  division                                           string,
  divisionID                                         string,
  system                                             string,
  prin                                               string,
  agent                                              string
)
PARTITIONED BY (denver_date string)
STORED AS PARQUET
LOCATION '${s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
;
