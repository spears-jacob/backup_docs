CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_onlineform_errors_dashboard
  (
   visit__visit_id                                                     string
  ,visit__application_details__application_name                        string
  ,visit__login__logged_in                                             boolean
  ,pageview__sequence_number                                           int
  ,pageview__count_binary                                              int
  ,state__view__current_page__page_sequence_number                     int
  ,state__view__current_page__dec_page_title                           string
  ,submit__sequence_number                                             int
  ,submit_state__view__current_page__elements__element_string_value    string
  ,submit__count_binary                                                int
  ,error__sequence_number                                              int
  ,application__error__enc_error_extras                                map<string,string>
  ,field                                                               string
  ,message                                                             string
  ,custom_visit_had_call                                               int
  ,issue_description                                                   string
  ,cause_description                                                   string
  ,call_date                                                           string
  )
PARTITIONED BY
( partition_date_utc                                                   string  )
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
