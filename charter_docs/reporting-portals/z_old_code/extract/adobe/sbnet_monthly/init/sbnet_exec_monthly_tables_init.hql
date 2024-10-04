USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
(
  company string,
  source_file string,
  hhs_logged_in int,
  total_hhs int,
  my_account_page_views bigint,
  total_login_attempts bigint,
  total_login_successes bigint,
  rescheduled_service_appointments int,
  cancelled_service_appointments int,
  support_page_views bigint,
  online_statement_views bigint,
  one_time_payment_attempts bigint,
  one_time_payments bigint,
  auto_pay_setup_attempts bigint,
  auto_pay_setup_successes bigint,
  total_account_creation_attempts bigint,
  total_new_accounts_created bigint,
  new_account_creation_attempts_on_chtr_network bigint,
  new_accounts_created_on_chtr_network bigint,
  new_account_creation_attempts_off_chtr_network bigint,
  new_accounts_created_off_chtr_network bigint,
  total_sub_user_creation_attempts bigint,
  total_new_sub_users_created bigint,
  sub_user_creation_attempts_on_chtr_network bigint,
  sub_users_created_on_chtr_network bigint,
  sub_user_creation_attempts_off_chtr_network bigint,
  sub_users_created_off_chtr_network bigint,
  total_username_recovery_attempts bigint,
  total_username_recovery_successes bigint,
  username_recovery_attempts_on_chtr_network bigint,
  username_recovery_successes_on_chtr_network bigint,
  username_recovery_attempts_off_chtr_network bigint,
  username_recovery_successes_off_chtr_network bigint,
  total_attempts_to_reset_password bigint,
  total_successful_password_resets bigint,
  attempts_to_reset_password_on_chtr_network bigint,
  successful_password_resets_on_chtr_network bigint,
  attempts_to_reset_password_off_chtr_network bigint,
  successful_password_resets_off_chtr_network bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
(
  company string,
  source_file string,
  hhs_logged_in int,
  total_hhs int,
  my_account_page_views bigint,
  total_login_attempts bigint,
  total_login_successes bigint,
  rescheduled_service_appointments int,
  cancelled_service_appointments int,
  support_page_views bigint,
  online_statement_views bigint,
  one_time_payment_attempts bigint,
  one_time_payments bigint,
  one_time_payments_with_autopay_setup bigint,
  auto_pay_setup_attempts bigint,
  auto_pay_setup_successes bigint,
  total_account_creation_attempts bigint,
  total_new_accounts_created bigint,
  new_account_creation_attempts_on_chtr_network bigint,
  new_accounts_created_on_chtr_network bigint,
  new_account_creation_attempts_off_chtr_network bigint,
  new_accounts_created_off_chtr_network bigint,
  total_sub_user_creation_attempts bigint,
  total_new_sub_users_created bigint,
  sub_user_creation_attempts_on_chtr_network bigint,
  sub_users_created_on_chtr_network bigint,
  sub_user_creation_attempts_off_chtr_network bigint,
  sub_users_created_off_chtr_network bigint,
  total_username_recovery_attempts bigint,
  total_username_recovery_successes bigint,
  username_recovery_attempts_on_chtr_network bigint,
  username_recovery_successes_on_chtr_network bigint,
  username_recovery_attempts_off_chtr_network bigint,
  username_recovery_successes_off_chtr_network bigint,
  total_attempts_to_reset_password bigint,
  total_successful_password_resets bigint,
  attempts_to_reset_password_on_chtr_network bigint,
  successful_password_resets_on_chtr_network bigint,
  attempts_to_reset_password_off_chtr_network bigint,
  successful_password_resets_off_chtr_network bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_subscriber_counts_manual
(
  company String,
  year_month String,
  subscriber_counts bigint
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
(
  company String,
  year_month String,
  metric String,
  value bigint
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_bhn_accounts_manual
(
  company String,
  year_month String,
  accounts String,
  role String,
  site_id String,
  new_accounts String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_fed_ID_auths_monthly
(
year_month STRING,
source_app STRING,
total_login_attempts bigint,
total_login_successes bigint,
percent_login_success Decimal(15,4),
unique_users_logged_in bigint,
company STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_agg_totals_instances
(
  year_month string,
  hhs_logged_in int,
  total_hhs int,
  percent_hhs_logged_in Decimal(15,4),
  unique_hhs_mom_change Decimal(15,4),
  unique_hhs_3month_change Decimal(15,4),
  my_account_page_views bigint,
  total_login_attempts bigint,
  total_login_successes bigint,
  percent_login_success Decimal(15,4),
  rescheduled_service_appointments int,
  cancelled_service_appointments int,
  support_page_views bigint,
  online_statement_views bigint,
  one_time_payment_attempts bigint,
  one_time_payments bigint,
  percent_one_time_payment_success Decimal(15,4),
  auto_pay_setup_attempts bigint,
  auto_pay_setup_successes bigint,
  percent_auto_pay_success Decimal(15,4),
  total_account_creation_attempts bigint,
  total_new_accounts_created bigint,
  percent_total_account_creation_success Decimal(15,4),
  new_account_creation_attempts_on_chtr_network bigint,
  new_accounts_created_on_chtr_network bigint,
  percent_account_creation_success_on_chtr_network Decimal(15,4),
  new_account_creation_attempts_off_chtr_network bigint,
  new_accounts_created_off_chtr_network bigint,
  percent_account_creation_success_off_chtr_network Decimal(15,4),
  total_sub_user_creation_attempts bigint,
  total_new_sub_users_created bigint,
  percent_total_sub_user_creation_success Decimal(15,4),
  sub_user_creation_attempts_on_chtr_network bigint,
  sub_users_created_on_chtr_network bigint,
  percent_sub_user_creation_success_on_chtr_network Decimal(15,4),
  sub_user_creation_attempts_off_chtr_network bigint,
  sub_users_created_off_chtr_network bigint,
  percent_sub_user_creation_success_off_chtr_network Decimal(15,4),
  total_username_recovery_attempts bigint,
  total_username_recovery_successes bigint,
  percent_total_username_recovery_success Decimal(15,4),
  username_recovery_attempts_on_chtr_network bigint,
  username_recovery_successes_on_chtr_network bigint,
  percent_username_recovery_success_on_chtr_network Decimal(15,4),
  username_recovery_attempts_off_chtr_network bigint,
  username_recovery_successes_off_chtr_network bigint,
  percent_username_recovery_success_off_chtr_network Decimal(15,4),
  total_attempts_to_reset_password bigint,
  total_successful_password_resets bigint,
  percent_total_password_reset_success Decimal(15,4),
  attempts_to_reset_password_on_chtr_network bigint,
  successful_password_resets_on_chtr_network bigint,
  percent_password_reset_success_on_chtr_network Decimal(15,4),
  attempts_to_reset_password_off_chtr_network bigint,
  successful_password_resets_off_chtr_network bigint,
  percent_password_reset_success_off_chtr_network Decimal(15,4)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_agg_totals_visits
(
  year_month string,
  hhs_logged_in int,
  total_hhs int,
  percent_hhs_logged_in Decimal(15,4),
  unique_hhs_mom_change Decimal(15,4),
  unique_hhs_3month_change Decimal(15,4),
  my_account_page_views bigint,
  total_login_attempts bigint,
  total_login_successes bigint,
  percent_login_success Decimal(15,4),
  rescheduled_service_appointments int,
  cancelled_service_appointments int,
  support_page_views bigint,
  online_statement_views bigint,
  one_time_payment_attempts bigint,
  one_time_payments bigint,
  one_time_payments_with_autopay_setup bigint,
  percent_one_time_payment_success Decimal(15,4),
  auto_pay_setup_attempts bigint,
  auto_pay_setup_successes bigint,
  percent_auto_pay_success Decimal(15,4),
  total_account_creation_attempts bigint,
  total_new_accounts_created bigint,
  percent_total_account_creation_success Decimal(15,4),
  new_account_creation_attempts_on_chtr_network bigint,
  new_accounts_created_on_chtr_network bigint,
  percent_account_creation_success_on_chtr_network Decimal(15,4),
  new_account_creation_attempts_off_chtr_network bigint,
  new_accounts_created_off_chtr_network bigint,
  percent_account_creation_success_off_chtr_network Decimal(15,4),
  total_sub_user_creation_attempts bigint,
  total_new_sub_users_created bigint,
  percent_total_sub_user_creation_success Decimal(15,4),
  sub_user_creation_attempts_on_chtr_network bigint,
  sub_users_created_on_chtr_network bigint,
  percent_sub_user_creation_success_on_chtr_network Decimal(15,4),
  sub_user_creation_attempts_off_chtr_network bigint,
  sub_users_created_off_chtr_network bigint,
  percent_sub_user_creation_success_off_chtr_network Decimal(15,4),
  total_username_recovery_attempts bigint,
  total_username_recovery_successes bigint,
  percent_total_username_recovery_success Decimal(15,4),
  username_recovery_attempts_on_chtr_network bigint,
  username_recovery_successes_on_chtr_network bigint,
  percent_username_recovery_success_on_chtr_network Decimal(15,4),
  username_recovery_attempts_off_chtr_network bigint,
  username_recovery_successes_off_chtr_network bigint,
  percent_username_recovery_success_off_chtr_network Decimal(15,4),
  total_attempts_to_reset_password bigint,
  total_successful_password_resets bigint,
  percent_total_password_reset_success Decimal(15,4),
  attempts_to_reset_password_on_chtr_network bigint,
  successful_password_resets_on_chtr_network bigint,
  percent_password_reset_success_on_chtr_network Decimal(15,4),
  attempts_to_reset_password_off_chtr_network bigint,
  successful_password_resets_off_chtr_network bigint,
  percent_password_reset_success_off_chtr_network Decimal(15,4)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS sbnet_exec_monthly_agg_instances
(
  company String,
  hhs_logged_in int,
  total_hhs int,
  percent_hhs_logged_in Decimal(15,4),
  unique_hhs_mom_change Decimal(15,4),
  unique_hhs_3month_change Decimal(15,4),
  my_account_page_views bigint,
  total_login_attempts bigint,
  total_login_successes bigint,
  percent_login_success Decimal(15,4),
  rescheduled_service_appointments int,
  cancelled_service_appointments int,
  support_page_views bigint,
  online_statement_views bigint,
  one_time_payment_attempts bigint,
  one_time_payments bigint,
  percent_one_time_payment_success Decimal(15,4),
  auto_pay_setup_attempts bigint,
  auto_pay_setup_successes bigint,
  percent_auto_pay_success Decimal(15,4),
  total_account_creation_attempts bigint,
  total_new_accounts_created bigint,
  percent_total_account_creation_success Decimal(15,4),
  new_account_creation_attempts_on_chtr_network bigint,
  new_accounts_created_on_chtr_network bigint,
  percent_account_creation_success_on_chtr_network Decimal(15,4),
  new_account_creation_attempts_off_chtr_network bigint,
  new_accounts_created_off_chtr_network bigint,
  percent_account_creation_success_off_chtr_network Decimal(15,4),
  total_sub_user_creation_attempts bigint,
  total_new_sub_users_created bigint,
  percent_total_sub_user_creation_success Decimal(15,4),
  sub_user_creation_attempts_on_chtr_network bigint,
  sub_users_created_on_chtr_network bigint,
  percent_sub_user_creation_success_on_chtr_network Decimal(15,4),
  sub_user_creation_attempts_off_chtr_network bigint,
  sub_users_created_off_chtr_network bigint,
  percent_sub_user_creation_success_off_chtr_network Decimal(15,4),
  total_username_recovery_attempts bigint,
  total_username_recovery_successes bigint,
  percent_total_username_recovery_success Decimal(15,4),
  username_recovery_attempts_on_chtr_network bigint,
  username_recovery_successes_on_chtr_network bigint,
  percent_username_recovery_success_on_chtr_network Decimal(15,4),
  username_recovery_attempts_off_chtr_network bigint,
  username_recovery_successes_off_chtr_network bigint,
  percent_username_recovery_success_off_chtr_network Decimal(15,4),
  total_attempts_to_reset_password bigint,
  total_successful_password_resets bigint,
  percent_total_password_reset_success Decimal(15,4),
  attempts_to_reset_password_on_chtr_network bigint,
  successful_password_resets_on_chtr_network bigint,
  percent_password_reset_success_on_chtr_network Decimal(15,4),
  attempts_to_reset_password_off_chtr_network bigint,
  successful_password_resets_off_chtr_network bigint,
  percent_password_reset_success_off_chtr_network Decimal(15,4)
)
PARTITIONED BY (year_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS sbnet_exec_monthly_agg_visits
(
  company String,
  hhs_logged_in int,
  total_hhs int,
  percent_hhs_logged_in Decimal(15,4),
  unique_hhs_mom_change Decimal(15,4),
  unique_hhs_3month_change Decimal(15,4),
  my_account_page_views bigint,
  total_login_attempts bigint,
  total_login_successes bigint,
  percent_login_success Decimal(15,4),
  rescheduled_service_appointments int,
  cancelled_service_appointments int,
  support_page_views bigint,
  online_statement_views bigint,
  one_time_payment_attempts bigint,
  one_time_payments bigint,
  one_time_payments_with_autopay_setup bigint,
  percent_one_time_payment_success Decimal(15,4),
  auto_pay_setup_attempts bigint,
  auto_pay_setup_successes bigint,
  percent_auto_pay_success Decimal(15,4),
  total_account_creation_attempts bigint,
  total_new_accounts_created bigint,
  percent_total_account_creation_success Decimal(15,4),
  new_account_creation_attempts_on_chtr_network bigint,
  new_accounts_created_on_chtr_network bigint,
  percent_account_creation_success_on_chtr_network Decimal(15,4),
  new_account_creation_attempts_off_chtr_network bigint,
  new_accounts_created_off_chtr_network bigint,
  percent_account_creation_success_off_chtr_network Decimal(15,4),
  total_sub_user_creation_attempts bigint,
  total_new_sub_users_created bigint,
  percent_total_sub_user_creation_success Decimal(15,4),
  sub_user_creation_attempts_on_chtr_network bigint,
  sub_users_created_on_chtr_network bigint,
  percent_sub_user_creation_success_on_chtr_network Decimal(15,4),
  sub_user_creation_attempts_off_chtr_network bigint,
  sub_users_created_off_chtr_network bigint,
  percent_sub_user_creation_success_off_chtr_network Decimal(15,4),
  total_username_recovery_attempts bigint,
  total_username_recovery_successes bigint,
  percent_total_username_recovery_success Decimal(15,4),
  username_recovery_attempts_on_chtr_network bigint,
  username_recovery_successes_on_chtr_network bigint,
  percent_username_recovery_success_on_chtr_network Decimal(15,4),
  username_recovery_attempts_off_chtr_network bigint,
  username_recovery_successes_off_chtr_network bigint,
  percent_username_recovery_success_off_chtr_network Decimal(15,4),
  total_attempts_to_reset_password bigint,
  total_successful_password_resets bigint,
  percent_total_password_reset_success Decimal(15,4),
  attempts_to_reset_password_on_chtr_network bigint,
  successful_password_resets_on_chtr_network bigint,
  percent_password_reset_success_on_chtr_network Decimal(15,4),
  attempts_to_reset_password_off_chtr_network bigint,
  successful_password_resets_off_chtr_network bigint,
  percent_password_reset_success_off_chtr_network Decimal(15,4)
)
PARTITIONED BY (year_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_monthly_adjustment_init
(
company STRING,
year_month STRING,
metric STRING,
unit STRING,
value_type STRING,
value DECIMAL(15,4),
mom_perc_chg DECIMAL(15,4),
mom_diff DECIMAL(15,4),
prior_3_mo_perc_chg DECIMAL(15,4),
prior_3_mo_diff DECIMAL(15,4),
ytd_avg DECIMAL(15,4),
prev_months_max_year_month STRING,
prev_months_max_val DECIMAL(15,4),
prev_months_min_year_month STRING,
prev_months_min_val DECIMAL(15,4),
change_comment STRING,
tableau_field STRING,
jira_ticket STRING
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.sbnet_exec_tableau_metric_lkp
(
hive_metric STRING,
tableau_display STRING,
unit STRING,
value_type STRING
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='')
;

CREATE TABLE IF NOT EXISTS sbnet_exec_monthly_pivot
(
unit string,
value_type string,
value decimal(15,4),
mom_perc_chg decimal(15,4),
mom_diff decimal(15,4),
prior_3_mo_perc_chg decimal(15,4),
prior_3_mo_diff decimal(15,4),
ytd_avg decimal(15,4),
prev_months_max_year_month string,
prev_months_max_val decimal(15,4),
prev_months_min_year_month string,
prev_months_min_val decimal(15,4),
change_comment string)
PARTITIONED BY (
company string,
year_month string,
metric string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE IF NOT EXISTS sbnet_exec_monthly_tableau
(
unit STRING,
value_type STRING,
value DECIMAL(15,4),
mom_perc_chg DECIMAL(15,4),
mom_diff DECIMAL(15,4),
prior_3_mo_perc_chg DECIMAL(15,4),
prior_3_mo_diff DECIMAL(15,4),
ytd_avg DECIMAL(15,4),
prev_months_max_year_month STRING,
prev_months_max_val DECIMAL(15,4),
prev_months_min_year_month STRING,
prev_months_min_val DECIMAL(15,4),
change_comment STRING,
tableau_field STRING,
jira_ticket STRING
)
PARTITIONED BY (
company STRING,
year_month STRING,
metric STRING)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE IF NOT EXISTS sbnet_exec_monthly_adjustment
(
unit STRING,
value_type STRING,
value DECIMAL(15,4),
mom_perc_chg DECIMAL(15,4),
mom_diff DECIMAL(15,4),
prior_3_mo_perc_chg DECIMAL(15,4),
prior_3_mo_diff DECIMAL(15,4),
ytd_avg DECIMAL(15,4),
prev_months_max_year_month STRING,
prev_months_max_val DECIMAL(15,4),
prev_months_min_year_month STRING,
prev_months_min_val DECIMAL(15,4),
change_comment STRING,
tableau_field STRING,
jira_ticket STRING
)
PARTITIONED BY (
company STRING,
year_month STRING,
metric STRING)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;
