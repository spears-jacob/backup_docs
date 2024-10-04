USE ${env:ENVIRONMENT};

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
;
