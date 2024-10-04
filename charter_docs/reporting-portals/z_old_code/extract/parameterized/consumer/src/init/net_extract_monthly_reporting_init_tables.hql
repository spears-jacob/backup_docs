USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS net_products_agg_monthly_adjustment
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

CREATE TABLE IF NOT EXISTS net_products_agg_monthly_tableau
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
jira_ticket STRING)
PARTITIONED BY (
company STRING,
year_month STRING,
metric STRING)
;

CREATE TABLE IF NOT EXISTS net_products_agg_monthly_pivot
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
;

CREATE TABLE IF NOT EXISTS net_bill_pay_analytics_monthly(
step STRING ,
count_hhs INT,
count_user_names INT,
count_visits INT,
count_visitors INT,
count_page_views INT,
company STRING
) COMMENT 'Monthly aggregate providing counts of bill pay analytics separated by steps'
PARTITIONED BY (year_month STRING)
;

CREATE TABLE IF NOT EXISTS net_bill_pay_analytics_STVA_monthly(
step STRING,
count_hhs INT,
count_user_names INT,
count_visits INT,
count_visitors INT,
count_page_views int
) COMMENT 'Monthly aggregate providing counts of bill pay analytics separated by steps'
PARTITIONED BY (year_month STRING)
;


CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_monthly_subscriber_counts_manual
(
company String,
year_month String,
subscriber_counts bigint
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_monthly_bhn_sso_metrics_manual
(
company String,
year_month String,
metric String,
value bigint
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_monthly_bhn_accounts_manual
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
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1")
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_products_agg_monthly_adjustment_init
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

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_products_monthly_tableau_metric_lkp
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

CREATE TABLE IF NOT EXISTS bounce_rate_hist
(
bounces INT,
entries INT,
page_name STRING
)
PARTITIONED BY
(company STRING,partition_date STRING)
;
