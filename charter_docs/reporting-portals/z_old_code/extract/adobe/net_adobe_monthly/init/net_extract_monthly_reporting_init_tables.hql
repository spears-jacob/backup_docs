USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_extract_monthly_run_date(
run_date STRING
) COMMENT 'Monthly run date for old jobs'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_hhs_sections_monthly
(
year_month STRING COMMENT 'run month and year constant for all metrics as the reference year-month',
message_category STRING COMMENT 'message category',
hhs_month INT COMMENT 'Unique Account Numbers per month',
page_section STRING COMMENT 'Unique page sections per month',
company STRING
) COMMENT 'Monthly aggegate providing counts of household visits boken down by page and message'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_return_frequency_monthly(
year_month STRING,
zip_code STRING,
unique_visitor INT,
company STRING 
) COMMENT 'Montly aggregate of unique visitors by zip code'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_visit_metrics_monthly(
year_month STRING,
metric STRING,
visits_count INT,
company STRING
) COMMENT 'Monthly aggregate providing counts of account creation'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_hh_total_monthly(
year_month STRING, 
unique_visits INT, 
unique_visitors INT, 
unique_visitors_logged INT, 
unique_email_visitors INT, 
unique_hh INT,
company STRING
) COMMENT 'Monthly aggregate providing counts of visitors'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_hh_account_monthly (
year_month STRING,
total_unique_hhs BIGINT,
account_number_list ARRAY<STRING>
) COMMENT 'Monthly aggregate providing counts of household visits'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_zip_metrics_monthly(
year_month STRING,
zip_code string COMMENT 'location zip code of the session', 
dma string COMMENT 'DMA', 
location string COMMENT 'location region of the session', 
unique_hh INT COMMENT 'the number of households for the partition_date', 
unique_visits INT COMMENT 'the number of visits for the partition_date', 
unique_visitors INT COMMENT 'the number of visitors for the partition_date', 
unique_logged_in_visitors INT COMMENT 'the number of looged in visitors for the partition_date', 
total_page_views INT COMMENT 'the number of page views for the partition_date'
) COMMENT 'Monthly aggregate providing counts of household visitors and page views grouped by location details'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_visits_agg_monthly (
year_month STRING,
visits BIGINT,
visitors BIGINT,
customer__user_ids BIGINT, 
customer__households BIGINT, 
customer__test_accounts BIGINT,
customer__network_status ARRAY<STRING>, 
network_type ARRAY<STRING>,
customer__subscriptionTypes ARRAY<STRING>, 
device__os ARRAY<STRING>, 
location__country_name ARRAY<STRING>, 
location__region ARRAY<STRING>, 
location__city ARRAY<STRING>, 
location__state ARRAY<STRING>, 
location__zip_code ARRAY<STRING>, 
total_page_views_by_section ARRAY<STRING>,
first_time_users BIGINT,
total_conversions_by_type BIGINT,
total_login_failures BIGINT
) COMMENT 'Monthly aggregate table providing visits information including network and user details'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_views_agg_monthly (
year_month STRING,
visits BIGINT,
views BIGINT,
visitors BIGINT, 
customer__user_ids BIGINT,
customer__households BIGINT, 
customer__onnet MAP<STRING, INT>,
customer__subscriptiontype MAP<STRING, INT>, 
location__country_name MAP<STRING, INT>, 
location__region MAP<STRING, INT>, 
location__city MAP<STRING, INT>, 
location__state MAP<STRING, INT>, 
location__zip_code MAP<STRING, INT>, 
program__type MAP<STRING, INT>,
program__title MAP<STRING, INT>,
program__genres MAP<STRING, INT>,
programmer__linear__network_name MAP<STRING, INT>,
programmer__linear__channel_category MAP<STRING, INT>,
programmer__linear__channel_number MAP<STRING, INT>,
network_type MAP<STRING, INT>,
total_stream_failures BIGINT,
total_views_last_month BIGINT
) COMMENT 'Monthly aggregate table providing view information including account and location details'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_auth_flow_page_visits_monthly (
auth_flow STRING,
page_name STRING,
count_hhs BIGINT,
count_user_names BIGINT,
count_visits BIGINT,
count_visitors BIGINT,
count_page_views BIGINT
) COMMENT 'Monthly aggregate table providing autentication flow page information for recover, reset and create user'
PARTITIONED BY (year_month STRING) 
row format delimited fields terminated by '\t'
lines terminated by '\n'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_network_status_monthly( 
year_month STRING,
btm_visits INT,
on_plant_visits INT,
off_plant_visits INT,
undetermined_visits INT,
other_visits int
) COMMENT 'Monthly aggregate table providing visits counts based on connection network status groups'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_message_names_monthly(
year_month STRING,
message__name STRING,
message__category STRING,
state__view__current_page__name STRING,
state__view__current_page__section STRING,
operation__user_entry__type STRING,
state__content__stream__type STRING,
state__content__stream__content_format STRING,
visit__connection__network_status STRING,
visit__device__operating_system STRING,
visit__connection__type STRING,
visit__device__browser__name STRING,
visit INT,
`count` int
) COMMENT 'Monthly aggregate providing counts of visits and total grouped by message, state and visit characteristics'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_reset_password_monthly(
run_year STRING,
run_month STRING,
step STRING,
count_hhs INT,
count_user_names INT,
count_visits INT,
count_visitors INT,
count_page_views int
) COMMENT 'Monthly aggregate providing counts of reset account users, visitors and visits grouped by step'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_reset_user_monthly(
run_year STRING,
run_month STRING,
step STRING,
count_hhs INT,
count_user_names INT,
count_visits INT,
count_visitors INT,
count_page_views int
) COMMENT 'Monthly aggregate providing counts of user recovery, visitors and visits grouped by step'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_hhs_pvs_links_monthly(
year_month STRING COMMENT 'run month and year constant for all metrics as the reference year-month', 
hhs_month INT COMMENT 'Unique Account Numbers per month', 
message_name STRING COMMENT 'Unique message names per month', 
message_category STRING COMMENT 'message category', 
page_name STRING COMMENT 'Unique page names per month', 
page_section STRING COMMENT 'Unique page sections per month', 
operation_type STRING COMMENT 'Unique operation types per month',
company STRING 
) COMMENT 'Monthly aggegate providing counts of household visits boken down by page, message and operation'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_create_username_monthly(
run_year STRING,
run_month STRING,
step STRING,
count_hhs INT,
count_user_names INT,
count_visits INT,
count_visitors INT,
count_page_views int
)
COMMENT 'Montly aggregate providing counts of account creation page views separated by step (1-10)'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS orc TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS net_products_agg_monthly
(
hhs_logged_in BIGINT, 
total_hhs BIGINT, 
percent_hhs_logged_in DECIMAL(15,4), 
unique_hhs_mom_change DECIMAL(15,4), 
unique_hhs_3month_change DECIMAL(15,4), 
total_login_attempts BIGINT, 
total_login_successes BIGINT, 
percent_login_success DECIMAL(15,4), 
total_page_views_count INT, 
webmail_page_views_count INT, 
my_account_page_views_count INT, 
support_page_views_count INT, 
view_online_statement_count INT, 
ask_charter_requests_count INT, 
refresh_requests_count INT, 
video_page_views_count INT, 
video_plays_count INT, 
new_ids_charter_count INT, 
new_ids_charter_count_bam INT, 
new_ids_charter_count_btm INT, 
new_ids_charter_count_nbtm INT, 
attempts_create_id_count_bam INT, 
attempts_create_id_count_btm INT, 
attempts_create_id_count_nbtm INT, 
new_ids_not_charter_count INT, 
total_attempts_id_off_count INT, 
attempts_recover_username_btm_count INT, 
succesfull_username_recovery_btm_count INT, 
attempts_recover_username_bam_count INT, 
succesfull_username_recovery_bam_count INT, 
attempts_recover_username_nbtm_count INT, 
succesfull_username_recovery_nbtm_count INT, 
attempts_reset_password_btm_count INT, 
successful_reset_password_btm_count INT, 
attempts_reset_password_bam_count INT, 
successful_reset_password_bam_count INT, 
attempts_reset_password_nbtm_count INT, 
successful_reset_password_nbtm_count INT, 
attempts_recover_username_password_btm_count INT, 
successfully_recover_username_password_btm_count INT, 
attempts_recover_username_password_bam_count INT, 
successfully_recover_username_password_bam_count INT, 
attempts_recover_username_password_nbtm_count INT, 
successfully_recover_username_password_nbtm_count INT, 
one_time_payment_count INT, 
one_time_payments_confirm_count INT, 
setup_autopay_count INT, 
successful_autopay_confirm_count INT, 
saved_bill_notifications_count INT, 
saved_appoint_reminders_count INT, 
rescheduled_service_appoint_count INT, 
cancelled_service_appoint_count INT, 
saved_service_alerts_count INT, 
saved_contact_information_details_count INT, 
attempts_create_id_count INT, 
attempts_reset_password_count INT, 
successful_reset_password_count INT, 
new_ids_charter_count_all INT, 
new_ids_charter_count_on_net INT, 
new_ids_charter_count_off_net INT, 
attempts_create_id_count_all INT, 
attempts_create_id_count_on_net INT, 
attempts_create_id_count_off_net INT, 
attempts_recover_username_count_all INT, 
attempts_recover_username_count_on_net INT, 
attempts_recover_username_count_off_net INT, 
successful_username_recovery_count_all INT, 
successful_username_recovery_count_on_net INT, 
successful_username_recovery_count_off_net INT, 
attempts_reset_password_count_all INT, 
attempts_reset_password_count_on_net INT, 
attempts_reset_password_count_off_net INT, 
successful_reset_password_count_all INT, 
successful_reset_password_count_on_net INT, 
successful_reset_password_count_off_net INT, 
attempts_recover_username_password_count_all INT, 
attempts_recover_username_password_count_on_net INT, 
attempts_recover_username_password_count_off_net INT, 
successfully_recover_username_password_count_all INT, 
successfully_recover_username_password_count_on_net INT, 
successfully_recover_username_password_count_off_net INT, 
company STRING, 
modem_router_resets INT, 
percent_auto_pay_success DECIMAL(15,4), 
percent_id_recovery_success_off_chtr_network DECIMAL(15,4), 
percent_id_recovery_success_on_chtr_network DECIMAL(15,4), 
percent_new_ids_charter_count_on_net DECIMAL(15,4), 
percent_one_time_payment_success DECIMAL(15,4), 
percent_password_reset_success_off_chtr_network DECIMAL(15,4), 
percent_success_new_ids_charter_off_net DECIMAL(15,4), 
percent_success_recover_reset_username_password_off_net DECIMAL(15,4), 
percent_success_recover_reset_username_password_on_net DECIMAL(15,4), 
percent_success_reset_password_on_net DECIMAL(15,4), 
percent_total_create_id_count_all DECIMAL(15,4), 
percent_total_password_reset_success DECIMAL(15,4), 
percent_total_success_recover_reset_username_password DECIMAL(15,4), 
percent_total_username_recovery_success DECIMAL(15,4)
)
COMMENT 'Monthly aggregation providing events and visits counts grouped by NET products'
PARTITIONED BY (year_month STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS orc TBLPROPERTIES('serialization.null.format'='')
;

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
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
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
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
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
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
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
STORED AS orc TBLPROPERTIES('serialization.null.format'='')
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
STORED AS orc TBLPROPERTIES('serialization.null.format'='')
;

CREATE TABLE IF NOT EXISTS net_preferred_comm
(
report_date STRING , 
contact_info_updated BIGINT, 
preferences_set BIGINT, 
enrolled_paperlessbilling BIGINT)
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
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
