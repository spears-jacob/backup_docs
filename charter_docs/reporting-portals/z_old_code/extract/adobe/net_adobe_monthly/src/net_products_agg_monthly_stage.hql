-------------------------------------------------------------------------------

--Drops and rebuilds temp tables for net monthly agg staging

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop and rebuild net_fed_ID_monthly temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.net_fed_ID_monthly PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_fed_ID_monthly
(
year_month STRING,
source_app STRING,
total_login_attempts bigint,
total_login_successes bigint,
percent_login_success Decimal(15,4),
hhs_logged_in bigint,
company STRING
)
;

SELECT '*****-- END - Drop and rebuild net_fed_ID_monthly temp table --*****'
;

-- END - Drop and rebuild net_fed_ID_monthly temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop and rebuild net_webmail_pv_count temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.net_webmail_pv_count PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_webmail_pv_count
(
webmail_page_views_count BIGINT,
company STRING,
year_month STRING
)
;

SELECT '*****-- END - Drop and rebuild net_webmail_pv_count temp table --*****'
;

-- END - Drop and rebuild net_webmail_pv_count temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop and rebuild net_bill_pay_analytics_monthly temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.net_bill_pay_analytics_monthly PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_bill_pay_analytics_monthly
(
step STRING ,
count_hhs INT,
count_user_names INT,
count_visits INT,
count_visitors INT,
count_page_views INT,
company STRING,
year_month STRING
) COMMENT 'Monthly aggregate providing counts of bill pay analytics separated by steps'
stored AS orc TBLPROPERTIES('serialization.null.format'='')
;

SELECT '*****-- END - Drop and rebuild net_bill_pay_analytics_monthly temp table --*****'
;

-- END - Drop and rebuild net_bill_pay_analytics_monthly temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop and rebuild new_id_counts_all temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.new_id_counts_all PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.new_id_counts_all
(
new_ids_charter_count_all INT,
year_month STRING,
company STRING
)
;

SELECT '*****-- END - Drop and rebuild new_id_counts_all temp table --*****'
;

-- END - Drop and rebuild new_id_counts_all temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop and rebuild net_products_agg_monthly_stage temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.net_products_agg_monthly_stage PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_products_agg_monthly_stage

(
hhs_logged_in bigint,
total_hhs bigint,
percent_hhs_logged_in Decimal(15,4),
unique_hhs_mom_change Decimal(15,4),
unique_hhs_3month_change Decimal(15,4),
--
total_page_views_count INT,
--
my_account_page_views_count INT,
support_page_views_count INT,
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
--
saved_bill_notifications_count INT,
saved_appoint_reminders_count INT,
rescheduled_service_appoint_count INT,
cancelled_service_appoint_count INT,
saved_service_alerts_count INT,
saved_contact_information_details_count INT,
attempts_create_id_count INT,
attempts_reset_password_count INT,
successful_reset_password_count INT,
--
--new_ids_charter_count_all INT,
new_ids_charter_count_on_net INT,
new_ids_charter_count_off_net INT,
--
attempts_create_id_count_all INT,
attempts_create_id_count_on_net INT,
attempts_create_id_count_off_net INT,
--
attempts_recover_username_count_all INT,
attempts_recover_username_count_on_net INT,
attempts_recover_username_count_off_net INT,
--
successful_username_recovery_count_all INT,
successful_username_recovery_count_on_net INT,
successful_username_recovery_count_off_net INT,
--
attempts_reset_password_count_all INT,
attempts_reset_password_count_on_net INT,
attempts_reset_password_count_off_net INT,
--
successful_reset_password_count_all INT,
successful_reset_password_count_on_net INT,
successful_reset_password_count_off_net INT,
--
attempts_recover_username_password_count_all INT,
attempts_recover_username_password_count_on_net INT,
attempts_recover_username_password_count_off_net INT,
--
successfully_recover_username_password_count_all INT,
successfully_recover_username_password_count_on_net INT,
successfully_recover_username_password_count_off_net INT,
--
company STRING,
year_month STRING,
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
stored AS orc TBLPROPERTIES('serialization.null.format'='')
;

SELECT '*****-- END - Drop and rebuild net_products_agg_monthly_stage temp table --*****'
;

-- Drop and rebuild net_products_agg_monthly_stage temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

