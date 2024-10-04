SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

--Takes all hive table data and pivots into tableau feeder table format

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT}
;

-------------------------------------------------------------------------------

INSERT OVERWRITE TABLE sbnet_exec_monthly_pivot
PARTITION (company,year_month,metric)

SELECT
unit AS unit,
value_type AS value_type,
metric_value AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'hive_table' AS change_comment,
company AS company,
year_month AS year_month,
metric_name AS metric
FROM
    (SELECT
    company,
    year_month,
    NULL AS unit,
    NULL AS value_type,
    MAP
        (
        'company',company,
        'hhs_logged_in',hhs_logged_in,
        'total_hhs',total_hhs,
        'percent_hhs_logged_in',percent_hhs_logged_in,
        'unique_hhs_mom_change',unique_hhs_mom_change,
        'unique_hhs_3month_change',unique_hhs_3month_change,
        'my_account_page_views',my_account_page_views,
        'total_login_attempts',total_login_attempts,
        'total_login_successes',total_login_successes,
        'percent_login_success',percent_login_success,
        'rescheduled_service_appointments',rescheduled_service_appointments,
        'cancelled_service_appointments',cancelled_service_appointments,
        'support_page_views',support_page_views,
        'online_statement_views',online_statement_views,
        'one_time_payment_attempts',one_time_payment_attempts,
        'one_time_payments',one_time_payments,
        'one_time_payments_with_autopay_setup', one_time_payments_with_autopay_setup,
        'percent_one_time_payment_success',percent_one_time_payment_success,
        'auto_pay_setup_attempts',auto_pay_setup_attempts,
        'auto_pay_setup_successes',auto_pay_setup_successes,
        'percent_auto_pay_success',percent_auto_pay_success,
        'total_account_creation_attempts',total_account_creation_attempts,
        'total_new_accounts_created',total_new_accounts_created,
        'percent_total_account_creation_success',percent_total_account_creation_success,
        'new_account_creation_attempts_on_chtr_network',new_account_creation_attempts_on_chtr_network,
        'new_accounts_created_on_chtr_network',new_accounts_created_on_chtr_network,
        'percent_account_creation_success_on_chtr_network',percent_account_creation_success_on_chtr_network,
        'new_account_creation_attempts_off_chtr_network',new_account_creation_attempts_off_chtr_network,
        'new_accounts_created_off_chtr_network',new_accounts_created_off_chtr_network,
        'percent_account_creation_success_off_chtr_network',percent_account_creation_success_off_chtr_network,
        'total_sub_user_creation_attempts',total_sub_user_creation_attempts,
        'total_new_sub_users_created',total_new_sub_users_created,
        'percent_total_sub_user_creation_success',percent_total_sub_user_creation_success,
        'sub_user_creation_attempts_on_chtr_network',sub_user_creation_attempts_on_chtr_network,
        'sub_users_created_on_chtr_network',sub_users_created_on_chtr_network,
        'percent_sub_user_creation_success_on_chtr_network',percent_sub_user_creation_success_on_chtr_network,
        'sub_user_creation_attempts_off_chtr_network',sub_user_creation_attempts_off_chtr_network,
        'sub_users_created_off_chtr_network',sub_users_created_off_chtr_network,
        'percent_sub_user_creation_success_off_chtr_network',percent_sub_user_creation_success_off_chtr_network,
        'total_username_recovery_attempts',total_username_recovery_attempts,
        'total_username_recovery_successes',total_username_recovery_successes,
        'percent_total_username_recovery_success',percent_total_username_recovery_success,
        'username_recovery_attempts_on_chtr_network',username_recovery_attempts_on_chtr_network,
        'username_recovery_successes_on_chtr_network',username_recovery_successes_on_chtr_network,
        'percent_username_recovery_success_on_chtr_network',percent_username_recovery_success_on_chtr_network,
        'username_recovery_attempts_off_chtr_network',username_recovery_attempts_off_chtr_network,
        'username_recovery_successes_off_chtr_network',username_recovery_successes_off_chtr_network,
        'percent_username_recovery_success_off_chtr_network',percent_username_recovery_success_off_chtr_network,
        'total_attempts_to_reset_password',total_attempts_to_reset_password,
        'total_successful_password_resets',total_successful_password_resets,
        'percent_total_password_reset_success',percent_total_password_reset_success,
        'attempts_to_reset_password_on_chtr_network',attempts_to_reset_password_on_chtr_network,
        'successful_password_resets_on_chtr_network',successful_password_resets_on_chtr_network,
        'percent_password_reset_success_on_chtr_network',percent_password_reset_success_on_chtr_network,
        'attempts_to_reset_password_off_chtr_network',attempts_to_reset_password_off_chtr_network,
        'successful_password_resets_off_chtr_network',successful_password_resets_off_chtr_network,
        'percent_password_reset_success_off_chtr_network',percent_password_reset_success_off_chtr_network,
        'year_month',year_month
        ) met_values
    FROM sbnet_exec_monthly_agg_visits
    WHERE year_month = '${env:YEAR_MONTH}'
    AND company <> 'Total Combined'
    ) mv
LATERAL VIEW EXPLODE(met_values) xyz AS metric_name,metric_value
;

SELECT "*****-- End SBNET Exec Monthly Pivot Table --*****"
;
