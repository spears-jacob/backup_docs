-------------------------------------------------------------------------------

--Bring data from manual files into tableau feeder table format
--Brings in only currently-used data points

-------------------------------------------------------------------------------


USE ${env:ENVIRONMENT};

DROP TABLE IF EXISTS ${env:TMP_db}.net_products_agg_monthly_adj_manual PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_products_agg_monthly_adj_manual
(
unit STRING,
value_type STRING,
value DECIMAL(15,4),
mom_perc_Chg DECIMAL(15,4),
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
PARTITIONED BY 
(
company STRING,
year_month STRING,
metric STRING
)
;


-------------------------------------------------------------------------------
-- Begin L-CHTR Metric Calculations --

--Metric: total_hhs
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
subscriber_counts AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'total_hhs' AS metric
FROM
${env:TMP_db}.net_monthly_subscriber_counts_manual 
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'total_hhs' = dis.hive_metric
WHERE 
year_month = '${env:YEAR_MONTH}'
AND 
company = 'L-CHTR'
;

SELECT '*****-- END L-CHTR Metric Calculations --*****'
;

-- END L-CHTR Metric Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin BHN Metric Calculations --

--Metric: new_ids_charter_count
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
MAX(IF(acc.site_id='003' and acc.role='Administrator',CAST(REGEXP_REPLACE(REGEXP_REPLACE(acc.new_accounts,'"',''),',','') as bigint),NULL)) AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'new_ids_charter_count_all' AS metric
FROM
${env:TMP_db}.net_monthly_bhn_accounts_manual acc
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'new_ids_charter_count_all' = dis.hive_metric
WHERE 
acc.year_month = '${env:YEAR_MONTH}'
AND acc.company = 'L-BHN'
GROUP BY
company,
dis.tableau_display,
dis.unit,
dis.value_type
;

SELECT '*****-- END BHN new_ids_charter_count Metric --*****'
;

--Metric: attempts_recover_username_count_all
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
MAX(IF(sso.metric = 'username_recovery_attempts',sso.value,NULL)) AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'attempts_recover_username_count_all' AS metric
FROM
${env:TMP_db}.net_monthly_bhn_sso_metrics_manual sso
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'attempts_recover_username_count_all' = dis.hive_metric
WHERE 
sso.year_month = '${env:YEAR_MONTH}'
AND sso.company = 'L-BHN'
GROUP BY
company,
dis.tableau_display,
dis.unit,
dis.value_type
;

SELECT '*****-- END BHN attempts_recover_username_count_all Metric --*****'
;

--Metric: attempts_reset_password_count_all
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
MAX(IF(sso.metric = 'password_reset_attempts',sso.value,NULL)) AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'attempts_reset_password_count_all' AS metric
FROM
${env:TMP_db}.net_monthly_bhn_sso_metrics_manual sso
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'attempts_reset_password_count_all' = dis.hive_metric
WHERE 
sso.year_month = '${env:YEAR_MONTH}'
AND sso.company = 'L-BHN'
GROUP BY
company,
dis.tableau_display,
dis.unit,
dis.value_type
;

SELECT '*****-- END BHN attempts_reset_password_count_all Metric --*****'
;

--Metric: successful_reset_password_count_all
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
MAX(IF(sso.metric = 'password_reset_successes',sso.value,NULL)) AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'successful_reset_password_count_all' AS metric
FROM
${env:TMP_db}.net_monthly_bhn_sso_metrics_manual sso
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'successful_reset_password_count_all' = dis.hive_metric
WHERE 
sso.year_month = '${env:YEAR_MONTH}'
AND sso.company = 'L-BHN'
GROUP BY
company,
dis.tableau_display,
dis.unit,
dis.value_type
;

SELECT '*****-- END BHN successful_reset_password_count_all Metric --*****'
;

--Metric: total_hhs
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
subscriber_counts AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'total_hhs' AS metric
FROM
${env:TMP_db}.net_monthly_subscriber_counts_manual 
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'total_hhs' = dis.hive_metric
WHERE 
year_month = '${env:YEAR_MONTH}'
AND 
company = 'L-BHN'
;

SELECT '*****-- END BHN Metric Calculations --*****'
;

-- END BHN Metric Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin TWC Metric Calculations --

--Metric: total_hhs
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual 
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
subscriber_counts AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'manual_file' AS change_comment,
dis.tableau_display AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'total_hhs' AS metric
FROM
${env:TMP_db}.net_monthly_subscriber_counts_manual 
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'total_hhs' = dis.hive_metric
WHERE 
year_month = '${env:YEAR_MONTH}'
AND 
company = 'L-TWC'
;

SELECT '*****-- END TWC Metric Calculations --*****'
;

