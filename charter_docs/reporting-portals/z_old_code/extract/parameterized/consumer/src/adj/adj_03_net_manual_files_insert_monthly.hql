-------------------------------------------------------------------------------

--Bring data from manual files into tableau feeder table format
--Brings in only currently-used data points

-------------------------------------------------------------------------------
USE ${env:ENVIRONMENT};
SELECT '\n\n### Running net_manual_files_insert_monthly job for \n\n';


DROP TABLE IF EXISTS ${env:TMP_db}.net_products_agg_monthly_adj_manual PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_products_agg_monthly_adj_manual
  (unit STRING,
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

SELECT  dis.unit AS unit,
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
        year_month,
        'total_hhs' AS metric
FROM ${env:TMP_db}.net_monthly_subscriber_counts_manual m
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'total_hhs' = dis.hive_metric
;

SELECT '*****-- END L-CHTR Metric Calculations --*****'
;

-- END L-CHTR Metric Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin BHN Metric Calculations --
-- distinct values for site_id appear to be 3 and 5

--Metric: new_ids_charter_count
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual
PARTITION (company,year_month,metric)

SELECT  dis.unit AS unit,
        dis.value_type AS value_type,
        MAX(IF(acc.site_id IN('3','003') and acc.role='Administrator',CAST(REGEXP_REPLACE(REGEXP_REPLACE(acc.new_accounts,'"',''),',','') as bigint),NULL)) AS value,
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
        year_month,
        'new_ids_charter_count_all' AS metric
FROM    ${env:TMP_db}.net_monthly_bhn_accounts_manual acc
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
      ON 'new_ids_charter_count_all' = dis.hive_metric
WHERE acc.company = 'L-BHN'
      AND "${env:CADENCE}" <> 'daily'
GROUP BY  company,
          year_month,
          dis.tableau_display,
          dis.unit,
          dis.value_type
;

SELECT '*****-- END BHN new_ids_charter_count Metric --*****'
;

--Metric: attempts_recover_username_count_all
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual
PARTITION (company,year_month,metric)

SELECT  dis.unit AS unit,
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
        year_month,
        'attempts_recover_username_count_all' AS metric
FROM ${env:TMP_db}.net_monthly_bhn_sso_metrics_manual sso
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'attempts_recover_username_count_all' = dis.hive_metric
WHERE sso.company = 'L-BHN'
      AND "${env:CADENCE}" <> 'daily'
GROUP BY  company,
          year_month,
          dis.tableau_display,
          dis.unit,
          dis.value_type
;

SELECT '*****-- END BHN attempts_recover_username_count_all Metric --*****'
;

--Metric: attempts_reset_password_count_all
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual
PARTITION (company,year_month,metric)

SELECT  dis.unit AS unit,
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
        year_month,
        'attempts_reset_password_count_all' AS metric
FROM    ${env:TMP_db}.net_monthly_bhn_sso_metrics_manual sso
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'attempts_reset_password_count_all' = dis.hive_metric
WHERE sso.company = 'L-BHN'
  AND "${env:CADENCE}" <> 'daily'
GROUP BY  company,
          year_month,
          dis.tableau_display,
          dis.unit,
          dis.value_type
;

SELECT '*****-- END BHN attempts_reset_password_count_all Metric --*****'
;

--Metric: successful_reset_password_count_all
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual
PARTITION (company,year_month,metric)

SELECT  'visits' AS unit,
        'dec' AS value_type,
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
        year_month,
        'successful_reset_password_count_all' AS metric
FROM    ${env:TMP_db}.net_monthly_bhn_sso_metrics_manual sso
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'successful_reset_password_count_all' = dis.hive_metric
WHERE sso.company = 'L-BHN'
  AND "${env:CADENCE}" <> 'daily'
GROUP BY  company,
          year_month,
          dis.tableau_display,
          dis.unit,
          dis.value_type
;

SELECT '*****-- END BHN successful_reset_password_count_all Metric --*****'
;

--Metric: total_hhs
INSERT OVERWRITE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_manual
PARTITION (company,year_month,metric)

SELECT  unit,
        value_type,
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
        tableau_display AS tableau_field,
        NULL AS jira_ticket,
        company AS company,
        year_month,
        'total_hhs' AS metric
FROM    ${env:TMP_db}.net_monthly_subscriber_counts_manual m
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'total_hhs' = hive_metric
WHERE   company = 'L-BHN'
    AND "${env:CADENCE}" <> 'daily'
GROUP BY  year_month,
          company,
          unit,
          value_type,
          tableau_display,
          subscriber_counts
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

SELECT  'subscriber' AS unit,
        'dec' AS value_type,
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
        year_month,
        'total_hhs' AS metric
FROM    ${env:TMP_db}.net_monthly_subscriber_counts_manual
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON 'total_hhs' = dis.hive_metric
WHERE company = 'L-TWC'
  AND "${env:CADENCE}" <> 'daily'
GROUP BY  company,
          year_month,
          dis.tableau_display,
          subscriber_counts
;

SELECT '*****-- END TWC Metric Calculations --*****';


SELECT '\n\n*****-- Now Inserting Manual File entries into asp_${env:CADENCE}_agg_raw  --*****\n\n';
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT  value,
        unit,
        'resi' as domain,
        company,
        year_month as ${env:pf},
        metric
from ${env:TMP_db}.net_products_agg_monthly_adj_manual
WHERE "${env:CADENCE}" <> 'daily'
AND change_comment = 'manual_file';


SELECT '\n\n*****-- Finished Inserting Manual File entries into asp_${env:CADENCE}_agg_raw  --*****\n\n';


SELECT '\n\n### Completed the net_manual_files_insert_monthly job...." \n\n';
