-------------------------------------------------------------------------------

--Combines manual adjustment data and manual files data into one table
--Ensures that manual adjustment data "overrides" anything else with same partition

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined Manual + Adjustment Metrics --

DROP TABLE IF EXISTS ${env:TMP_db}.net_products_agg_monthly_adj_stage PURGE
;

CREATE TABLE ${env:TMP_db}.net_products_agg_monthly_adj_stage AS

SELECT
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.unit ELSE man.unit END AS unit,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.value_type ELSE man.value_type END AS value_type,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.value ELSE man.value END AS value,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.mom_perc_chg ELSE man.mom_perc_chg END AS mom_perc_chg,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.mom_diff ELSE man.mom_diff END AS mom_diff,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prior_3_mo_perc_chg ELSE man.prior_3_mo_perc_chg END AS prior_3_mo_perc_chg,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prior_3_mo_diff ELSE man.prior_3_mo_diff END AS prior_3_mo_diff,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.ytd_avg ELSE man.ytd_avg END AS ytd_avg,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_max_year_month ELSE man.prev_months_max_year_month END AS prev_months_max_year_month,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_max_val ELSE man.prev_months_max_val END AS prev_months_max_val,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_min_year_month ELSE man.prev_months_min_year_month END AS prev_months_min_year_month,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_min_val ELSE man.prev_months_min_val END AS prev_months_min_val,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.change_comment ELSE man.change_comment END AS change_comment,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.jira_ticket ELSE man.jira_ticket END AS jira_ticket,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.company ELSE man.company END AS company,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.year_month ELSE man.year_month END AS year_month,
CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.metric ELSE man.metric END AS metric
FROM
${env:TMP_db}.net_products_agg_monthly_adjustment_init iadj
LEFT JOIN 
    ${env:TMP_db}.net_products_agg_monthly_adj_manual man
    ON iadj.company = man.company AND iadj.year_month = man.year_month AND iadj.metric = man.metric
;

SELECT "*****-- End Combined Manual + Adjustment Metrics --*****"
;

-- End Combined Manual + Adjustment Metrics --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Adjustment Table Insert Metrics --

INSERT OVERWRITE TABLE net_products_agg_monthly_adjustment 
PARTITION (company,year_month,metric)

SELECT
dis.unit,
dis.value_type,
value,
mom_perc_Chg,
mom_diff,
prior_3_mo_perc_chg,
prior_3_mo_diff,
ytd_avg,
prev_months_max_year_month,
prev_months_max_val,
prev_months_min_year_month,
prev_months_min_val,
change_comment,
dis.tableau_display AS tableau_field,
jira_ticket,
company,
year_month,
metric
FROM 
${env:TMP_db}.net_products_agg_monthly_adj_stage
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON metric = dis.hive_metric
WHERE
company IS NOT NULL AND year_month IS NOT NULL AND metric IS NOT NULL
GROUP BY
company,
year_month,
metric,
dis.unit,
dis.value_type,
value,
mom_perc_Chg,
mom_diff,
prior_3_mo_perc_chg,
prior_3_mo_diff,
ytd_avg,
prev_months_max_year_month,
prev_months_max_val,
prev_months_min_year_month,
prev_months_min_val,
change_comment,
dis.tableau_display,
jira_ticket
;

SELECT "*****-- End Adjustment Table Insert Metrics --*****"
;
