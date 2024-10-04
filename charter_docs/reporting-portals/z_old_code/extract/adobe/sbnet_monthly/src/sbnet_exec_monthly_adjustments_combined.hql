SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

--Combines manual files and adjustments into adjustment table

--Will activate the query below on the next iteration, once we have extricated the manual file values from the main aggregation process and moved them here

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT}
;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined Manual + Adjustment Metrics --

--DROP TABLE IF EXISTS ${env:TMP_db}.sbnet_exec_agg_monthly_adj_stage PURGE
--;
--
--CREATE TABLE ${env:TMP_db}.sbnet_exec_agg_monthly_adj_stage AS
--
--SELECT
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.unit ELSE man.unit END AS unit,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.value_type ELSE man.value_type END AS value_type,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.value ELSE man.value END AS value,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.mom_perc_chg ELSE man.mom_perc_chg END AS mom_perc_chg,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.mom_diff ELSE man.mom_diff END AS mom_diff,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prior_3_mo_perc_chg ELSE man.prior_3_mo_perc_chg END AS prior_3_mo_perc_chg,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prior_3_mo_diff ELSE man.prior_3_mo_diff END AS prior_3_mo_diff,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.ytd_avg ELSE man.ytd_avg END AS ytd_avg,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_max_year_month ELSE man.prev_months_max_year_month END AS prev_months_max_year_month,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_max_val ELSE man.prev_months_max_val END AS prev_months_max_val,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_min_year_month ELSE man.prev_months_min_year_month END AS prev_months_min_year_month,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.prev_months_min_val ELSE man.prev_months_min_val END AS prev_months_min_val,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.change_comment ELSE man.change_comment END AS change_comment,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.jira_ticket ELSE man.jira_ticket END AS jira_ticket,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.company ELSE man.company END AS company,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.year_month ELSE man.year_month END AS year_month,
--CASE WHEN iadj.change_comment IS NOT NULL THEN iadj.metric ELSE man.metric END AS metric
--FROM
--${env:TMP_db}.sbnet_exec_agg_monthly_adjustment_init iadj
--LEFT JOIN 
--    ${env:TMP_db}.sbnet_exec_agg_monthly_adj_manual man
--    ON iadj.company = man.company AND iadj.year_month = man.year_month AND iadj.metric = man.metric
--;
--
--SELECT "*****-- End Combined Manual + Adjustment Metrics --*****"
--;


--Instead of the above, use the below to bring in ONLY manual adjustments until the next iteration. We are aware of the reduncancies, but this infrastructure makes it easier to build out in the future.

DROP TABLE IF EXISTS ${env:TMP_db}.sbnet_exec_agg_monthly_adj_stage PURGE
;

CREATE TABLE ${env:TMP_db}.sbnet_exec_agg_monthly_adj_stage AS

SELECT
unit,
value_type,
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
tableau_field,
jira_ticket,
company,
year_month,
metric
FROM 
${env:TMP_db}.sbnet_exec_monthly_adjustment_init
;

SELECT "*****-- End Combined Manual + Adjustment Metrics --*****"
;

-- End Combined Manual + Adjustment Metrics --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Adjustment Table Insert Metrics --

INSERT OVERWRITE TABLE sbnet_exec_monthly_adjustment 
PARTITION (company,year_month,metric)

SELECT
dis.unit,
dis.value_type,
stg.value,
stg.mom_perc_Chg,
stg.mom_diff,
stg.prior_3_mo_perc_chg,
stg.prior_3_mo_diff,
stg.ytd_avg,
stg.prev_months_max_year_month,
stg.prev_months_max_val,
stg.prev_months_min_year_month,
stg.prev_months_min_val,
stg.change_comment,
dis.tableau_display AS tableau_field,
stg.jira_ticket,
stg.company,
stg.year_month,
stg.metric
FROM 
${env:TMP_db}.sbnet_exec_agg_monthly_adj_stage stg
LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp dis
    ON stg.metric = dis.hive_metric
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







