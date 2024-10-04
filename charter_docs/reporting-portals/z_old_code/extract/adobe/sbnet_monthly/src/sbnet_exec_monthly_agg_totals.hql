SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

--Takes all hive and adjustment table data and pivots into tableau feeder table format

--Creates an additional 'company' for 'Total Combined' and inserts into tableau feeder table

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT}
;

-------------------------------------------------------------------------------
-- Begin Combined Adjustments + Hive Metrics --

INSERT OVERWRITE TABLE sbnet_exec_monthly_tableau
PARTITION (company,year_month,metric)

SELECT
CASE WHEN adj.change_comment IS NOT NULL THEN adj.unit ELSE dis.unit END AS unit,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.value_type ELSE dis.value_type END AS value_type,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.value ELSE hiv.value END AS value,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.mom_perc_chg ELSE hiv.mom_perc_chg END AS mom_perc_chg,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.mom_diff ELSE hiv.mom_diff END AS mom_diff,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prior_3_mo_perc_chg ELSE hiv.prior_3_mo_perc_chg END AS prior_3_mo_perc_chg,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prior_3_mo_diff ELSE hiv.prior_3_mo_diff END AS prior_3_mo_diff,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.ytd_avg ELSE hiv.ytd_avg END AS ytd_avg,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_max_year_month ELSE hiv.prev_months_max_year_month END AS prev_months_max_year_month,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_max_val ELSE hiv.prev_months_max_val END AS prev_months_max_val,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_min_year_month ELSE hiv.prev_months_min_year_month END AS prev_months_min_year_month,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_min_val ELSE hiv.prev_months_min_val END AS prev_months_min_val,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.change_comment ELSE hiv.change_comment END AS change_comment,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.tableau_field ELSE dis.tableau_display END AS tableau_field,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.jira_ticket ELSE NULL END AS jira_ticket,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.company ELSE hiv.company END AS company,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.year_month ELSE hiv.year_month END AS year_month,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.metric ELSE hiv.metric END AS metric
FROM
sbnet_exec_monthly_pivot hiv
LEFT JOIN 
    sbnet_exec_monthly_adjustment adj
    ON hiv.company = adj.company AND hiv.year_month = adj.year_month AND hiv.metric = adj.metric
LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp dis
    ON hiv.metric = dis.hive_metric
WHERE 
hiv.year_month = '${env:YEAR_MONTH}'
;

SELECT '*****-- End Combined Adjustments + Hive Metrics Into Tableau Feed Table --*****'
;

-- End Combined Adjustments + Hive Metrics --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Total Combined Metrics --

INSERT OVERWRITE TABLE sbnet_exec_monthly_tableau
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type ,
value AS value,
mom_perc_chg AS mom_perc_chg,
mom_diff AS mom_diff,
prior_3_mo_perc_chg AS prior_3_mo_perc_chg,
prior_3_mo_diff AS prior_3_mo_diff,
ytd_avg AS ytd_avg,
prev_months_max_year_month AS prev_months_max_year_month,
prev_months_max_val AS prev_months_max_val,
prev_months_min_year_month AS prev_months_min_year_month,
prev_months_min_val AS prev_months_min_val,
'total_calculations' AS change_comment,
dis.tableau_display AS tableau_field,
jira_ticket AS jira_ticket,
CASE WHEN (grouping_id & 1)!=0 THEN 'Total Combined' ELSE company END AS company,
year_month AS year_month,
metric
FROM 
(
SELECT
year_month,
company,
unit AS unit,
value_type AS value_type,
SUM(IF(value IS NOT NULL,value,NULL)) AS value,
mom_perc_chg AS mom_perc_chg,
mom_diff AS mom_diff,
prior_3_mo_perc_chg AS prior_3_mo_perc_chg,
prior_3_mo_diff AS prior_3_mo_diff,
ytd_avg AS ytd_avg,
prev_months_max_year_month AS prev_months_max_year_month,
prev_months_max_val AS prev_months_max_val,
prev_months_min_year_month AS prev_months_min_year_month,
prev_months_min_val AS prev_months_min_val,
'total_calculations' AS change_comment,
tableau_field AS tableau_field,
jira_ticket AS jira_ticket,
metric,
CAST(grouping__id AS INT) AS grouping_id
FROM 
sbnet_exec_monthly_tableau
WHERE year_month = '${env:YEAR_MONTH}'
    AND company <> 'Total Combined'
GROUP BY
year_month,
company,
metric,
unit,
value_type,
mom_perc_chg,
mom_diff,
prior_3_mo_perc_chg,
prior_3_mo_diff,
ytd_avg,
prev_months_max_year_month,
prev_months_max_val,
prev_months_min_year_month,
prev_months_min_val,
'total_calculations',
tableau_field,
jira_ticket
GROUPING SETS ((year_month,metric))
) met_group
LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp dis
    ON metric = dis.hive_metric
;

SELECT '*****-- End Total Combined Metrics Into Tableau Feed Table --*****' 
;