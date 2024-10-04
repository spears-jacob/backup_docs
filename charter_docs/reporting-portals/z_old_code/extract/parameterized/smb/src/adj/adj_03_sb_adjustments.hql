SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

USE ${env:ENVIRONMENT}
;

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
