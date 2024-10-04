SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

--Combines Adjustments-combined data and percent calculations into one table

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined percents + Adjustments-combined Metrics --

INSERT OVERWRITE TABLE sbnet_exec_monthly_tableau
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
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
${env:TMP_db}.sbnet_exec_monthly_percents
LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp dis
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

SELECT "*****-- End Tableau Table Insert Metrics --*****"
;
