SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

--Combines Adjustments-combined data and percent calculations into one table

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined percents + Adjustments-combined Metrics --

INSERT OVERWRITE TABLE net_products_agg_monthly_tableau
PARTITION (company,year_month,metric)

SELECT
dis.unit AS unit,
dis.value_type AS value_type,
per.value,
per.mom_perc_Chg,
per.mom_diff,
per.prior_3_mo_perc_chg,
per.prior_3_mo_diff,
per.ytd_avg,
per.prev_months_max_year_month,
per.prev_months_max_val,
per.prev_months_min_year_month,
per.prev_months_min_val,
per.change_comment,
dis.tableau_display AS tableau_field,
per.jira_ticket,
per.company,
per.year_month,
per.metric
FROM 
${env:TMP_db}.net_products_agg_monthly_percents per
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON metric = dis.hive_metric
WHERE
company IS NOT NULL AND year_month IS NOT NULL AND metric IS NOT NULL
GROUP BY
per.company,
per.year_month,
per.metric,
dis.unit,
dis.value_type,
per.value,
per.mom_perc_Chg,
per.mom_diff,
per.prior_3_mo_perc_chg,
per.prior_3_mo_diff,
per.ytd_avg,
per.prev_months_max_year_month,
per.prev_months_max_val,
per.prev_months_min_year_month,
per.prev_months_min_val,
per.change_comment,
dis.tableau_display,
per.jira_ticket
;

SELECT "*****-- End Tableau Table Insert Metrics --*****"
;
