

SELECT DISTINCT year_fiscal_month
FROM
(SELECT
metric_family,
hive_metric,
tableau_name,
name_w_total,
company,
metric_value,
metric_value_string,
fm_metric_total,
fm_metric_total_string,
year_fiscal_month,
data_source,
platform,
domain,
quantum_start_fm,
CASE
  WHEN company = 'CHTR' THEN 1
  WHEN company = 'BHN' THEN 2
  WHEN company = 'TWC' THEN 3
  WHEN company = 'Total Combined' THEN 5
  ELSE 4 END AS com_order
FROM asp_prod_monthly_app
GROUP BY
metric_family,
hive_metric,
tableau_name,
name_w_total,
company,
metric_value,
metric_value_string,
fm_metric_total,
fm_metric_total_string,
year_fiscal_month,
data_source,
platform,
domain,
quantum_start_fm,
CASE
  WHEN company = 'CHTR' THEN 1
  WHEN company = 'BHN' THEN 2
  WHEN company = 'TWC' THEN 3
  WHEN company = 'Total Combined' THEN 5
  ELSE 4 END
HAVING year_fiscal_month = MAX(year_fiscal_month)
  OR year_fiscal_month = ADD_MONTHS(MAX(year_fiscal_month),-12)
  OR year_fiscal_month = ADD_MONTHS(MAX(year_fiscal_month),-3)
  OR year_fiscal_month = ADD_MONTHS(MAX(year_fiscal_month),-2)
ORDER BY
metric_family,
tableau_name,
year_fiscal_month,
com_order
)a;


SET domain=app;
SET adobe=asp_metric_pivot_app_adobe;
SET quantum=asp_metric_pivot_app_quantum;
SET lkp=${env:LKP_db}.asp_pm_metric_lkp;
SET nulls=asp_pm_app_adobe_np;
SET uniraw=asp_v_pm_app_unified_raw;
SET adobe_stg=asp_prod_monthly_app_adobe_stage;
SET quantum_stg=asp_prod_monthly_app_quantum_stage;
SET stage=asp_prod_monthly_app_stage;
SET format=asp_prod_monthly_app;
SET tab=asp_v_prod_monthly_app_tab;
