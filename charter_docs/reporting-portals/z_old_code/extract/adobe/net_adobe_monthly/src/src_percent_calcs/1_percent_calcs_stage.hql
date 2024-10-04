-------------------------------------------------------------------------------

-- Populates the temp table net_products_agg_monthly_percents with percent calculations for L-CHTR, L-TWC, L-BHN, and Total Combined

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Drop and rebuild net_products_agg_monthly_percents temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.net_products_agg_monthly_percents PURGE
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_products_agg_monthly_percents
(
unit STRING, 
value_type STRING, 
value DECIMAL(15,4), 
mom_perc_chg DECIMAL(15,4), 
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
jira_ticket STRING)
PARTITIONED BY ( 
company STRING, 
year_month STRING, 
metric STRING)
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

SELECT '*****-- END - Drop and rebuild net_products_agg_monthly_percents temp table --*****'
;

-- END - Drop and rebuild net_products_agg_monthly_percents temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------