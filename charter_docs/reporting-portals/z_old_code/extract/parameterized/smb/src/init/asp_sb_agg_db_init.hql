USE ${env:ENVIRONMENT};


DROP TABLE IF EXISTS asp_sb_monthy_agg_calc PURGE;
DROP TABLE IF EXISTS asp_sb_fiscal_monthly_agg_calc PURGE;
DROP TABLE IF EXISTS asp_sb_daily_agg_calc PURGE;
DROP TABLE IF EXISTS asp_sb_monthy_agg_totals PURGE;
DROP TABLE IF EXISTS asp_sb_fiscal_monthly_agg_totals PURGE;
DROP TABLE IF EXISTS asp_sb_daily_agg_totals PURGE;

--- calc tables
CREATE TABLE IF NOT EXISTS asp_sb_monthly_agg_calc
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_sb_fiscal_monthly_agg_calc
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_fiscal_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_sb_daily_agg_calc
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    date_Denver STRING
)
;

--- total tables
CREATE TABLE IF NOT EXISTS asp_sb_monthly_agg_totals
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_sb_fiscal_monthly_agg_totals
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_fiscal_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_sb_daily_agg_totals
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    date_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_sb_bounce_rate_hist(
  bounces int,
  entries int,
  page_name string)
PARTITIONED BY (
  company string,
  date_denver string)
;

--- view abstraction layer ---
DROP VIEW IF EXISTS asp_v_sbnet_events ;
DROP VIEW IF EXISTS asp_v_bhn_bill_pay_events ;
DROP VIEW IF EXISTS asp_v_bhn_my_services_events ;
DROP VIEW IF EXISTS asp_v_twc_bus_global_events ;
DROP VIEW IF EXISTS asp_v_federated_id ;
DROP VIEW IF EXISTS asp_v_federated_id_auth_attempts_total ;

CREATE VIEW IF NOT EXISTS asp_v_sbnet_events AS SELECT * from prod.sbnet_events;
CREATE VIEW IF NOT EXISTS asp_v_bhn_bill_pay_events AS SELECT * from prod.bhn_bill_pay_events;
CREATE VIEW IF NOT EXISTS asp_v_bhn_my_services_events AS SELECT * from prod.bhnmyservices_events;
CREATE VIEW IF NOT EXISTS asp_v_twc_bus_global_events AS SELECT * from prod.twcbusglobal_events;
CREATE VIEW IF NOT EXISTS asp_v_federated_id AS SELECT * from prod.federated_id;
CREATE VIEW IF NOT EXISTS asp_v_federated_id_auth_attempts_total AS SELECT * from prod.federated_id_auth_attempts_total;

CREATE TABLE IF NOT EXISTS asp_sbnet_fiscal_monthly_trend_pre
(unit STRING,
value STRING,
domain STRING,
tableau_display STRING,
numerator1 STRING,
denominator1 STRING,
numerator3 STRING,
denominator3 STRING,
numerator12 STRING,
denominator12 STRING)
PARTITIONED BY
(company STRING,
year_month STRING,
metric STRING)
;

CREATE TABLE IF NOT EXISTS asp_sbnet_fiscal_monthly_trend_perc
(unit STRING,
value STRING,
mom_perc_chg DECIMAL (15,3),
prior_3_mo_perc_chg DECIMAL (15,3),
yoy_perc_chg DECIMAL (15,3),
domain STRING,
tableau_display STRING)
PARTITIONED BY
(company STRING,
year_month STRING,
metric STRING)
;

-- -- VIEWS
-- Make sure to add any new data sources here

DROP VIEW IF EXISTS asp_sb_monthly_agg_raw_adj;

CREATE VIEW IF NOT EXISTS asp_sb_monthly_agg_raw_adj
AS
  SELECT raw.company,
         raw.metric,
         COALESCE(adj.value,raw.value) as value,
         raw.unit,
         raw.year_month_Denver,
         adj.change_comment,
         adj.jira_ticket
  from  asp_monthly_agg_raw raw
  LEFT OUTER JOIN ( SELECT value, unit, company, year_month, metric, change_comment, jira_ticket
                    FROM sbnet_exec_monthly_adjustment
                    WHERE change_comment <> 'manual_file'
                    AND value IS NOT NULL) adj
  ON  raw.company = adj.company
  AND raw.year_month_Denver = adj.year_month
  AND raw.metric = adj.metric
  WHERE raw.domain = 'sb';


DROP VIEW IF EXISTS asp_sb_fiscal_monthly_agg_raw_adj;
CREATE VIEW IF NOT EXISTS asp_sb_fiscal_monthly_agg_raw_adj
AS
  SELECT raw.company,
         raw.metric,
         COALESCE(adj.value,raw.value) as value,
         raw.unit,
         raw.year_month as year_fiscal_month_denver,
         adj.change_comment,
         adj.jira_ticket
  from  asp_v_fiscal_monthly_agg_amn raw
  LEFT OUTER JOIN ( SELECT value, unit, company, year_month, metric, change_comment, jira_ticket
                    FROM sbnet_exec_monthly_adjustment
                    WHERE change_comment <> 'manual_file'
                    AND value IS NOT NULL) adj
  ON  raw.company = adj.company
  AND raw.year_month = adj.year_month
  AND raw.metric = adj.metric
  where raw.domain = 'sb';

DROP VIEW IF EXISTS asp_sb_daily_agg_raw;
CREATE VIEW IF NOT EXISTS asp_sb_daily_agg_raw
AS
  SELECt  company,
          metric,
          value,
          unit,
          date_Denver
  from asp_daily_agg_raw
  where domain = 'sb';

-- tableau_display names VIEWS


DROP VIEW IF EXISTS asp_v_sb_monthly_tableau;
CREATE VIEW IF NOT EXISTS asp_v_sb_monthly_tableau
AS
  SELECT am.value,
         am.unit,
         company,
         year_month_Denver as year_month,
         COALESCE(tableau_display,metric) as tableau_display,
         metric
  from agg_monthly am
  LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp names
  ON am.metric = names.hive_metric
  WHERE platform = 'asp'
  AND domain = 'sb'
;

DROP VIEW IF EXISTS asp_v_sb_fiscal_monthly_tableau;
CREATE VIEW IF NOT EXISTS asp_v_sb_fiscal_monthly_tableau
AS
  SELECT afm.value,
         afm.unit,
         company,
         year_fiscal_month_Denver as year_month,
         COALESCE(tableau_display,metric) as tableau_display,
         metric
  from agg_fiscal_monthly afm
  LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp names
  ON afm.metric = names.hive_metric
  WHERE platform = 'asp'
  AND domain = 'sb'
;

DROP VIEW IF EXISTS asp_v_sb_daily_tableau;
CREATE VIEW IF NOT EXISTS asp_v_sb_daily_tableau
AS
  SELECT ad.value,
         ad.unit,
         company,
         date_Denver,
         COALESCE(tableau_display,metric) as tableau_display,
         metric
  from agg_daily ad
  LEFT JOIN ${env:TMP_db}.sbnet_exec_tableau_metric_lkp names
  ON ad.metric = names.hive_metric
  WHERE platform = 'asp'
  AND domain = 'sb'
;

DROP VIEW IF EXISTS asp_sbnet_fiscal_monthly_trend_lags;
CREATE VIEW IF NOT EXISTS asp_sbnet_fiscal_monthly_trend_lags AS
SELECT
(value-(LAG(value,1) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num1m,
(LAG(value,1) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver)) AS den1m,
(value-(LAG(value,3) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num3m,
(LAG(value,3) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver)) AS den3m,
(value-(LAG(value,12) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num12m,
(LAG(value,12) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver)) AS den12m,
company,
year_fiscal_month_denver,
metric
FROM agg_fiscal_monthly
;

DROP VIEW IF EXISTS asp_v_sbnet_fiscal_monthly_trend
;
CREATE VIEW IF NOT EXISTS asp_v_sbnet_fiscal_monthly_trend AS
SELECT
*
FROM asp_sbnet_fiscal_monthly_trend_perc
;
