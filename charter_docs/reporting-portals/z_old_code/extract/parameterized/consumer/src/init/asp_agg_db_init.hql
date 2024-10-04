USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_monthly_agg_raw
(   value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (domain STRING, company STRING, year_month_Denver STRING, metric STRING)
;

CREATE TABLE IF NOT EXISTS asp_fiscal_monthly_agg_raw
(   value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (domain STRING, company STRING, year_fiscal_month_Denver STRING, metric STRING)
;

CREATE TABLE IF NOT EXISTS asp_daily_agg_raw
(   value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (domain STRING, company STRING, date_Denver STRING, metric STRING)
;


CREATE TABLE IF NOT EXISTS agg_monthly
(   value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (platform STRING, domain STRING, company STRING, year_month_Denver STRING, metric STRING)
;

CREATE TABLE IF NOT EXISTS agg_fiscal_monthly
(   value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (platform STRING, domain STRING, company STRING, year_fiscal_month_Denver STRING, metric STRING)
;

CREATE TABLE IF NOT EXISTS agg_daily
(   value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (platform STRING, domain STRING, company STRING, date_Denver STRING, metric STRING)
;

CREATE TABLE IF NOT EXISTS asp_net_webmail_twc_bhn_monthly(
  twc_webmail_page_views string,
  bhn_webmail_page_views string)
PARTITIONED BY (
  partition_year_month string)
;

CREATE TABLE IF NOT EXISTS asp_operational_daily(
  reportday string,
  metric string,
  value string,
  vsavgprior8wkssamedayofwk decimal(15,5),
  review_comment string,
  additional_comment string)
PARTITIONED BY (
  domain string,
  date_denver string)
;

DROP TABLE IF EXISTS asp_net_monthy_agg_calc PURGE;
DROP TABLE IF EXISTS asp_net_fiscal_monthly_agg_calc PURGE;
DROP TABLE IF EXISTS asp_net_daily_agg_calc PURGE;
DROP TABLE IF EXISTS asp_net_monthy_agg_totals PURGE;
DROP TABLE IF EXISTS asp_net_fiscal_monthly_agg_totals PURGE;
DROP TABLE IF EXISTS asp_net_daily_agg_totals PURGE;

--- calc tables
CREATE TABLE IF NOT EXISTS asp_net_monthly_agg_calc
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_net_fiscal_monthly_agg_calc
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_fiscal_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_net_daily_agg_calc
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    date_Denver STRING
)
;

--- total tables
CREATE TABLE IF NOT EXISTS asp_net_monthly_agg_totals
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_net_fiscal_monthly_agg_totals
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    year_fiscal_month_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_net_daily_agg_totals
(   company STRING,
    domain STRING,
    metric STRING,
    value DECIMAL(15,5),
    unit STRING,
    date_Denver STRING
)
;

CREATE TABLE IF NOT EXISTS asp_net_fiscal_monthly_trend_pre
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

CREATE TABLE IF NOT EXISTS asp_net_fiscal_monthly_trend_perc
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

--- view abstraction layer ---
DROP VIEW IF EXISTS asp_v_net_events ;
DROP VIEW IF EXISTS asp_v_bhn_bill_pay_events ;
DROP VIEW IF EXISTS asp_v_bhn_residential_events ;
DROP VIEW IF EXISTS asp_v_twc_residential_global_events ;
DROP VIEW IF EXISTS asp_v_net_webmail_twc_bhn_metrics_monthly ;
DROP VIEW IF EXISTS asp_v_federated_id ;
DROP VIEW IF EXISTS asp_v_federated_id_auth_attempts_total ;

CREATE VIEW IF NOT EXISTS asp_v_net_events AS SELECT * from prod.net_events;
CREATE VIEW IF NOT EXISTS asp_v_bhn_bill_pay_events AS SELECT * from prod.bhn_bill_pay_events;
CREATE VIEW IF NOT EXISTS asp_v_bhn_residential_events AS SELECT * from prod.bhn_residential_events;
CREATE VIEW IF NOT EXISTS asp_v_twc_residential_global_events AS SELECT * from prod.twc_residential_global_events;
CREATE VIEW IF NOT EXISTS asp_v_net_webmail_twc_bhn_metrics_monthly AS SELECT * from prod.net_webmail_twc_bhn_metrics_monthly;
CREATE VIEW IF NOT EXISTS asp_v_federated_id AS SELECT * from prod.federated_id;
CREATE VIEW IF NOT EXISTS asp_v_federated_id_auth_attempts_total AS SELECT * from prod.federated_id_auth_attempts_total;


-- -- VIEWS
-- Make sure to add any new data sources here
DROP VIEW IF EXISTS asp_v_amnesty;

CREATE VIEW asp_v_amnesty AS
SELECT  rep_suite,
        company,
        year_month,
        metric,
        unit,
        value,
        change_comment
FROM prod.asp_amnesty_hist
where company <> 'L-Total';


DROP VIEW IF EXISTS asp_net_monthly_agg_raw_adj;

CREATE VIEW IF NOT EXISTS asp_net_monthly_agg_raw_adj
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
                    FROM net_products_agg_monthly_adjustment
                    WHERE change_comment <> 'manual_file'
                    AND value IS NOT NULL) adj
  ON  raw.company = adj.company
  AND raw.year_month_Denver = adj.year_month
  AND raw.metric = adj.metric
  WHERE raw.domain = 'resi'
;

DROP VIEW IF EXISTS asp_v_fiscal_monthly_agg_amn;
CREATE VIEW IF NOT EXISTS asp_v_fiscal_monthly_agg_amn AS
SELECT  rep_suite AS domain,
        company,
        metric,
        value,
        unit,
        year_month,
        change_comment,
        NULL AS jira_ticket
FROM asp_v_amnesty
WHERE year_month < '2018-01'

 UNION ALL

SELECT  domain,
        company,
        metric,
        value,
        unit,
        year_fiscal_month_denver AS year_month,
        NULL AS change_comment,
        NULL AS jira_ticket
FROM asp_fiscal_monthly_agg_raw
WHERE (year_fiscal_month_denver >= '2018-01')
;

DROP VIEW IF EXISTS asp_net_fiscal_monthly_agg_raw_adj;
CREATE VIEW IF NOT EXISTS asp_net_fiscal_monthly_agg_raw_adj
AS
  SELECT raw.company,
         raw.metric,
         COALESCE(adj.value,raw.value) AS value,
         raw.unit,
         raw.year_month AS year_fiscal_month_denver,
         adj.change_comment,
         adj.jira_ticket
  FROM  asp_v_fiscal_monthly_agg_amn raw
  LEFT OUTER JOIN ( SELECT value, unit, company, year_month, metric, change_comment, jira_ticket
                    FROM net_products_agg_monthly_adjustment
                    WHERE change_comment <> 'manual_file'
                    AND value IS NOT NULL) adj
  ON  raw.company = adj.company
  AND raw.year_month = adj.year_month
  AND raw.metric = adj.metric
  WHERE raw.domain = 'resi';

DROP VIEW IF EXISTS asp_v_net_daily_agg_raw;
CREATE VIEW IF NOT EXISTS asp_v_net_daily_agg_raw
AS
  SELECT  company,
          metric,
          value,
          unit,
          date_Denver
  FROM asp_daily_agg_raw
  WHERE domain = 'resi';


-- tableau_display names VIEWS


DROP VIEW IF EXISTS asp_v_net_monthly_tableau;


CREATE VIEW IF NOT EXISTS asp_v_net_monthly_tableau
AS
  SELECT am.value,
         am.unit,
         company,
         year_month_Denver as year_month,
         COALESCE(tableau_display,metric) as tableau_display,
         metric
  from agg_monthly am
  LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp names
  ON am.metric = names.hive_metric
  WHERE platform = 'asp'
  AND domain = 'resi'
;

DROP VIEW IF EXISTS asp_v_net_fiscal_monthly_tableau;
CREATE VIEW IF NOT EXISTS asp_v_net_fiscal_monthly_tableau
AS
  SELECT afm.value,
         afm.unit,
         company,
         year_fiscal_month_Denver as year_month,
         COALESCE(tableau_display,metric) as tableau_display,
         metric
  from agg_fiscal_monthly afm
  LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp names
  ON afm.metric = names.hive_metric
  WHERE platform = 'asp'
  AND domain = 'resi'
;

DROP VIEW IF EXISTS asp_v_net_daily_tableau;
CREATE VIEW IF NOT EXISTS asp_v_net_daily_tableau
AS
  SELECT ad.value,
         ad.unit,
         company,
         date_Denver,
         COALESCE(tableau_display,metric) as tableau_display,
         metric
  from agg_daily ad
  LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp names
  ON ad.metric = names.hive_metric
  WHERE platform = 'asp'
  AND domain = 'resi'
;

DROP VIEW IF EXISTS asp_net_fiscal_monthly_trend_lags;
CREATE VIEW IF NOT EXISTS asp_net_fiscal_monthly_trend_lags AS
SELECT  (value-(LAG(value,1) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num1m,
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
