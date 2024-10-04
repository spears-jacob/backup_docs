USE ${env:ENVIRONMENT};


-- Temp table to get the run partition_date of job
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_kpi_reprocess_run_date(run_date STRING)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

-- Below Tables used in Homepage/Header/Footer KPI Calcs
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_auth(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  counts int COMMENT 'Number of instances',
  count_visits int COMMENT 'Number of visits'
)
COMMENT 'Daily aggregate table providing auth metrics on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_one_time_payments(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  page_name string COMMENT 'Page Name',
  counts int COMMENT 'Number of instances'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_auto_payments(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  page_name string COMMENT 'Page Name',
  counts int COMMENT 'Number of instances'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_bill_pay_search(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  iva_entries string COMMENT 'User entries into IVA',
  iva_results string COMMENT 'Responses from IVA',
  search_text string COMMENT 'User entries into search bar',
  counts int COMMENT 'Number of instances'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_upgrade_link_clicks(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  detail string COMMENT 'Page Name',
  counts int COMMENT 'Number of instances'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_search_email_voice(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  search_terms string COMMENT 'User entries into search bar',
  counts int COMMENT 'Number of instances'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_iva_email_voice(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  search_terms string COMMENT 'User entries into search bar',
  counts int COMMENT 'Number of instances'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_kpis_metrics_agg(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  detail string COMMENT 'Lower level detail divisions of Sub Metric',
  counts int COMMENT 'Number of instances',
  count_visits int COMMENT 'Number of visits'
)
COMMENT 'Daily aggregate table providing one time payment metrics (before/after new homepage launch 9/11/2017) on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date')
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.net_asp_kpis_bpln(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  counts int COMMENT 'Number of instances',
  count_visits int COMMENT 'Number of visits'
)
COMMENT 'Daily aggregate table providing Bill Pay from Card OR Local Navigation metrics on .NET'
PARTITIONED BY (report_date STRING COMMENT 'Report Date');



--------------------------------------------
-- Production Table for Tableau
CREATE TABLE IF NOT EXISTS asp_kpi_metrics_agg_daily(
  metric string COMMENT 'Metric Name',
  sub_metric string COMMENT 'Metric Sub Name',
  detail string COMMENT 'Lower level detail divisions of Sub Metric',
  counts int COMMENT 'Number of instances',
  count_visits int COMMENT 'Number of visits'
) COMMENT 'Daily aggregate table providing Create ID flow enhancements on .NET'
PARTITIONED BY (partition_date STRING)
;

---------------------------------------------
--Duration reporting

CREATE TABLE IF NOT EXISTS asp_metrics_payment_duration_raw
(
  visit__visit_id STRING,
  state__view__current_page__name STRING,
  message__timestamp STRING,
  message__category STRING,
  browser_size_breakpoint STRING,
  visit__settings STRING
)
PARTITIONED BY
(
  partition_date STRING,
  source_table STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metrics_payment_duration_order
(
  visit__visit_id STRING,
  state__view__current_page__name STRING,
  message__category STRING,
  message__timestamp STRING,
  next_page STRING,
  next_timestamp STRING,
  browser_size_breakpoint STRING,
  visit__settings STRING
)
PARTITIONED BY
(
  partition_date STRING,
  source_table STRING
)
;

CREATE TABLE IF NOT EXISTS asp_metrics_payment_duration_agg
(
  visit__visit_id STRING,
  payment_step_current STRING,
  current_page STRING,
  payment_step_next STRING,
  next_page STRING,
  duration INT,
  browser_size_breakpoint STRING
)
PARTITIONED BY
(
  partition_date STRING,
  source_table STRING
)
;


CREATE TABLE IF NOT EXISTS asp_metrics_payment_flow_stats (
    payment_step_current    string,
    payment_step_next        string,
    value                   decimal(18,3)
)
partitioned BY
(
  payment_flow            string,
  partition_date          string,
  metric                  string,
  source_table            string
)
;

CREATE TABLE IF NOT EXISTS payment_stats_avg like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_05 like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_25 like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_50 like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_75 like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_95 like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_stdev like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_min like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_max like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_total like asp_metrics_payment_flow_stats;
CREATE TABLE IF NOT EXISTS payment_stats_count like asp_metrics_payment_flow_stats;

CREATE VIEW IF NOT EXISTS asp_v_metrics_payment_duration_stats as
  SELECT * FROM asp_metrics_payment_flow_stats
;

CREATE TABLE IF NOT EXISTS asp_agg_page_load_time_grouping_sets
  (   grouping_id int,
      grouped_partition_date_denver string,
      grouped_partition_date_hour_denver string,
      grouped_browser_name string,
      grouped_breakpoints string,
      partition_date_hour_denver string,
      browser_name string,
      browser_size_breakpoint string,
      page_name string,
      section string,
      unique_visits int,
      page_views bigint,
      avg_pg_ld double,
      total_pg_ld double,
      05_percentile double,
      10_percentile double,
      15_percentile double,
      20_percentile double,
      25_percentile double,
      30_percentile double,
      35_percentile double,
      40_percentile double,
      45_percentile double,
      50_percentile double,
      55_percentile double,
      60_percentile double,
      65_percentile double,
      70_percentile double,
      75_percentile double,
      80_percentile double,
      85_percentile double,
      90_percentile double,
      95_percentile double,
      99_percentile double
  )

PARTITIONED BY (partition_date_denver string)
;

CREATE TABLE IF NOT EXISTS asp_hourly_page_load_tenths
( page_name string,
  pg_load_sec_tenths double,
  unique_visits bigint,
  instances bigint)
PARTITIONED BY
( date_denver string,
  date_hour_denver string,
  domain string)
;


CREATE TABLE IF NOT EXISTS asp_hourly_page_load_tenths_by_browser
( browser_name string,
  page_name string,
  pg_load_sec_tenths double,
  unique_visits bigint,
  instances bigint)
PARTITIONED BY
( date_denver string,
  date_hour_denver string,
  domain string
);

CREATE TABLE IF NOT EXISTS asp_top_browsers
(   rank_browser INT,
    browser_name string,
    unique_visits BIGINT )
PARTITIONED BY
(   date_denver string,
    domain string        )
;

CREATE TABLE IF NOT EXISTS asp_login_attempt_timing_agg
(
visit_id STRING,
tBeforeSignIn INT,
tLoginAttempt int,
difference int,
home_page_status STRING
)
PARTITIONed BY (partition_date STRING)
;

CREATE VIEW IF NOT EXISTS asp_v_asp_login_attempt_timing_agg_ptile
AS
SELECT
*
FROM prod.asp_login_attempt_timing_agg_ptile;

CREATE TABLE IF NOT EXISTS asp_metrics_detail(
  value bigint,
  detail string,
  unit string )
PARTITIONED BY (
  metric string,
  platform string,
  domain string,
  company string,
  date_denver string,
  source_table string);

  -- core views purposefully point to prod
CREATE VIEW IF NOT EXISTS asp_v_metric_detail as
select * from  prod.asp_metrics_detail;

CREATE TABLE IF NOT EXISTS asp_metrics_detail_hourly(
  value bigint,
  detail string,
  date_hour_denver string,
  unit string )
PARTITIONED BY (
  metric string,
  platform string,
  domain string,
  company string,
  date_denver string,
  source_table string);


  -- core views purposefully point to prod
CREATE VIEW IF NOT EXISTS asp_v_metric_detail_hourly as
select * from  prod.asp_metrics_detail_hourly;

----- counts view definition, 180 days worth daily, 30 days hourly
drop view if exists asp_v_counts;

CREATE VIEW asp_v_counts
 COMMENT 'Daily and Hourly aggregate view counts of devices, instances, visits, and hhs across all consolidated metrics'
AS
SELECT value,
       metric,
       date_hour_denver,
       unit,
       platform,
       domain,
       company,
       date_denver,
       source_table,
       "hourly" as grain
FROM prod.asp_counts_hourly
WHERE (date_denver > DATE_SUB(CURRENT_DATE, 30))

UNION

SELECT value,
       metric,
       "All Hours" as date_hour_denver,
       unit,
       platform,
       domain,
       company,
       date_denver,
       source_table,
        "daily" as grain
 FROM prod.asp_counts_daily
 WHERE (date_denver > DATE_SUB(CURRENT_DATE, 180));

 ----- counts view definition, 180 days worth daily, 30 days hourly
 drop view if exists asp_v_metrics_detail;

 CREATE VIEW asp_v_metrics_detail
  COMMENT 'Daily and Hourly aggregate view counts of devices, instances, visits, and hhs across all consolidated detail metrics'
 AS
 SELECT value,
        detail,
        metric,
        date_hour_denver,
        unit,
        platform,
        domain,
        company,
        date_denver,
        source_table,
        "hourly" as grain
 FROM prod.asp_metrics_detail_hourly
 WHERE (date_denver > DATE_SUB(CURRENT_DATE, 30))

 UNION

 SELECT value,
        detail,
        metric,
        "All Hours" as date_hour_denver,
        unit,
        platform,
        domain,
        company,
        date_denver,
        source_table,
         "daily" as grain
  FROM prod.asp_metrics_detail
  WHERE (date_denver > DATE_SUB(CURRENT_DATE, 180));




-----------------------------


DROP VIEW IF EXISTS asp_v_hourly_page_load_tenths_by_top_browser;
CREATE VIEW IF NOT EXISTS asp_v_hourly_page_load_tenths_by_top_browser as
select  pl.date_denver,
        date_hour_denver,
        pl.domain,
        page_name,
        rank_browser,
        pl.browser_name,
        pg_load_sec_tenths,
        pl.unique_visits,
        instances
FROM asp_hourly_page_load_tenths_by_browser pl
inner join asp_top_browsers tb
  ON  pl.date_Denver = tb.date_denver
  AND pl.domain = tb.domain
  AND pl.browser_name = tb.browser_name
;

DROP VIEW IF EXISTS asp_v_hourly_page_load_seconds_by_top_browser;
CREATE VIEW IF NOT EXISTS asp_v_hourly_page_load_seconds_by_top_browser as
select  pl.date_denver,
        date_hour_denver,
        pl.domain,
        page_name,
        rank_browser,
        pl.browser_name,
        ROUND(pg_load_sec_tenths,0) as pg_load_sec,
        SUM(pl.unique_visits) as unique_visits,
        SUM(instances) as instances
FROM asp_hourly_page_load_tenths_by_browser pl
inner join asp_top_browsers tb
  ON  pl.date_Denver = tb.date_denver
  AND pl.domain = tb.domain
  AND pl.browser_name = tb.browser_name
GROUP BY pl.date_denver,
         date_hour_denver,
         pl.domain,
         page_name,
         rank_browser,
         pl.browser_name,
         ROUND(pg_load_sec_tenths,0)
;
