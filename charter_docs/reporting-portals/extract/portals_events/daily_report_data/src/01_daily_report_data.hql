USE ${env:ENVIRONMENT};

set hive.vectorized.execution.enabled = false;

SET AppFiguresLag=3;
SELECT "\n\nThe AppFiguresLag variable is used to set how long to lag the app figures counts and reviews,\n\n now set to ${hiveconf:AppFiguresLag} days.\n\n";

set MetricLag=91;
SELECt "
        The MetricLag variable is used to set how long to lag the metrics, to allow for past X week day average:

        ${hiveconf:MetricLag}


        Now extracting atomic (single-valued) measurements which are counts.

        ";

DROP TABLE IF EXISTS dailyReportDataPreAggregate;

CREATE TEMPORARY TABLE dailyReportDataPreAggregate AS

select label_date_Denver as date_denver,
       date_add(label_date_Denver,1) AS ReportDay,
       metric_name as metric,
       STRING(CAST(SUM(metric_value) as INT)) as value,
       application_name AS domain
  from prod.venona_set_agg_portals
 where (label_date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
   AND grain='daily'
   and grouping_id = '2049'
   AND application_name in ('specnet', 'myspectrum', 'smb')
   AND unit_type='instances'
   AND metric_name IN ('portals_login_attempts',
                    'portals_login_failures',
                    'portals_site_unique_auth',
                    'portals_support_page_views',
                    'portals_iva_opens',
                    'portals_call_support',
                    'portals_view_online_statments',
                    'portals_tv_equipment_reset_flow_starts',
                    'portals_tv_equipment_reset_flow_successes',
                    'portals_tv_equipment_reset_flow_failures',
                    'portals_voice_equipment_reset_flow_starts',
                    'portals_voice_equipment_reset_flow_successes',
                    'portals_voice_equipment_reset_flow_failures',
                    'portals_internet_equipment_reset_flow_starts',
                    'portals_internet_equipment_reset_flow_successes',
                    'portals_internet_equipment_reset_flow_failures',
                    'portals_rescheduled_service_appointments',
                    'portals_cancelled_service_appointments',
                    'portals_one_time_payment_starts',
                    'portals_one_time_payment_successes',
                    'portals_one_time_payment_failures',
                    'portals_one_time_payment_successes_with_ap_enroll',
                    'portals_set_up_auto_payment_starts',
                    'portals_set_up_auto_payment_failures',
                    'portals_set_up_auto_payment_successes',
                    'portals_equipment_confirm_edit_ssid_select_action',
                    'portals_scp_click_confirm_pause_device'
                   )
 GROUP BY application_name,
          metric_name,
          label_date_Denver
 order by application_name,
          label_date_Denver;

----
-- Venona bounces for resi
----
INSERT INTO dailyReportDataPreAggregate
select date_denver,
       date_add(date_Denver,1) AS ReportDay,
       'venona_bounces_home-unauth' as metric,
       STRING(SUM(bounces)) as value,
       'specnet' as domain
  from asp_v_bounces_entries
 where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
   AND domain in ('resi')
   AND page_name='home-unauth'
 GROUP BY domain,
          date_Denver
 order by domain,
          date_Denver;
----
-- Venona entries for resi
----
INSERT INTO dailyReportDataPreAggregate
select date_denver,
       date_add(date_Denver,1) AS ReportDay,
       'venona_entries_home-unauth'as metric,
       STRING(SUM(entries)) as value,
       'specnet' as domain
  from asp_v_bounces_entries
 where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
   AND domain in ('resi')
   AND page_name='home-unauth'
 GROUP BY domain,
          date_Denver
 order by domain,
          date_Denver;

----
-- Venona bounces for smb
----
INSERT INTO dailyReportDataPreAggregate
select date_denver,
       date_add(date_Denver,1) AS ReportDay,
       'venona_bounces_home-unauth' as metric,
       STRING(SUM(bounces)) as value,
       domain
  from asp_v_bounces_entries
 where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
   AND domain = 'smb'
   AND page_name='homeUnauth'
 GROUP BY domain,
          date_Denver
 order by domain,
          date_Denver;

----
-- Venona entries for smb
----
INSERT INTO dailyReportDataPreAggregate
select date_denver,
       date_add(date_Denver,1) AS ReportDay,
       'venona_entries_home-unauth'as metric,
       STRING(SUM(entries)) as value,
       domain
  from asp_v_bounces_entries
 where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
   AND domain = 'smb'
   AND page_name='homeUnauth'
 GROUP BY domain,
          date_Denver
 order by domain,
          date_Denver;

-- App Downloads and Updates
INSERT INTO dailyReportDataPreAggregate
select DISTINCT  a.date_Denver,
                 date_add(a.date_Denver,${hiveconf:AppFiguresLag}) AS ReportDay,
                 a.metric,
                 a.value,
                 d.domain
from  asp_v_app_daily_app_figures  a
INNER JOIN dailyReportDataPreAggregate d
on a.date_Denver = d.date_Denver
WHERE a.company = 'CHTR'
and d.domain = 'myspectrum'
;

-----------
-- Below is the comparison with prior one week of the same day of the week

DROP TABLE IF EXISTS asp_daily_report_data_staging;

CREATE TEMPORARY TABLE asp_daily_report_data_staging AS
select domain,
       date_Denver,
       ReportDay,
       metric,
       string(value) as value,
       ROUND(((value/((lag( value, 07) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY domain, metric order by date_Denver )) / 8) ) - 1 )
              ,5) as VsAVGPrior8wksSameDayOfWk,
     --ROUND(((value/lag(value, 07) over (PARTITION BY domain, metric order by date_Denver )) - 1 ),5) as VsPrior1wkSameDayOfWk,
       'Not a review metric' as review_comment,
       '' as additional_comment
  from dailyReportDataPreAggregate
;

----------------------
-- Below is weighted averaging for percentage comparison metrics


DROP TABLE IF EXISTS asp_daily_report_data_staging_weighted_averaging_bounces;

CREATE TEMPORARY TABLE asp_daily_report_data_staging_weighted_averaging_bounces AS
select domain,
       date_Denver,
       ReportDay,
       metric,
       string(value) as value,
       ROUND(((lag( value, 07) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY domain, metric order by date_Denver )) / 8)
              ,5) as AVGPrior8wksSameDayOfWk
       --ROUND((lag( value, 07) over (PARTITION BY domain, metric order by date_Denver )),5) as Prior1wkSameDayOfWk
  from dailyReportDataPreAggregate
 where metric in('venona_bounces_home-unauth')
;

DROP TABLE IF EXISTS asp_daily_report_data_staging_weighted_averaging_entries;
CREATE TEMPORARY TABLE asp_daily_report_data_staging_weighted_averaging_entries AS
select domain,
       date_Denver,
       ReportDay,
       metric,
       string(value) as value,
       ROUND(((lag( value, 07) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY domain, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY domain, metric order by date_Denver )) / 8)
              ,5) as AVGPrior8wksSameDayOfWk
     --ROUND((lag( value, 07) over (PARTITION BY domain, metric order by date_Denver )),5) as Prior1wkSameDayOfWk
  from dailyReportDataPreAggregate
 where metric in('venona_entries_home-unauth')
;

-- Calculate bounces and bounce difference from weighted average of past 8 same day of week

INSERT INTO asp_daily_report_data_staging
select b.domain,
       b.date_Denver,
       b.ReportDay,
       'venona_bounce_rate_home_unauth' as metric,
       string(ROUND(b.value/e.value,5)) as value,
       ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
     --ROUND((b.Prior1wkSameDayOfWk/e.Prior1wkSameDayOfWk)-(b.value/e.value),5) as VsPrior1wkSameDayOfWk,
       'Not a review metric' as review_comment,
       '' as additional_comment
  from asp_daily_report_data_staging_weighted_averaging_bounces b
 INNER JOIN (select *
               FROM asp_daily_report_data_staging_weighted_averaging_entries
              WHERE metric = 'venona_entries_home-unauth') e
    ON b.date_Denver = e.date_Denver
   AND b.domain = e.domain
;

-- Adds App reviews
INSERT INTO asp_daily_report_data_staging
SELECt DISTINCT d.domain,
                a.date_Denver,
                date_add(a.date_Denver,${hiveconf:AppFiguresLag}) AS ReportDay,
                'App Review' as metric,
                a.stars as value,
                null as VsAVGPrior8wksSameDayOfWk,
              --null as VsPrior1wkSameDayOfWk,
                a.review as review_comment,
                a.platform AS additional_comment
FROM asp_v_app_daily_app_figures_reviews a
INNER JOIN dailyReportDataPreAggregate d
on a.date_Denver = d.date_Denver
and a.company = 'CHTR'
and d.domain ='myspectrum'
;

-- Metric Tableau name Lookup
DROP TABLE IF EXISTS ${env:LKP_db}.asp_daily_report_data_metrics PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.asp_daily_report_data_metrics
( hive_metric STRING,
  tableau_metric STRING
);

-- Change names of metrics here to be reflected in Tableau

INSERT INTO TABLE ${env:LKP_db}.asp_daily_report_data_metrics VALUES
    ('portals_login_attempts', 'Login Attempts'),
    ('portals_login_failures', 'Login Failures'),
    ('portals_site_unique_auth', 'Login Successes'),
    ('portals_support_page_views', 'Support Page Views'),
    ('portals_iva_opens', 'IVA Opens'),
    ('portals_call_support', 'Call Support'),
    ('portals_view_online_statments', 'View Online Statments'),
    ('portals_one_time_payment_starts', 'OTP Starts'),
    ('portals_one_time_payment_successes', 'OTP Successes'),
    ('portals_one_time_payment_failures', 'OTP Failures'),
    ('portals_one_time_payment_successes_with_ap_enroll', 'OTP w/AP Successes'),
    ('portals_set_up_auto_payment_starts', 'Set Up AP Starts'),
    ('portals_set_up_auto_payment_failures', 'Set Up AP Failures'),
    ('portals_set_up_auto_payment_successes', 'Set Up AP Successes'),
    ('portals_equipment_confirm_edit_ssid_select_action', 'Edit SSID Confirmations'),
    ('portals_tv_equipment_reset_flow_starts', 'TV Reset Starts'),
    ('portals_tv_equipment_reset_flow_successes', 'TV Reset Successes'),
    ('portals_tv_equipment_reset_flow_failures', 'TV Reset Fails'),
    ('portals_voice_equipment_reset_flow_starts', 'Voice Reset Starts'),
    ('portals_voice_equipment_reset_flow_successes', 'Voice Reset Successes'),
    ('portals_voice_equipment_reset_flow_failures', 'Voice Reset Failures'),
    ('portals_internet_equipment_reset_flow_starts', 'Internet Equipment Reset Starts'),
    ('portals_internet_equipment_reset_flow_successes', 'Internet Reset Successes'),
    ('portals_internet_equipment_reset_flow_failures', 'Internet Reset Failures'),
    ('portals_rescheduled_service_appointments', 'Rescheduled Appointments'),
    ('portals_cancelled_service_appointments', 'Cancelled Appointments'),
    ('venona_bounces_home-unauth', 'Bounces on Unauth Home Page'),
    ('venona_entries_home-unauth', 'Entries on Unauth Home Page'),
    ('venona_bounce_rate_home_unauth', 'Bounce Rate on Unauth Home Page'),
    ('portals_scp_click_confirm_pause_device', 'WiFi Equipment Pauses')
;


-- Insert in to daily table

INSERT OVERWRITE TABLE asp_daily_report_data PARTITION(date_denver)
SELECt DISTINCT reportday,
                COALESCE(tableau_metric, metric) AS metric,
                STRING(value) as value,
                VsAVGPrior8wksSameDayOfWk,
              --vsprior1wksamedayofwk,
                review_comment,
                additional_comment,
                domain,
                date_denver
FROM asp_daily_report_data_staging
LEFT OUTER JOIN ${env:LKP_db}.asp_daily_report_data_metrics
ON metric = hive_metric
;
