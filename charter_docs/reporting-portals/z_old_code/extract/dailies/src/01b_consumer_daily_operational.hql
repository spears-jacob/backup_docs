USE ${env:ENVIRONMENT};

set MetricLag=91;

SELECt "
        The MetricLag variable is used to set how long to lag the metrics, to allow for past X week day average:

        ${hiveconf:MetricLag}


        Now extracting atomic (single-valued) measurements which are counts.

        ";

-- READ ME IF CHANGING BELOW -- asp_v_counts_daily adobe source, et cetera
-- When the time comes to switch this report completely off of the adobe source OR
-- the counts.tsv source, make sure to kick off this flow after the prior aggregation
-- step, such as after the app figures ingest, or the portals daily set agg finishes.


DROP TABLE IF EXISTS dailyReportPreAggregate;

CREATE TEMPORARY TABLE dailyReportPreAggregate AS

select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        STRING(SUM(value)) as value,
        'CHARTER' AS company
from asp_v_venona_counts_daily
where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
AND domain = 'resi'
-- AND grain = 'daily'
AND ((metric IN ('all_equipment_reset_flow_successes|Equipment Reset Flow Successes All|Spectrum.net||',
                 'cancelled_service_appointments|Cancelled Service Appointments|Spectrum.net||',
                 'iva_opens|IVA Opens|Spectrum.net||',
                 'login_attempts|Login Attempts|Spectrum.net||',
                 'rescheduled_service_appointments|Rescheduled Service Appointments|Spectrum.net||',
                 'search_results_clicked|Search Results Clicked|Spectrum.net||',
                 'support_section_page_views|Support Section Page Views|Spectrum.net||',
                 'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|Spectrum.net||',
                 'view_online_statement|View Online Statement|Spectrum.net||'
                )
              --AND company = 'CHARTER' --metrics specific to Charter. Breakout the company so the metrics don't duplicate.
                AND unit = 'instances')
OR
    (metric IN ('site_unique_auth|Site Unique Values Authenticated|Spectrum.net||',
                'webmail_views|Webmail Views|Spectrum.net||',
                'site_unique_pc|Device Count PC|Spectrum.net||',
                'site_unique_mobile|Device Count Mobile|Spectrum.net||',
                'one_time_payment_successes|One Time Payment Successes|Spectrum.net||',
                'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|Spectrum.net||',
                'set_up_auto_payment_successes|Auto Pay Enrollment Successes|Spectrum.net||')
        --  AND company = 'CHARTER'
            AND unit = 'visits')
)
GROUP BY metric,
         date_Denver
order by metric,
         date_Denver;
----
-- Venona bounces
----
INSERT INTO dailyReportPreAggregate
select  date_denver,
        date_add(date_Denver,1) AS ReportDay,
        'venona_bounces_home-unauth' as metric,
        STRING(SUM(bounces)) as value,
        'CHARTER' AS company
from asp_v_bounces_entries
where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
AND domain = 'resi'
AND page_name='home-unauth'
GROUP BY date_Denver
order by date_Denver;
----
-- Venona entries
----
INSERT INTO dailyReportPreAggregate
select  date_denver,
        date_add(date_Denver,1) AS ReportDay,
        'venona_entries_home-unauth'as metric,
        STRING(SUM(entries)) as value,
        'CHARTER' AS company
from asp_v_bounces_entries
where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
AND domain = 'resi'
AND page_name='home-unauth'
GROUP BY date_Denver
order by date_Denver;

-----------
-- Below is the comparison with prior eight weeks average of the same day of the week

DROP TABLE IF EXISTS asp_operational_daily_staging;

CREATE TEMPORARY TABLE asp_operational_daily_staging AS
select  company,
        date_Denver,
        ReportDay,
        metric,
        string(value) as value,
        ROUND(((value/((lag( value, 07) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY company, metric order by date_Denver )) / 8) ) - 1 )
              ,5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from dailyReportPreAggregate
;

----------------------
-- Below is weighted averaging for percentage comparison metrics


DROP TABLE IF EXISTS asp_operational_daily_staging_weighted_averaging_bounces;

CREATE TEMPORARY TABLE asp_operational_daily_staging_weighted_averaging_bounces AS
select  company,
        date_Denver,
        ReportDay,
        metric,
        string(value) as value,
        ROUND(((lag( value, 07) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY company, metric order by date_Denver )) / 8)
              ,5) as AVGPrior8wksSameDayOfWk
from dailyReportPreAggregate
where metric in('venona_bounces_home-unauth', 'bounces_home-unauth')
;

DROP TABLE IF EXISTS asp_operational_daily_staging_weighted_averaging_exits_auth;
CREATE TEMPORARY TABLE asp_operational_daily_staging_weighted_averaging_exits_auth AS
select  company,
        date_Denver,
        ReportDay,
        metric,
        string(value) as value,
        ROUND(((lag( value, 07) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY company, metric order by date_Denver )) / 8)
              ,5) as AVGPrior8wksSameDayOfWk
from dailyReportPreAggregate
where metric in('exits_exit_home-authenticated')
;

DROP TABLE IF EXISTS asp_operational_daily_staging_weighted_averaging_exits_unauth;
CREATE TEMPORARY TABLE asp_operational_daily_staging_weighted_averaging_exits_unauth AS
select  company,
        date_Denver,
        ReportDay,
        metric,
        string(value) as value,
        ROUND(((lag( value, 07) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY company, metric order by date_Denver )) / 8)
              ,5) as AVGPrior8wksSameDayOfWk
from dailyReportPreAggregate
where metric in('exits_exit_home-unauth')
;

DROP TABLE IF EXISTS asp_operational_daily_staging_weighted_averaging_entries;
CREATE TEMPORARY TABLE asp_operational_daily_staging_weighted_averaging_entries AS
select  company,
        date_Denver,
        ReportDay,
        metric,
        string(value) as value,
        ROUND(((lag( value, 07) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 14) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 21) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 28) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 35) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 42) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 49) over (PARTITION BY company, metric order by date_Denver ) +
                        lag( value, 56) over (PARTITION BY company, metric order by date_Denver )) / 8)
              ,5) as AVGPrior8wksSameDayOfWk
from dailyReportPreAggregate
where metric in('venona_entries_home-unauth','entries_home-unauth','entries_exit_home-authenticated','entries_exit_home-unauth')
;

-- Calculate bounces and bounce difference from weighted average of past 8 same day of week
INSERT INTO asp_operational_daily_staging
select  b.company,
        b.date_Denver,
        b.ReportDay,
        'bounce_rate_home_unauth' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_bounces b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             WHERE metric = 'entries_home-unauth') e
  ON  b.date_Denver = e.date_Denver
  AND b.company = e.company
;

INSERT INTO asp_operational_daily_staging
select  b.company,
        b.date_Denver,
        b.ReportDay,
        'venona_bounce_rate_home_unauth' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_bounces b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             WHERE metric = 'venona_entries_home-unauth') e
  ON  b.date_Denver = e.date_Denver
  AND b.company = e.company
;

INSERT INTO asp_operational_daily_staging
select  b.company,
        b.date_Denver,
        b.ReportDay,
        'exit_rate_resi_auth' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_exits_auth b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             WHERE metric = 'entries_exit_home-authenticated') e
  ON  b.date_Denver = e.date_Denver
  AND b.company = e.company
;

INSERT INTO asp_operational_daily_staging
select  b.company,
        b.date_Denver,
        b.ReportDay,
        'exit_rate_resi_unauth' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_exits_unauth b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             WHERE metric = 'entries_exit_home-unauth') e
  ON  b.date_Denver = e.date_Denver
  AND b.company = e.company
;
-- Metric Tableau name Lookup
DROP TABLE IF EXISTS ${env:LKP_db}.asp_daily_metrics PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.asp_daily_metrics
( hive_metric STRING,
  tableau_metric STRING
);

-- Change names of metrics here to be reflected in Tableau

INSERT INTO TABLE ${env:LKP_db}.asp_daily_metrics VALUES
    ('bounce_rate_home_unauth',                                                                       'Bounce Rate on Unauth Home Page'), --chtr
    ('exit_rate_resi_auth',                                                                           'Exit Rate on Resi auth Home Page'), --chtr
    ('exit_rate_resi_unauth',                                                                         'Exit Rate on Resi Unauth Home Page'), --chtr
    ('login_attempts_adobe' ,                                                                         'Login Attempts'), --chtr
    ('total_login_attempts' ,                                                                         'Login Attempts'), --bhn, twc
    ('authenticated_visits' ,                                                                         'Authenticated Visits'), --chtr
    ('webmail_visits' ,                                                                               'Webmail Visits'), --chtr
    ('statement_views' ,                                                                              'Statement Views'), --chtr
    ('view_online_statement_count' ,                                                                  'Statement Views'),--bhn, twc
    ('one_time_payments_updated' ,                                                                    'One Time Payments'),--chtr
    ('one_time_payments_confirm_count' ,                                                              'One Time Payments'), --bhn, twc
    ('one_time_payment_with_auto_pay_updated' ,                                                       'One Time Payment w/ AutoPay Enrollment'), --chtr
    ('one_time_payments_with_autopay_confirm_count' ,                                                 'One Time Payment w/ AutoPay Enrollment'),--bhn, twc
    ('auto_pay_enrollment_updated' ,                                                                  'AutoPay Enrollment'), --chtr
    ('successful_autopay_confirm_count' ,                                                             'AutoPay Enrollment'),--bhn, twc
    ('refresh_requests_count' ,                                                                       'Refresh STB'), --chtr, twc
    ('modem_router_resets' ,                                                                          'Equipment Reboot Completions'), --chtr, twc
    ('new_ids_incl_sub_accts' ,                                                                       'New Identities Created incl. sub accounts'), --chtr, twc
    ('rescheduled_service_appoint_count' ,                                                            'Reschedules'), --chtr, twc
    ('cancelled_service_appoint_count' ,                                                              'Cancels'), --chtr, twc
    ('support_page_views_count' ,                                                                     'Support Page Views'), --chtr, twc, bhn
    ('combined_credential_recoveries',                                                                'Combined Credential Recoveries'), --chtr
    ('successful_username_recovery_count_all',                                                        'Combined Credential Recoveries'), --twc
    ('successful_reset_password_count_all',                                                           'Combined Credential Recoveries'), --twc
    ('search_results_clicked',                                                                        'Search Results Clicked'), --chtr
    ('iva_opens',                                                                                     'IVA Opens'), --chtr
    ('ask_charter_requests_count',                                                                    'IVA Opens'), --twc
    ('count_os_is_iOSAndroid',                                                                        'Visits either iOS or Android'), --chtr
    ('count_os_not_iOSAndroid',                                                                       'Visits neither iOS nor Android'), --chtr
    ('iva_opens|IVA Opens|Spectrum.net||',                                                            'IVA Opens'),
    ('login_attempts|Login Attempts|Spectrum.net||',                                                  'Login Attempts'),
    ('one_time_payment_successes|One Time Payment Successes|Spectrum.net||',                          'One Time Payments'),
    ('otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|Spectrum.net||', 'One Time Payment w/ AutoPay Enrollment'),
    ('rescheduled_service_appointments|Rescheduled Service Appointments|Spectrum.net||',              'Reschedules'),
    ('search_results_clicked|Search Results Clicked|Spectrum.net||',                                  'Search Results Clicked'),
    ('support_section_page_views|Support Section Page Views|Spectrum.net||',                          'Support Page Views'),
    ('total_new_ids_created_successes|Total New IDs Created Successes|Spectrum.net||',                'New Identities Created incl. sub accounts'),
    ('tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|Spectrum.net||',                  'Refresh STB'),
    ('site_unique_auth|Site Unique Values Authenticated|Spectrum.net||',                              'Authenticated Visits'),
    ('webmail_views|Webmail Views|Spectrum.net||',                                                    'Webmail Visits'),
    ('os_ios_or_android|Operating System - iOS or Android|Spectrum.net||',                            'Visits either iOS or Android'), --chtr
    ('os_not_ios_or_android|Operating System - Not iOS or Android|Spectrum.net||',                    'Visits neither iOS nor Android'), --chtr
--Adding quantum/venona-ready metrics
    ('venona_bounce_rate_home_unauth',                                                                'Venona Bounce Rate on Unauth Home Page'),
    ('view_online_statement|View Online Statement|Spectrum.net||',                                    'View Online Statement'),
    ('set_up_auto_payment_successes|Auto Pay Enrollment Successes|Spectrum.net||',                    'Auto Pay Enrollment Successes'),
    ('cancelled_service_appointments|Cancelled Service Appointments|Spectrum.net||',                  'Cancelled Service Appointments'),
    ('tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|Spectrum.net||',            'Equipment Reset Flow Starts TV'),
    ('all_equipment_reset_flow_successes|Equipment Reset Flow Successes All|Spectrum.net||',          'Equipment Reset Flow Successes All'),
    ('site_unique_pc|Device Count PC|Spectrum.net||',                                                 'Desktop/Laptop'),
    ('site_unique_mobile|Device Count Mobile|Spectrum.net||',                                         'Mobile Devices')

;


-- Insert in to daily table

INSERT OVERWRITE TABLE asp_operational_daily PARTITION(domain, date_denver)
SELECt DISTINCT reportday,
                COALESCE(tableau_metric, metric) AS metric,
                STRING(value) as value,
                vsavgprior8wkssamedayofwk,
                review_comment,
                null as additional_comment,
                CASE
                     WHEN company = 'L-BHN'  THEN 'Residential BHN'
                     WHEN company = 'L-TWC'  THEN 'Residential TWC'
                     WHEN company = 'L-CHTR' THEN 'Spectrum.net'
                     WHEN company = 'CHARTER' THEN 'Spectrum.net - Venona'
                     ELSE 'UNDEFINED'
                END as domain,
                date_denver
FROM asp_operational_daily_staging
LEFT OUTER JOIN ${env:LKP_db}.asp_daily_metrics
ON metric = hive_metric
;

-- net visits by page load, auth and unauth

SELECt "
        Atomic metrics have been extracted.

        Now extracting page load time measurements.

        ";

-- page load time -- page views

--drop table if exists asp_operational_daily_page_views_by_page_load_time;
CREATE TABLE if not exists asp_operational_daily_page_views_by_page_load_time
  (   ReportDay string,
      page_name string,
      hot_pg_load_sec string,
      cold_pg_load_sec string,
      page_views int
  )
  PARTITIONED BY (domain STRING, date_Denver STRING )
;

-- page load time -- page views -- venona

INSERT OVERWRITE TABLE asp_operational_daily_page_views_by_page_load_time PARTITION(domain, date_Denver)

SELECT  date_add(date_denver,1) AS ReportDay,
        auth_unauth_page as page_name,
        hot_pg_load_sec as hot_pg_load_sec,
        cold_pg_load_sec as cold_pg_load_sec,
        SUM(count_page_views) as page_views,
        'Spectrum.net - Venona' as domain,
        date_denver
FROM  (SELECT
          date_denver,
          CASE WHEN page_name = 'home-authenticated' THEN 'home-authenticated'
               WHEN page_name = 'home-unauth' THEN 'home-unauth'
               ELSE 'other'
          END as auth_unauth_page,
          count_page_views,
          hot_pg_load_sec,
          cold_pg_load_sec
        FROM asp_v_page_render_time_seconds_page_views_visits
        WHERE date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
            AND lower(page_name) RLIKE ".*home.*auth.*"
            AND domain = 'resi'
      ) dictionary
GROUP BY  date_denver,
          auth_unauth_page,
          hot_pg_load_sec,
          cold_pg_load_sec
;
