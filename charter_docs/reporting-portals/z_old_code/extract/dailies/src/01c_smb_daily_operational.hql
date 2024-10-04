USE ${env:ENVIRONMENT};

set MetricLag=91;

SELECt "
        The MetricLag variable is used to set how long to lag the metrics, to allow for past X week day average:

        ${hiveconf:MetricLag}


        Now extracting atomic (single-valued) measurements which are counts.

        ";

DROP TABLE IF EXISTS dailyReportPreAggregate;

CREATE TEMPORARY TABLE dailyReportPreAggregate AS
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        STRING(SUM(value)) as value,
        'CHARTER' as company
from asp_v_venona_counts_daily
where (date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag}))
AND domain = 'sb'
-- ANd grain = 'daily'
AND ((metric IN ('cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net||',
                 'login_attempts_adobe|Login Attempts|SpectrumBusiness.net||',
                 'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||',
                 'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net||',
                 'search_results_clicked|Search Results Clicked|SpectrumBusiness.net||',
                 'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||',
                 'support_page_views|Support Section Page Views|SpectrumBusiness.net||',
                 'view_online_statement|View Online Statement|SpectrumBusiness.net||'
                )
        --AND company = 'CHARTER' --metrics specific to Charter. Breakout the company so the metrics don't duplicate.
            AND unit = 'instances')
        OR
        (metric IN ('site_unique_auth|Site Unique Values Authenticated|SpectrumBusiness.net||',
                    'site_unique_pc|Device Count PC|SpectrumBusiness.net||',
                    'site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||',
                    'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net||',
                    'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net||',
                    'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net||',
                    'site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||',
                    'site_unique_pc|Device Count PC|SpectrumBusiness.net||')
              --AND company = 'CHARTER'
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
AND domain = 'smb'
AND page_name='homeUnauth'
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
AND domain = 'smb'
AND page_name='homeUnauth'
GROUP BY date_Denver
order by date_Denver;


----------------------
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
where metric in('venona_bounces_home-unauth','bounces_Login')
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
where metric in('exits_exit_overview')
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
where metric in('exits_exit_login')
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
where metric in('venona_entries_home-unauth','entries_Login','entries_exit_overview','entries_exit_login')
;

-- Calculate bounces and bounce difference from weighted average of past 8 same day of week

INSERT INTO asp_operational_daily_staging
select  b.company,
        b.date_Denver,
        b.ReportDay,
        'bounce_rate_Login' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_bounces b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             WHERE metric = 'entries_Login') e
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
        'exit_rate_sb_auth' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_exits_auth b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             where metric = 'entries_exit_overview') e
  ON  b.date_Denver = e.date_Denver
  AND b.company = e.company
;

INSERT INTO asp_operational_daily_staging
select  b.company,
        b.date_Denver,
        b.ReportDay,
        'exit_rate_sb_unauth' as metric,
        string(ROUND(b.value/e.value,5)) as value,
        ROUND((b.AVGPrior8wksSameDayOfWk/e.AVGPrior8wksSameDayOfWk)-(b.value/e.value),5) as VsAVGPrior8wksSameDayOfWk,
        'Not a review metric' as review_comment
from asp_operational_daily_staging_weighted_averaging_exits_unauth b
INNER JOIN (select *
              FROM asp_operational_daily_staging_weighted_averaging_entries
             WHERE metric = 'entries_exit_login') e
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
    ('bounce_rate_Login',                                                                                             'Bounce Rate on Unauth Home Page'), --chtr
    ('login_attempts_adobe' ,                                                                                         'Login Attempts'), --chtr
    ('total_login_attempts' ,                                                                                         'Login Attempts'), --twc, bhn
    ('authenticated_visits' ,                                                                                         'Authenticated Visits'), --chtr
    ('view_statement' ,                                                                                               'Statement Views'), --chtr
    ('online_statement_views' ,                                                                                       'Statement Views'), --bhn, twc
    ('one_time_payment_updated' ,                                                                                     'One Time Payments'), --chtr
    ('one_time_payments' ,                                                                                            'One Time Payments'), --bhn, twc
    ('one_time_payment_w_autopay_updated',                                                                            'One Time Payment w/ AutoPay Enrollment'), --chtr
    ('auto_payment_confirm_updated' ,                                                                                 'AutoPay Enrollment'),--chtr
    ('auto_pay_setup_successes' ,                                                                                     'AutoPay Enrollment'), --bhn, twc
    ('reschedules' ,                                                                                                  'Reschedules'),--chtr
    ('rescheduled_service_appointments' ,                                                                             'Reschedules'), --twc
    ('cancels' ,                                                                                                      'Cancels'),--chtr
    ('cancelled_service_appointments' ,                                                                               'Cancels'), --twc
    ('new_admin_accounts_created' ,                                                                                   'New Admin Accounts Created'),--chtr
    ('sub_acct_created',                                                                                              'New Sub Accounts Created'),--chtr
    ('total_new_sub_users_created',                                                                                   'New Sub Accounts Created'), --twc
    ('support_pvs' ,                                                                                                  'Support Page Views'),--chtr
    ('support_page_views' ,                                                                                           'Support Page Views'), --twc
    ('search_results_clicked',                                                                                        'Search Results Clicked'),--chtr
    ('count_os_is_iOSAndroid',                                                                                        'Visits either iOS or Android'),--chtr
    ('count_os_not_iOSAndroid',                                                                                       'Visits neither iOS nor Android'),--chtr
    ('exit_rate_sb_auth',                                                                                             'Exit Rate on SB auth Home Page'),--chtr
    ('exit_rate_sb_unauth',                                                                                           'Exit Rate on SB unauth Home Page'),--chtr
    ('site_unique_auth|Site Unique Values Authenticated|SpectrumBusiness.net||',                                      'Authenticated Visits'),
    ('auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net||',                                 'AutoPay Enrollment'),
    ('cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net||',                          'Cancels'),
    ('os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net||',                                    'Visits either iOS or Android'),
    ('os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net||',                            'Visits neither iOS nor Android'),
    ('login_attempts_adobe|Login Attempts|SpectrumBusiness.net||',                                                    'Login Attempts'),
    ('new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||',                                  'New Admin Accounts Created'),
    ('one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net||',                                    'One Time Payments'),
    ('otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net||',         'One Time Payment w/ AutoPay Enrollment'),
    ('rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net||',                      'Reschedules'),
    ('search_results_clicked|Search Results Clicked|SpectrumBusiness.net||',                                          'Search Results Clicked'),
    ('sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||',                                              'New Sub Accounts Created'),
    ('support_page_views|Support Section Page Views|SpectrumBusiness.net||',                                          'Support Page Views'),
    ('view_online_statement|View Online Statement|SpectrumBusiness.net||',                                            'Statement Views'),
    ('site_unique_pc|Device Count PC|SpectrumBusiness.net||',                                                         'Desktop/Laptop'),
    ('site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||',                                                 'Mobile Devices'),
    ('venona_bounce_rate_home_unauth',                                                                                'Venona Bounce Rate on Unauth Home Page'),
    ('site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||',                                                 'Device Count Mobile'),
    ('site_unique_pc|Device Count PC|SpectrumBusiness.net||',                                                         'Device Count PC')

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
                    WHEN company = 'L-BHN' then 'BHN Business'
                    WHEN company = 'L-TWC' then 'TWC Business'
                    WHEN company = 'L-CHTR' then 'SB.net'
                    WHEN company = 'CHARTER' THEN 'SB.net - Venona'
                    ELSE 'UNDEFINED'
                END as domain,
                date_denver
FROM asp_operational_daily_staging
LEFT OUTER JOIN ${env:LKP_db}.asp_daily_metrics
ON metric = hive_metric
;

-- sbnet visits by page load, auth and unauth

SELECt "
        Atomic metrics have been extracted.

        Now extracting page load time measurements.

        ";


-- page load time -- page views -- venona

INSERT OVERWRITE TABLE asp_operational_daily_page_views_by_page_load_time PARTITION(domain, date_Denver)

SELECT  date_add(date_denver,1) AS ReportDay,
        auth_unauth_page as page_name,
        hot_pg_load_sec as hot_pg_load_sec,
        cold_pg_load_sec as cold_pg_load_sec,
        SUM(count_page_views) as page_views,
        'SB.net - Venona' as domain,
        date_denver
FROM  (SELECT
          date_denver,
          CASE WHEN page_name = 'homeAuth' THEN 'home-authenticated'
               WHEN page_name IN('homeUnauth','login','Login') THEN 'home-unauth'
               ELSE 'other'
          END as auth_unauth_page,
          count_page_views,
          hot_pg_load_sec,
          cold_pg_load_sec
        FROM asp_v_page_render_time_seconds_page_views_visits
        WHERE date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
            AND page_name in ('homeAuth','homeUnauth','login','Login')
            AND domain = 'smb'
      ) dictionary
GROUP BY  date_denver,
          auth_unauth_page,
          hot_pg_load_sec,
          cold_pg_load_sec
;

SELECt "

        Page load time measurements have been extracted.


        ";
