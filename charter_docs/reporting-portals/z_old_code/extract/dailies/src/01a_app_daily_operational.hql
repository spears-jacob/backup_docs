USE ${env:ENVIRONMENT};

SET AppFiguresLag=3;
SELECT "\n\nThe AppFiguresLag variable is used to set how long to lag the app figures counts and reviews,\n\n now set to ${hiveconf:AppFiguresLag} days.\n\n";

set MetricLag=91;
SELECt "
        The MetricLag variable is used to set how long to lag the metrics, to allow for past X week day average:

        ${hiveconf:MetricLag}


        Now extracting atomic (single-valued) measurements which are counts.

        ";


DROP TABLE IF EXISTS dailyReportPreAggregate;

--Venona authenticated visits
CREATE TEMPORARY TABLE dailyReportPreAggregate AS
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        'Authenticated Visits' as metric,
        'Venona_CHTR' as company,
        SUM(value) as value
from asp_v_venona_counts_daily
where metric IN ('site_unique|Site Unique Values|MySpectrum||')
  --ANd grain = 'daily'
  AND unit IN ('visits')
  AND company IN ('BH','CHARTER','TWC')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
  'Venona_CHTR'
order by date_Denver
;

-- Venona equipment reboot completions
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        'Equipment Reboot Completions' AS metric,
        'Venona_CHTR' as company,
        SUM(value) as value
from asp_v_venona_counts
where metric IN('internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum||',
                'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum||')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
  AND unit IN ('instances')
  ANd grain = 'daily'
GROUP BY date_Denver,
         'Equipment Reboot Completions',
         'Venona_CHTR'
order by metric,
         date_Denver
;

---- Venona Charter Metrics. Sometimes these are a combination of BHN, TWC, CHTR, and UNAUTH from SPC raw table
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        'Venona_CHTR' as company,
        SUM(value) as value
from asp_v_venona_counts_daily
where metric IN('support_page_views|Support Page Views|MySpectrum||',
                'call_support|Call Support|MySpectrum||',
                'view_statement|View Statement|MySpectrum||',
                'one_time_payment_success|One Time Payment Success|MySpectrum||',
                'autopay_enroll_success|AutoPay Enroll Success|MySpectrum|legacy_app|',
                'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||',
                'rescheduled_appointments|Rescheduled Appointments|MySpectrum||',
                'cancelled_appointments|Cancelled Appointments|MySpectrum||',
                'session_start|Session Starts|MySpectrum||')
-- ANd grain = 'daily'
  AND unit IN ('instances')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         metric,
         'Venona_CHTR'
order by metric,
         date_Denver
;

-- App Downloads and Updates
INSERT INTO dailyReportPreAggregate
select DISTINCT  a.date_Denver,
                 date_add(a.date_Denver,${hiveconf:AppFiguresLag}) AS ReportDay,
                 a.metric,
                 a.company,
                 a.value
from  asp_v_app_daily_app_figures  a
INNER JOIN dailyReportPreAggregate d
on a.date_Denver = d.date_Denver
WHERE a.company = 'CHTR'
and d.company like '%CHTR%'
;

-- Below is the comparison with prior four weeks average of the same day of the week
DROP TABLE IF EXISTS asp_operational_daily_staging;

CREATE TEMPORARY TABLE asp_operational_daily_staging AS
select
        company,
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
        'Not a review metric' as review_comment,
        'No additional comments' AS additional_comment
from dailyReportPreAggregate
;

-- Adds App reviews
INSERT INTO asp_operational_daily_staging
SELECt DISTINCT a.company,
                a.date_Denver,
                date_add(a.date_Denver,${hiveconf:AppFiguresLag}) AS ReportDay,
                'App Review' as metric,
                a.stars as value,
                null as VsAVGPrior8wksSameDayOfWk,
                a.review as review_comment,
                a.platform AS additional_comment
FROM asp_v_app_daily_app_figures_reviews a
INNER JOIN dailyReportPreAggregate d
on a.date_Denver = d.date_Denver
and a.company = 'CHTR'
and d.company like '%CHTR%'
;

-- Metric Tableau name Lookup
DROP TABLE IF EXISTS ${env:LKP_db}.asp_daily_metrics PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.asp_app_daily_metrics
( hive_metric STRING,
  tableau_metric STRING
)
;

-- Change names of metrics here to be reflected in Tableau

INSERT INTO TABLE ${env:LKP_db}.asp_app_daily_metrics VALUES
('View Statement',                                                                    'Statement Views'),
('One Time Payment Success',                                                          'One Time Payments'),
('AutoPay Enroll Success',                                                            'AutoPay Enrollments'),
('Refresh Digital Receiver Requests',                                                 'Refresh STB'),
('Modem Router Resets',                                                               'Equipment Reboot Completions'),
('Rescheduled Appointments',                                                          'Reschedules'),
('Cancelled Appointments',                                                            'Cancels'),
('view_statement|View Statement|MySpectrum||',                                        'Statement Views'),
('one_time_payment_success|One Time Payment Success|MySpectrum||',                    'One Time Payments'),
('autopay_enroll_success|AutoPay Enroll Success|MySpectrum|legacy_app|',              'AutoPay Enrollments'),
('tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||',        'Refresh STB'),
('rescheduled_appointments|Rescheduled Appointments|MySpectrum||',                    'Reschedules'),
('cancelled_appointments|Cancelled Appointments|MySpectrum||',                        'Cancels'),
('support_page_views|Support Page Views|MySpectrum||',                                'Support Page Views'),
('call_support|Call Support|MySpectrum||',                                            'Call Support'),
('session_start|Session Starts|MySpectrum||',                                         'Session Starts')
;
--

INSERT OVERWRITE TABLE asp_operational_daily PARTITION(domain, date_denver)
SELECt DISTINCT reportday,
                COALESCE(tableau_metric, metric) AS metric,
                STRING(value) as value,
                vsavgprior8wkssamedayofwk,
                review_comment,
                additional_comment,
                CASE
                    WHEN company = 'CHTR' then 'My Spectrum App' --app in its entirety includes all BHN and will include TWC
                    WHEN company = 'Venona_CHTR' then 'My Spectrum App - Venona' --app in its entirety includes all BHN and will include TWC
                    WHEN company = 'TWCC' then 'My TWC'
                    ELSE 'UNDEFINED'
                END as domain,
                date_denver
FROM asp_operational_daily_staging
LEFT OUTER JOIN ${env:LKP_db}.asp_app_daily_metrics
ON metric = hive_metric
;
