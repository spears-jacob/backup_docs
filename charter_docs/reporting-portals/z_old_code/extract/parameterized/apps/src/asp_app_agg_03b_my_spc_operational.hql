USE ${env:ENVIRONMENT};

SET AppFiguresLag=3;
SELECT "\n\nThe AppFiguresLag variable is used to set how long to lag the app figures counts and reviews,\n\n now set to ${hiveconf:AppFiguresLag} days.\n\n";

SET MetricLag=61;
SELECT "\n\nThe MetricLag variable is used to set how long to lag the metrics, to allow for past X week day average,\n\n now set to ${hiveconf:MetricLag} days.\n\n";


DROP TABLE IF EXISTS dailyReportPreAggregate;
-- need to sum these visits but need to manually create the company column so it doesn't split out across the three.
CREATE TEMPORARY TABLE dailyReportPreAggregate AS
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        'Authenticated Visits' as metric,
        'CHTR' as company,
        STRING(SUM(value)) as value
from asp_app_daily_agg_my_spc_raw
where metric IN ('Visits','Authenticated Sessions / Visits')
  and company in ('BHN','CHTR','TWCC')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
        'CHTR'
order by date_Denver
;

--TWC visits are users who went directly to the TWC app
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        'Authenticated Visits' as metric,
        'TWCC' as company,
        SUM(value) as value
from asp_app_daily_agg_my_twc_raw
where metric IN ('Visits')
  and company in ('TWCC')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         'TWCC'
order by date_Denver
;

--Venona authenticated visits
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        'Authenticated Visits' as metric,
        'Venona_CHTR' as company,
        SUM(value) as value
from asp_venona_counts_daily
where metric IN ('site_unique|Site Unique Values|MySpectrum||')
  AND unit IN ('visits')
  AND company IN ('BH','CHARTER','TWC')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
  'Venona_CHTR'
order by date_Denver
;

---- Support Page Views query for Charter and BHN
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        'CHTR' as company,
        SUM(value) as value
from asp_app_daily_agg_my_spc_raw
where metric IN('Support Page Views')
  and company in ('BHN','CHTR')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         metric,
         'CHTR'
order by date_Denver
;

-- TWC Support Page Views for TWC
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        'TWCC' as company,
        SUM(value) as value
from asp_app_daily_agg_my_twc_raw
where metric IN('Support Page Views')
  and company in ('TWCC')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         metric,
         'TWCC'
order by date_Denver
;

---- Charter Metrics. Sometimes these are a combination of BHN, TWC, CHTR, and UNAUTH from SPC raw table
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        'CHTR' as company,
        SUM(value) as value
from asp_app_daily_agg_my_spc_raw
where metric IN('Call Support',
                'View Statement',
                'One Time Payment Success',
                'AutoPay Enroll Success',
                'Refresh Digital Receiver Requests',
                'Modem Router Resets',
                'Rescheduled Appointments',
                'Cancelled Appointments',
                'Crashes iOS',
                'Crashes Android',
                'Launches iOS',
                'Launches Android')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         metric,
         'CHTR'
order by metric,
         date_Denver
;

-- Venona equipment reboot completions
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        'Equipment Reboot Completions' AS metric,
        'Venona_CHTR' as company,
        SUM(value) as value
from asp_venona_counts_daily
where metric IN('internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum||',
                'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum||')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
  AND unit IN ('instances')
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
from asp_venona_counts_daily
where metric IN('support_page_views|Support Page Views|MySpectrum||',
                'call_support|Call Support|MySpectrum||',
                'view_statement|View Statement|MySpectrum||',
                'one_time_payment_success|One Time Payment Success|MySpectrum||',
                'autopay_enroll_success|AutoPay Enroll Success|MySpectrum|legacy_app|',
                'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||',
                'rescheduled_appointments|Rescheduled Appointments|MySpectrum||',
                'cancelled_appointments|Cancelled Appointments|MySpectrum||',
                'session_start|Session Starts|MySpectrum||')
  AND unit IN ('instances')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         metric,
         'Venona_CHTR'
order by metric,
         date_Denver
;

------ TWC Metrics from TWC raw table
INSERT INTO dailyReportPreAggregate
select  date_Denver,
        date_add(date_Denver,1) AS ReportDay,
        metric,
        'TWCC' as company,
        SUM(value) as value
from asp_app_daily_agg_my_twc_raw
where metric IN('Call Support',
                'View Statement',
                'One Time Payment Success',
                'AutoPay Enroll Success',
                'Refresh Digital Receiver Requests',
                'Modem Router Resets',
                'Rescheduled Appointments',
                'Cancelled Appointments')
  ANd date_Denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:MetricLag})
GROUP BY date_Denver,
         metric,
         'TWCC'
order by metric,
         date_Denver
;

--- iOS crash rate for Charter. Divides crashes by launches
INSERT INTO dailyReportPreAggregate
select  d.date_Denver,
        d.ReportDay,
        'Crash Rate iOS' as metric,
        'CHTR' as company,
        ROUND(n.value/d.value,5) as value
from dailyReportPreAggregate d
INNER JOIN (select  date_Denver,
                    value
            from dailyReportPreAggregate
            where metric ='Crashes iOS') n
ON d.date_Denver = n.date_Denver
where metric ='Launches iOS'
;

--- Android crash rate for Charter. Divides crashes by launches
INSERT INTO dailyReportPreAggregate
select  d.date_Denver,
        d.ReportDay,
        'Crash Rate Android' as metric,
        'CHTR' as company,
        ROUND(n.value/d.value,5) as value
from dailyReportPreAggregate d
INNER JOIN (select  date_Denver,
                    value
            from dailyReportPreAggregate
            where metric ='Crashes Android') n
ON d.date_Denver = n.date_Denver
where metric ='Launches Android'
;

-- App Downloads and Updates (added company on 2/27 to separate MySpectrum App and My TWC)
INSERT INTO dailyReportPreAggregate
select DISTINCT  a.date_Denver,
                 date_add(a.date_Denver,${hiveconf:AppFiguresLag}) AS ReportDay,
                 a.metric,
                 a.company,
                 a.value
from  asp_app_daily_app_figures  a
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

-- Adds App reviews. Added company to select and join criteria on 2/27 so reviews are attributed to correct app.
INSERT INTO asp_operational_daily_staging
SELECt DISTINCT a.company,
                a.date_Denver,
                date_add(a.date_Denver,${hiveconf:AppFiguresLag}) AS ReportDay,
                'App Review' as metric,
                a.stars as value,
                null as VsAVGPrior8wksSameDayOfWk,
                a.review as review_comment,
                a.platform AS additional_comment
FROM asp_app_daily_app_figures_reviews a
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
