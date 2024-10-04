USE ${env:ENVIRONMENT};

set hive.vectorized.execution.enabled = false;

-- ALL_SPECNET_SMB = NB_SPECNET_SMB + NB_PGLOADTIME
-- ALL_MYSPECTRUM = NB_MYSPECTRUM + NB_APP
-- ALL_TOTAL = ALL_SMBRRESI*2 + ALL_MYSPECTRUM

SET NB_SPECNET_SMB=22;
SET NB_PGLOADTIME=1;
SET NB_MYSPECTRUM=22;
SET NB_APP=5;
SET ALL_SPECNET_SMB=23;
SET ALL_MYSPECTRUM=27;
SET ALL_TOTAL=73;

DROP TABLE IF EXISTS asp_daily_report_data_summary_staging;

CREATE TEMPORARY TABLE asp_daily_report_data_summary_staging AS
SELECT reportday,
       domain,
       date_denver as date_denver,
       'daily_metric' as metric_type,
       ${hiveconf:NB_SPECNET_SMB} as metric_count_threshold,
       count(distinct metric) as metric_count,
       CASE WHEN count(distinct metric)= ${hiveconf:NB_SPECNET_SMB} THEN true else false end as status
  from asp_daily_report_data
 WHERE domain in ('specnet','smb')
   AND value is not null
   and metric in (
               'Bounce Rate on Unauth Home Page',
               'Login Attempts',
               'Login Failures',
               'Login Successes',
               'Support Page Views',
               --'IVA Opens',
               'View Online Statments',
               'OTP Starts',
               'OTP Failures',
               'OTP Successes',
               --'OTP w/AP Successes',
               'Set Up AP Starts',
               'Set Up AP Failures',
               'Set Up AP Successes',
               'Edit SSID Confirmations',
               'Internet Equipment Reset Starts',
               'Internet Reset Failures',
               'Internet Reset Successes',
               'TV Reset Starts',
               'TV Reset Fails',
               'TV Reset Successes',
               'Voice Reset Starts',
               'Voice Reset Failures',
               'Voice Reset Successes'
               --,'Cancelled Appointments',
               --'Rescheduled Appointments'
             )
 group by domain,
          date_denver,
          reportday;

insert into asp_daily_report_data_summary_staging
SELECT reportday,
       domain,
       date_denver as date_denver,
       'daily_metric' as metric_type,
       ${hiveconf:NB_MYSPECTRUM} as metric_count_threshold,
       count(distinct metric) as metric_count,
       CASE WHEN count(distinct metric)= ${hiveconf:NB_MYSPECTRUM} THEN true else false end as status
  from asp_daily_report_data
 WHERE domain in ('myspectrum')
   AND value is not null
   and metric in (
             'Login Attempts',
             --'Login Failures',
             'Login Successes',
             'Support Page Views',
             'Call Support',
             'View Online Statments',
             'OTP Starts',
             'OTP Failures',
             'OTP Successes',
             --'OTP w/AP Successes',
             'Set Up AP Starts',
             'Set Up AP Failures',
             'Set Up AP Successes',
             'Edit SSID Confirmations',
             'Internet Equipment Reset Starts',
             'Internet Reset Failures',
             'Internet Reset Successes',
             'TV Reset Starts',
             'TV Reset Fails',
             'TV Reset Successes',
             'Voice Reset Starts',
             'Voice Reset Failures',
             'Voice Reset Successes',
             --,'Cancelled Appointments',
             --'Rescheduled Appointments'
             'WiFi Equipment Pauses'
           )
group by domain,
        date_denver,
        reportday;

insert into asp_daily_report_data_summary_staging
SELECT reportday,
       domain,
       date_denver as date_denver,
       'app_metric' as metric_type,
       ${hiveconf:NB_APP} as metric_count_threshold,
       count(distinct metric) as metric_count,
       CASE WHEN count(distinct metric)= ${hiveconf:NB_APP} THEN true else false end as status
  from asp_daily_report_data
 WHERE domain in ('myspectrum')
   AND value is not null
   and metric in (
             'App Downloads Android',
             'App Downloads iOS',
             'App Updates Android',
             'App Updates iOS',
             'App Review'
           )
group by domain,
        date_denver,
        reportday;

insert into asp_daily_report_data_summary_staging
select date_add(date_denver,1) AS ReportDay,
       CASE WHEN domain='Spectrum.net' THEN 'specnet'
            else domain end as domain,
       date_denver,
       'page_load_time' as metric_type,
       ${hiveconf:NB_PGLOADTIME} as metric_count_threshold,
       count(distinct pg_load_type) as metric_count,
       CASE WHEN count(distinct pg_load_type)= ${hiveconf:NB_PGLOADTIME} THEN true else false end as status
  from asp_v_hourly_page_load_tenths_quantum
 where pg_load_type in ('Cold Fully Loaded')
   AND domain !='idm'
   and instances is not null
 group by domain,
          date_denver,
          date_add(date_denver,1) ;

insert into asp_daily_report_data_summary_staging
select reportday,
       domain,
       date_denver,
       'all_metric' as metric_type,
       ${hiveconf:ALL_SPECNET_SMB} as metric_count_threshold,
       sum(metric_count) as metric_count,
       CASE WHEN sum(metric_count)= ${hiveconf:ALL_SPECNET_SMB} THEN true else false end as status
  from asp_daily_report_data_summary_staging
 where domain in ('specnet','smb')
 group by reportday,
          domain,
          date_denver;

insert into asp_daily_report_data_summary_staging
select reportday,
       domain,
       DATE_SUB(to_date(reportday),1) as data_denver,
       'all_metric' as metric_type,
       ${hiveconf:ALL_MYSPECTRUM} as metric_count_threshold,
       sum(metric_count) as metric_count,
       CASE WHEN sum(metric_count)= ${hiveconf:ALL_MYSPECTRUM} THEN true else false end as status
  from asp_daily_report_data_summary_staging
 where domain in ('myspectrum')
 group by reportday,
          domain;

insert into asp_daily_report_data_summary_staging
select reportday,
       'all_domain' as domain,
        DATE_SUB(to_date(reportday),1) as data_denver,
       'all_metric' as metric_type,
       ${hiveconf:ALL_TOTAL} as metric_count_threshold,
       sum(metric_count) as metric_count,
       CASE WHEN sum(metric_count)= ${hiveconf:ALL_TOTAL} THEN true else false end as status
  from asp_daily_report_data_summary_staging
 where metric_type !='all_metric'
 group by reportday;

INSERT OVERWRITE TABLE asp_daily_report_data_summary PARTITION(date_denver)
SELECt DISTINCT reportday,
                domain,
                metric_type,
                metric_count_threshold,
                metric_count,
                status,
                date_denver
FROM asp_daily_report_data_summary_staging
;
--insert IDM daily metric
