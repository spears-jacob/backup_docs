USE ${env:ENVIRONMENT};

--- Footprint/MSO is indistinguishable until authentication occurs.
--- At this time, only authenticated counts are being reported (line 106).
--- Parameters for testing fiscal_monthly cadence:
--- export CADENCE=fiscal_monthly; export ymd=year_fiscal_month_Denver;

SELECT '\n\nNow running asp_app_agg_02d_my_spc...\n\n';


CREATE TEMPORARY TABLE ${env:TMP_db}.asp_app_agg_my_spc_long_metric_names AS
  SELECT
      company,
      metric,
      value,
      unit,
      ${env:ymd}
  FROM  asp_venona_counts_${env:CADENCE}
  WHERE  source_table = 'asp_v_venona_events_portals_msa'
  AND    ${env:ymd} >= '2018-08'
  and    domain = 'app'
  AND ( (unit = 'devices'   AND metric ='site_unique|Site Unique Values|MySpectrum||'
        ) OR
        (unit = 'instances' AND metric
           IN('page_views|Page Views|MySpectrum||',
              'support_page_views|Support Page Views|MySpectrum||',
              'view_statement|View Statement|MySpectrum||',
              'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||',
              'call_support|Call Support|MySpectrum||',
              'session_start|Session Starts|MySpectrum||')
        ) OR
        (unit = 'visits' AND metric
           IN('site_unique|Site Unique Values|MySpectrum||',
              'cancelled_appointments|Cancelled Appointments|MySpectrum||',
              'rescheduled_appointments|Rescheduled Appointments|MySpectrum||',
              'dashboard_view|Dashboard View|MySpectrum||',
              'equipment_list_view|Equipment List View|MySpectrum||',
              'account_view|Account View|MySpectrum||',
              'bill_pay_view|Bill Pay View|MySpectrum||',
              'support_view|Support View|MySpectrum||',
              'one_time_payment_start|One Time Payment Start|MySpectrum||',
              'one_time_payment_success|One Time Payment Success|MySpectrum||',
              'set_up_auto_payment_starts|Auto Pay Enrollment Starts|MySpectrum||',
              'set_up_auto_payment_successes|Auto Pay Enrollment Successes|MySpectrum||'
              'forgot_password_flow_start|Forgot Password Flow Start|MySpectrum||',
              'forgot_password_flow_success|Forgot Password Flow Success|MySpectrum||',
              'forgot_username_flow_start|Forgot Username Flow Start|MySpectrum||',
              'forgot_username_flow_success|Forgot Username Flow Success|MySpectrum||')
        )
      )
;


-- Venona equipment reboot completions
INSERT INTO ${env:TMP_db}.asp_app_agg_my_spc_long_metric_names
SELECT
    company,
    'Modem Router Resets' as metric,
    SUM(value) as value,
    unit,
    ${env:ymd}
FROM  asp_venona_counts_${env:CADENCE}
WHERE  source_table = 'asp_v_venona_events_portals_msa'
AND    ${env:ymd} >= '2018-08'
and    domain = 'app'
AND    metric IN('internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|MySpectrum||',
                'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|MySpectrum||')
AND unit IN ('instances')
GROUP BY  company,
          unit,
          ${env:ymd}
;


-- Metric Tableau name Lookup
DROP TABLE IF EXISTS ${env:LKP_db}.asp_app_metrics PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:LKP_db}.asp_app_metrics
( tableau_metric STRING,
  unit           STRING,
  hive_metric    STRING
)
;

-- Change names of metrics here to be reflected in Tableau using the following lookup
INSERT INTO TABLE ${env:LKP_db}.asp_app_metrics VALUES
('Page Views',                         'instances',  'page_views|Page Views|MySpectrum||'),
('Visits',                             'visits',     'site_unique|Site Unique Values|MySpectrum||'),
('Unique Visitors',                    'devices',    'site_unique|Site Unique Values|MySpectrum||'),
('Cancelled Appointments',             'visits',     'cancelled_appointments|Cancelled Appointments|MySpectrum||'),
('Rescheduled Appointments',           'visits',     'rescheduled_appointments|Rescheduled Appointments|MySpectrum||'),
('Support Page Views',                 'instances',  'support_page_views|Support Page Views|MySpectrum||'),
('Call Support',                       'instances',  'call_support|Call Support|MySpectrum||'),
('View Statement',                     'instances',  'view_statement|View Statement|MySpectrum||'),
('Dashboard View',                     'visits',     'dashboard_view|Dashboard View|MySpectrum||'),
('Equipment List View',                'visits',     'equipment_list_view|Equipment List View|MySpectrum||'),
('Account View',                       'visits',     'account_view|Account View|MySpectrum||'),
('Bill Pay View',                      'visits',     'bill_pay_view|Bill Pay View|MySpectrum||'),
('Support View',                       'visits',     'support_view|Support View|MySpectrum||'),
('Refresh Digital Receiver Requests',  'instances',  'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|MySpectrum||'),
('One Time Payment Start',             'visits',     'one_time_payment_start|One Time Payment Start|MySpectrum||' ),
('One Time Payment Success',           'visits',     'one_time_payment_success|One Time Payment Success|MySpectrum||'),
('AutoPay Enroll Start',               'visits',     'set_up_auto_payment_starts|Auto Pay Enrollment Starts|MySpectrum||'),
('AutoPay Enroll Success',             'visits',     'set_up_auto_payment_successes|Auto Pay Enrollment Successes|MySpectrum||'),
('Forgot Password Attempt',            'visits',     'forgot_password_flow_start|Forgot Password Flow Start|MySpectrum||'),
('Forgot Password Success',            'visits',     'forgot_password_flow_success|Forgot Password Flow Success|MySpectrum||'),
('Forgot Username Attempt',            'visits',     'forgot_username_flow_start|Forgot Username Flow Start|MySpectrum||'),
('Forgot Username Success',            'visits',     'forgot_username_flow_success|Forgot Username Flow Success|MySpectrum||'),
('Session Starts',                     'instances',  'session_start|Session Starts|MySpectrum||')
;

-- Metric Insert with nice names (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting AUTHENTICATED metrics with nice Tableau-friendly names...\n\n';

INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT  company,
        COALESCE (tableau_metric, metric) as metric_name,
        value,
        l.unit,
        ${env:ymd}
FROM ${env:TMP_db}.asp_app_agg_my_spc_long_metric_names l
LEFT JOIN ${env:LKP_db}.asp_app_metrics s
  on  l.metric = s.hive_metric
  AND l.unit   = s.unit
WHERE company <> 'Unknown'   -- comment this out to include unauthenticated counts
;
