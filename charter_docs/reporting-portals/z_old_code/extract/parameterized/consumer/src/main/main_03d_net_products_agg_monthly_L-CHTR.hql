-------------------------------------------------------------------------------
-- Spectrum.net fiscal monthly data
-- provided by your friends in Product Intelligence
-- export CADENCE=fiscal_monthly; export pf=year_fiscal_month_Denver;
-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

SELECT '\n\nNow running main_03d_net_products_agg_monthly_L-CHTR ...\n\n';

DROP TABLE IF EXISTS ${env:TMP_db}.asp_consumer_agg_long_metric_names;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_consumer_agg_long_metric_names AS
  SELECT
      company,
      metric,
      value,
      unit,
      ${env:pf}
  FROM  asp_venona_counts_${env:CADENCE}
  WHERE  source_table = 'asp_v_venona_events_portals_specnet'
  AND    ${env:pf} >= '2018-09'
  and    domain = 'resi'
  AND ( (unit = 'instances' AND metric
           IN('support_section_page_views|Support Section Page Views|Spectrum.net||',
              'view_online_statement|View Online Statement|Spectrum.net||')
        ) OR
        (unit = 'visits' AND metric
           IN('site_unique|Site Unique Values|Spectrum.net||',
           'set_up_auto_payment_successes|Auto Pay Enrollment Successes|Spectrum.net||',
           'one_time_payment_successes|One Time Payment Successes|Spectrum.net||',
  --  These metrics are currently only available in Adobe.... uncomment when available in Venona
  --         'new_ids_created_successes|New IDs Created Successes|Spectrum.net||',
  --         'password_reset_successes|Password Reset Successes|Spectrum.net||',
  --         'username_recovery_successes|Username Recovery Successes|Spectrum.net||',
  --         'new_ids_created_attempts|New IDs Created Attempts|Spectrum.net||',
  --         'password_reset_attempts|Password Reset Attempts|Spectrum.net||',
  --         'username_recovery_attempts|Username Recovery Attempts|Spectrum.net||',
           'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|Spectrum.net||',
           'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|Spectrum.net||',
           'rescheduled_service_appointments|Rescheduled Service Appointments|Spectrum.net||',
           'cancelled_service_appointments|Cancelled Service Appointments|Spectrum.net||')
        )
      )
;
-------------------------------------------------------------------------------
-- Adobe Counts --
-- remove these once metrics are available from Venona
-------------------------------------------------------------------------------
INSERT INTO ${env:TMP_db}.asp_consumer_agg_long_metric_names
  SELECT
      company,
      metric,
      value,
      unit,
      ${env:pf}
  FROM  asp_counts_${env:CADENCE}
  WHERE  source_table = 'asp_v_net_events'
  AND    ${env:pf} >= '2018-09'
  and    domain = 'resi'
  AND  (unit = 'visits' AND metric
        in ('password_reset_attempts|Password Reset Attempts|Spectrum.net_adobe||',
            'password_reset_successes|Password Reset Successes|Spectrum.net_adobe||',
            'username_recovery_attempts|Username Recovery Attempts|Spectrum.net_adobe||',
            'username_recovery_successes|Username Recovery Successes|Spectrum.net_adobe||',
            'ids_created_successes|New IDs Created Successes|Spectrum.net_adobe||',
            'new_ids_created_attempts|New IDs Created Attempts|Spectrum.net_adobe||'
           )
        )

;
-------------------------------------------------------------------------------

----- Insert into agg table -------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
  SELECT value,
         unit,
         'resi' as domain,
         company,
         ${env:pf},
         metric
  FROM ${env:TMP_db}.asp_consumer_agg_long_metric_names
  order by ${env:pf},
           metric,
           company
;
