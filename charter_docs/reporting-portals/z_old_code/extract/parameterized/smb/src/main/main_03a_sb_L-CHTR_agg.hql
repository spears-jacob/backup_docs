-------------------------------------------------------------------------------
-- SpectrumBusiness.net fiscal monthly data
-- provided by your friends in Product Intelligence
-- export CADENCE=fiscal_monthly; export pf=year_fiscal_month_Denver;
-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

SELECT '\n\nNow running main_03a_sb_L-CHTR_agg ...\n\n';

DROP TABLE IF EXISTS ${env:TMP_db}.asp_sb_agg_long_metric_names;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_sb_agg_long_metric_names AS
  SELECT
      company,
      metric,
      value,
      unit,
      ${env:pf}
  FROM  asp_venona_counts_${env:CADENCE}
  WHERE  source_table = 'asp_v_venona_events_portals_smb'
  AND    ${env:pf} >= '2018-08'
  and    domain = 'sb'
  AND ( (unit = 'instances' AND metric
           IN( 'support_page_views|Support Section Page Views|SpectrumBusiness.net||',
               'view_online_statement|View Online Statement|SpectrumBusiness.net||',
               'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||',
               'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||',
               'new_ids_created_attempts|New IDs Created Attempts|SpectrumBusiness.net||',
               'sub_user_creation_attempts|New Sub Accounts Creation Attempts|SpectrumBusiness.net||'
             )
        ) OR
        (unit = 'visits' AND metric
           IN( 'site_unique_auth|Site Unique Values Authenticated|SpectrumBusiness.net||',
               'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net||',
               'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net||',
               'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net||',
               'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net||',
               'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net||',
               'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net||')
        )
      )
;


-------------------------------------------------------------------------------
----- Insert into agg table -------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)
  SELECT value,
         unit,
         'sb' as domain,
         company,
         ${env:pf},
         metric
  FROM ${env:TMP_db}.asp_sb_agg_long_metric_names
  order by ${env:pf},
           metric,
           company
;
