USE ${env:ENVIRONMENT};

--getting data for monthly report
--for the last 6 months
CREATE TEMPORARY TABLE asp_product_monthly_latest_tmp as
select case when cast(substr(current_date,9,10) as int) > 20 then substr(add_months(current_date, 0),0,7)
            when cast(substr(current_date,9,10) as int) < 10 then substr(add_months(current_date, -1),0,7)
       end as m1,
       case when cast(substr(current_date,9,10) as int) > 20 then substr(add_months(current_date, -1),0,7)
            when cast(substr(current_date,9,10) as int) < 10 then substr(add_months(current_date, -2),0,7)
       end as m2,
       case when cast(substr(current_date,9,10) as int) > 20 then substr(add_months(current_date, -2),0,7)
            when cast(substr(current_date,9,10) as int) < 10 then substr(add_months(current_date, -3),0,7)
       end as m3,
       case when cast(substr(current_date,9,10) as int) > 20 then substr(add_months(current_date, -3),0,7)
            when cast(substr(current_date,9,10) as int) < 10 then substr(add_months(current_date, -4),0,7)
       end as m4,
       case when cast(substr(current_date,9,10) as int) > 20 then substr(add_months(current_date, -4),0,7)
            when cast(substr(current_date,9,10) as int) < 10 then substr(add_months(current_date, -5),0,7)
       end as m5,
       case when cast(substr(current_date,9,10) as int) > 20 then substr(add_months(current_date, -5),0,7)
            when cast(substr(current_date,9,10) as int) < 10 then substr(add_months(current_date, -6),0,7)
       end as m6;

CREATE TEMPORARY TABLE asp_product_monthly_latest_6months as
select value
  from
      (select map('m1',m1,
                 'm2',m2,
                 'm3',m3,
                 'm4',m4,
                 'm5',m5,
                 'm6',m6) as tmp_map
        from asp_product_monthly_latest_tmp) a
lateral view explode (tmp_map) explode_table as name, value;

TRUNCATE TABLE asp_product_monthly_last_6months;
INSERT INTO TABLE asp_product_monthly_last_6months
SELECT metric_name,
       application_name,
       metric_order,
       application_order,
       report_metric_name,
       CASE WHEN month_order <= 3 THEN '3m' END AS 3m_filter,
       label_date_denver,
       metric_value,
       current_date as run_date
FROM
      (Select metric_name,
              application_name,
              CASE WHEN metric_name = 'unique_households' THEN '1_unique_households'
                   WHEN metric_name = 'unique_visitors' THEN '2_unique_visitors'
                   WHEN metric_name = 'support_page_views' THEN '3_support_page_views'
                   WHEN metric_name = 'one_time_payments' THEN '4_one_time_payments'
                   WHEN metric_name = 'autopay_setups' THEN '5_autopay_setups'
                   WHEN metric_name = 'equipment_reset' THEN '6_equipment_reset'
                   WHEN metric_name = 'scp_device' THEN '7_scp_device'
                   WHEN metric_name = 'identity' THEN '8_identity'
                   WHEN metric_name = 'appointments' THEN '9_appointments'
                   WHEN metric_name = 'Digital First Contact Rate' THEN '91_digital_first_contact_rate'
                   WHEN metric_name = 'Call in Rate' THEN '92_call_in_rate'
                   WHEN metric_name = 'error_message_rate' THEN '93_error_message_rate'
                   ELSE metric_name
              END AS metric_order,
              CASE WHEN application_name = 'Spectrum.net' THEN '1_Spectrum.net'
                   WHEN application_name = 'My Spectrum App' THEN '2_My Spectrum App'
                   WHEN application_name = 'SpectrumBusiness.net' THEN '3_SpectrumBusiness.net'
                   WHEN application_name = 'Spectrum Mobile Account App' THEN '4_Spectrum Mobile Account App'
                   WHEN application_name = 'Consumer' THEN '1_Consumer'
                   WHEN application_name = 'SMB' THEN '2_SMB'
                   WHEN application_name = 'new_ids_created' THEN '1_new_ids_created'
                   WHEN application_name = 'password_reset_recovery' THEN '2_password_reset_recovery'
                   ELSE application_name
              END AS application_order,
              CASE WHEN metric_name = 'unique_households' THEN 'Unique Households'
                   WHEN metric_name = 'unique_visitors' THEN 'Unique Visitors'
                   WHEN metric_name = 'support_page_views' THEN 'Support Page Views'
                   WHEN metric_name = 'one_time_payments' THEN 'One Time Payments'
                   WHEN metric_name = 'autopay_setups' THEN 'AutoPay Setups'
                   WHEN metric_name = 'equipment_reset' THEN 'Equipment Reset'
                   WHEN metric_name = 'identity' THEN 'Identity'
                   WHEN metric_name = 'appointments' THEN 'Appointments (Views/Cancel/Reschedule)'
                   WHEN metric_name = 'error_message_rate' THEN 'Error Message Rate %'
                   WHEN metric_name = 'scp_device' THEN 'SCP Device Management (Cancel/Pause/Unpause)'
                   ELSE metric_name
              END AS report_metric_name,
              metric_value,
              label_date_denver,
              dense_rank() over(order by label_date_denver desc) as month_order
      FROM asp_product_monthly_metrics
     WHERE substr(label_date_denver,0,7) in (
           select value
             from asp_product_monthly_latest_6months)) a;

--last month page load time
TRUNCATE TABLE asp_product_monthly_last_month;
INSERT INTO TABLE asp_product_monthly_last_month
select 'Page Load Time' as metric_name,
       application_name,
       CASE WHEN application_name = 'Spectrum.net' THEN '1_Spectrum.net'
            WHEN application_name = 'SpectrumBusiness.net' THEN '2_SpectrumBusiness.net'
       END AS application_order,
       pct_less_than2,
       pct_between2_and4,
       pct_between4_and6,
       pct_larger_than6,
       label_date_denver,
       current_date as run_date
  from asp_product_monthly_time
 WHERE substr(label_date_denver,0,7) in (
       select value
         from asp_product_monthly_latest_6months
        order by value desc
        limit 1);
