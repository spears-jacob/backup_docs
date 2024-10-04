USE ${env:ENVIRONMENT};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;

SELECT "\n\nFor 5: Other Metrics\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_other_metrics PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_other_metrics AS
SELECT
       CASE WHEN application_name = 'specnet' THEN 'Spectrum.net'
            WHEN application_name = 'smb'     THEN 'SpectrumBusiness.net'
            WHEN application_name = 'myspectrum' THEN 'My Spectrum App'
       END AS application_name,
       concat(metric_name,'_instances') as metric_name,
       metric_value,
       label_date_denver
  FROM prod.venona_set_agg_portals
 WHERE label_date_denver = '${env:label_date_denver}'
   and grouping_id = 2049
   AND application_name in ('specnet','smb','myspectrum')
   and metric_name in ('portals_support_page_views',
                       'portals_one_time_payment_successes',
                       'portals_set_up_auto_payment_successes',
                       'portals_all_equipment_reset_flow_successes'
                      )
   and grain in ('fiscal_monthly')
   AND unit_type='instances';

INSERT INTO ${env:TMP_db}.asp_monthly_other_metrics
SELECT
       CASE WHEN application_name = 'specnet' THEN 'Spectrum.net'
            WHEN application_name = 'smb'     THEN 'SpectrumBusiness.net'
            WHEN application_name = 'myspectrum' THEN 'My Spectrum App'
       END AS application_name,
       concat(metric_name,'_devices') as metric_name,
       metric_value,
       label_date_denver
  FROM prod.venona_set_agg_portals
 WHERE label_date_denver = '${env:label_date_denver}'
   and grouping_id = 2049
   AND application_name in ('specnet','smb','myspectrum')
   and metric_name in ('portals_site_unique'
                      )
   and grain in ('fiscal_monthly')
   AND unit_type='devices';

INSERT INTO ${env:TMP_db}.asp_monthly_other_metrics
SELECT
       CASE WHEN application_name = 'specnet' THEN 'Spectrum.net'
            WHEN application_name = 'smb'     THEN 'SpectrumBusiness.net'
            WHEN application_name = 'myspectrum' THEN 'My Spectrum App'
       END AS application_name,
       'portals_cancelled_rescheduled_view_service_appointments_instances' as metric_name,
       sum(metric_value) as metric_value,
       label_date_denver
  FROM prod.venona_set_agg_portals
 WHERE label_date_denver = '${env:label_date_denver}'
   and grouping_id = 2049
   AND application_name in ('specnet','smb','myspectrum')
   and metric_name in ('portals_cancelled_service_appointments',
                      'portals_rescheduled_service_appointments',
                      'portals_tech_tracker_unauth_views'
                      )
   and grain in ('fiscal_monthly')
   AND unit_type='instances'
 group by application_name,
          label_date_denver;

INSERT INTO ${env:TMP_db}.asp_monthly_other_metrics
SELECT
       CASE WHEN application_name = 'specnet' THEN 'Spectrum.net'
            WHEN application_name = 'smb'     THEN 'SpectrumBusiness.net'
            WHEN application_name = 'myspectrum' THEN 'My Spectrum App'
       END AS application_name,
       'portals_pause_unpause_cancel_scp_device_instances' as metric_name,
       sum(metric_value) as metric_value,
       label_date_denver
  FROM prod.venona_set_agg_portals
 WHERE label_date_denver = '${env:label_date_denver}'
   and grouping_id = 2049
   AND application_name in ('myspectrum')
   and metric_name in ('portals_scp_click_pause_device',
                       'portals_scp_click_unpause_device',
                       'portals_scp_click_cancel_pause_device'
                      )
   and grain in ('fiscal_monthly')
   AND unit_type='instances'
 group by application_name,
          label_date_denver;

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
       'unique_visitors' as metric_name,
       metric_value,
       current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_other_metrics
 where metric_name='portals_site_unique_devices';

INSERT into TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
       'support_page_views' as metric_name,
       metric_value,
       current_date as run_date,
       label_date_denver
FROM ${env:TMP_db}.asp_monthly_other_metrics
where metric_name='portals_support_page_views_instances';

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
       'one_time_payments' as metric_name,
       metric_value,
       current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_other_metrics
 where metric_name='portals_one_time_payment_successes_instances';

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
      'autopay_setups' as metric_name,
      metric_value,
      current_date as run_date,
      label_date_denver
 FROM ${env:TMP_db}.asp_monthly_other_metrics
where metric_name='portals_set_up_auto_payment_successes_instances';

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
       'equipment_reset' as metric_name,
       metric_value,
       current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_other_metrics
 where metric_name='portals_all_equipment_reset_flow_successes_instances';

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
      'appointments' as metric_name,
      metric_value,
      current_date as run_date,
      label_date_denver
 FROM ${env:TMP_db}.asp_monthly_other_metrics
where metric_name='portals_cancelled_rescheduled_view_service_appointments_instances';

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT application_name,
      'scp_device' as metric_name,
      metric_value,
      current_date as run_date,
      label_date_denver
 FROM ${env:TMP_db}.asp_monthly_other_metrics
where metric_name='portals_pause_unpause_cancel_scp_device_instances';
