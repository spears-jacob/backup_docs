USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=512000000;
set mapreduce.input.fileinputformat.split.minsize=512000000;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

SELECT "\n\nFor 5: Other Metrics\n\n";
-- creating temp table to be inserted into final asp_product_monthly_metrics
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_other_metrics PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_other_metrics AS
SELECT 
  CASE
    WHEN application_name = 'specnet' THEN 'Spectrum.net'
    WHEN application_name = 'smb' THEN 'SpectrumBusiness.net'
    WHEN application_name = 'myspectrum' THEN 'My Spectrum App'
    WHEN application_name = 'specmobile' THEN 'Spectrum Mobile Account App'
    WHEN application_name = 'spectrumcommunitysolutions' THEN 'SpectrumCommunitySolutions.net'
    ELSE application_name
    END AS application_name,
  grouping_id,
  CASE
    WHEN lower(device_type) in ('androidphone', 'androidtablet') THEN 'Android'
    WHEN lower(device_type) in ('ipad', 'iphone') then 'iOS'
    ELSE device_type 
    END AS device_type,
  CASE 
    WHEN lower(form_factor) in ('pc') THEN 'Web'
    WHEN lower(form_factor) in ('phone', 'tablet', 'mobile') THEN 'Mobile Web'
    ELSE form_factor
    END as form_factor,
  CASE 
    WHEN metric_name = 'portals_support_page_views' THEN 'support_page_views'
    WHEN metric_name = 'portals_one_time_payment_successes' THEN 'one_time_payments'
    WHEN metric_name = 'portals_set_up_auto_payment_successes' THEN 'autopay_setups'
    WHEN metric_name = 'portals_all_equipment_reset_flow_successes' THEN 'equipment_reset'
    WHEN metric_name = 'portals_internet_equipment_reset_flow_successes' THEN 'internet_equipment_reset'
    WHEN metric_name = 'portals_tv_equipment_reset_flow_successes' THEN 'tv_equipment_reset'
    WHEN metric_name = 'portals_voice_equipment_reset_flow_successes' THEN 'voice_equipment_reset'
    WHEN metric_name = 'portals_view_online_statments' THEN 'view_online_statement'
    WHEN metric_name = 'portals_equipment_confirm_edit_ssid_select_action' THEN 'confirm_edit_ssid'
    WHEN metric_name in ('portals_scp_click_pause_device',
                          'portals_scp_click_unpause_device',
                          'portals_scp_click_cancel_pause_device') THEN 'scp_device' 
    END as metric_name,
  metric_value,
  label_date_denver
FROM ${env:DASP_db}.quantum_set_agg_portals
WHERE label_date_denver = '${hiveconf:label_date_denver}'
  AND (
  grouping_id = 65502 -- app_name,form_factor
  OR grouping_id = 65503 --app_name
  OR grouping_id = 57311 --app_name, device_type
  OR grouping_id = 65535 -- date only
  )
  AND application_name in ('specnet','smb','myspectrum','specmobile', 'spectrumcommunitysolutions', 'All App Versions')
  and metric_name in ('portals_support_page_views',
                      'portals_one_time_payment_successes',
                      'portals_set_up_auto_payment_successes',
                      'portals_all_equipment_reset_flow_successes',
                      'portals_internet_equipment_reset_flow_successes',
                      'portals_tv_equipment_reset_flow_successes',
                      'portals_voice_equipment_reset_flow_successes',
                      'portals_view_online_statments',
                      'portals_equipment_confirm_edit_ssid_select_action',
                      'portals_scp_click_pause_device',
                      'portals_scp_click_unpause_device',
                      'portals_scp_click_cancel_pause_device'
                    )
  and grain in ('fiscal_monthly')
  AND unit_type='instances';

INSERT into TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT CASE 
        WHEN grouping_id = 65503 THEN application_name
        WHEN grouping_id = 65502 THEN application_name||'-'||form_factor
        WHEN grouping_id = 57311 THEN application_name||'-'||device_type
        WHEN grouping_id = 65535 THEN 'Overall'
        ELSE application_name END as application_name,
       metric_name,
       sum(if(metric_value = 0, NULL, metric_value)) as metric_value,
       current_date as run_date,
       label_date_denver
FROM ${env:TMP_db}.asp_monthly_other_metrics
where (grouping_id = 65503) --app_name breakout
  OR (grouping_id = 65502 AND application_name in ('Spectrum.net', 'SpectrumBusiness.net', 'SpectrumCommunitySolutions.net'))-- app_name:form_factor breakout
  OR (grouping_id = 57311 AND application_name in ('My Spectrum App', 'Spectrum Mobile Account App')) -- app_name:device_type breakout
  OR (grouping_id = 65535) -- date only
group by 
      CASE 
      WHEN grouping_id = 65503 THEN application_name
      WHEN grouping_id = 65502 THEN application_name||'-'||form_factor
      WHEN grouping_id = 57311 THEN application_name||'-'||device_type
      WHEN grouping_id = 65535 THEN 'Overall'
      ELSE application_name END,
      metric_name,
      current_date,
      label_date_denver
;

INSERT into TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT  
       'Overall - Residential' as application_name,
       metric_name,
       sum(if(metric_value = 0, NULL, metric_value)) as metric_value,
       current_date as run_date,
       label_date_denver
FROM ${env:TMP_db}.asp_monthly_other_metrics
WHERE grouping_id = 65503 --app_name breakout
AND application_name in ('Spectrum.net', 'My Spectrum App')
GROUP BY 
      'Overall - Residential',
      metric_name,
      current_date,
      label_date_denver
;

INSERT into TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT  
       'Overall - Commercial' as application_name,
       metric_name,
       sum(if(metric_value = 0, NULL, metric_value)) as metric_value,
       current_date as run_date,
       label_date_denver
FROM ${env:TMP_db}.asp_monthly_other_metrics
WHERE grouping_id = 65503 --app_name breakout
AND application_name in ('SpectrumBusiness.net', 'SpectrumCommunitySolutions.net')
GROUP BY 
      'Overall - Commercial',
      metric_name,
      current_date,
      label_date_denver
;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_other_metrics PURGE;
