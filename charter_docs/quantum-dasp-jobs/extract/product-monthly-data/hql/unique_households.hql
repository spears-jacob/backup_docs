USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

SELECT "\n\nFor 3: unique_households\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_unique_visitors_and_hh PURGE;
CREATE TEMPORARY TABLE IF NOT EXISTS  ${env:TMP_db}.asp_monthly_unique_visitors_and_hh AS 
--Creating temp table
SELECT
  CASE 
    WHEN application_name = 'specnet' THEN 'Spectrum.net'
    WHEN application_name = 'smb'     THEN 'SpectrumBusiness.net'
    WHEN application_name = 'myspectrum' THEN 'My Spectrum App'
    WHEN application_name = 'specmobile' THEN 'Spectrum Mobile Account App'
    WHEN application_name = 'spectrumcommunitysolutions' THEN 'SpectrumCommunitySolutions.net'
    else application_name
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
    WHEN metric_name = 'portals_site_unique' AND unit_type = 'devices' THEN 'unique_visitors'
    WHEN metric_name = 'portals_site_unique_auth' AND unit_type = 'devices' THEN 'unique_auth_visitors' 
    WHEN metric_name = 'portals_site_unique' and unit_type = 'accounts' THEN 'unique_households'
    END as metric_name,
  metric_value,
  label_date_denver
  FROM ${env:DASP_db}.quantum_set_agg_portals
 WHERE label_date_denver = '${hiveconf:label_date_denver}'
   and (
        grouping_id = 131071 --date only
        OR grouping_id = 131007 --date, app_name
        OR grouping_id = 114623 -- date, app_name, device_type
        OR grouping_id = 131005 -- date, app_name, form_factor
        )
   AND application_name in ('specnet','smb','myspectrum','specmobile', 'spectrumcommunitysolutions', 'All App Versions')
   and metric_name in ('portals_site_unique', 'portals_site_unique_auth'
                      )
   and grain in ('fiscal_monthly')
   AND unit_type in ('devices', 'accounts');

INSERT INTO TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT CASE 
       WHEN grouping_id = 131007 THEN application_name
       WHEN grouping_id = 131005 THEN application_name||'-'||form_factor
       WHEN grouping_id = 114623 THEN application_name||'-'||device_type
       WHEN grouping_id = 131071 THEN 'Overall'
       ELSE application_name END as application_name,
       metric_name,
       sum(metric_value) as metric_value,
       current_date as run_date,
       label_date_denver
      FROM ${env:TMP_db}.asp_monthly_unique_visitors_and_hh
      where ((grouping_id = 131007) --app_name breakout
      OR (grouping_id = 131005 AND application_name in ('Spectrum.net', 'SpectrumBusiness.net', 'SpectrumCommunitySolutions.net'))-- app_name:form_factor breakout
      OR (grouping_id = 114623 AND application_name in ('My Spectrum App', 'Spectrum Mobile Account App')) -- app_name:device_type breakout
      OR (grouping_id = 131071)) -- date only
      and metric_name in ('unique_visitors', 'unique_auth_visitors', 'unique_households')
      group by 
      CASE 
       WHEN grouping_id = 131007 THEN application_name
       WHEN grouping_id = 131005 THEN application_name||'-'||form_factor
       WHEN grouping_id = 114623 THEN application_name||'-'||device_type
       WHEN grouping_id = 131071 THEN 'Overall'
       ELSE application_name END,
      metric_name,
      current_date,
      label_date_denver;
-- Unique HH and devices for resi overall
INSERT INTO TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT
application_name,
metric_name,
metric_value,
current_date as run_date,
label_date_denver
FROM (
    SELECT
    '${hiveconf:label_date_denver}' AS label_date_denver,
    CASE 
    WHEN application_name in ('specnet', 'myspectrum') THEN 'Overall - Residential'
    WHEN application_name in ('smb', 'spectrumcommunitysolutions') THEN 'Overall - Commercial'
    END as application_name,
    MAP(
        'unique_auth_visitors', COUNT(distinct if(portals_site_unique_auth > 0, device_id, NULL)),
        'unique_visitors', COUNT(distinct if(portals_site_unique > 0, device_id, NULL)),
        'unique_households', COUNT(distinct if(portals_site_unique > 0, portals_unique_acct_key, NULL))
    ) as tmp_map
    FROM ${env:DASP_db}.quantum_metric_agg_portals
    WHERE (denver_date >= ("${hiveconf:START_DATE}") AND denver_date < ("${hiveconf:END_DATE}"))
    AND application_name in ('specnet', 'myspectrum', 'smb', 'spectrumcommunitysolutions')
    group by  
    CASE 
    WHEN application_name in ('specnet', 'myspectrum') THEN 'Overall - Residential'
    WHEN application_name in ('smb', 'spectrumcommunitysolutions') THEN 'Overall - Commercial'
    END 
) as tall
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_unique_households PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_unique_visitors PURGE; 