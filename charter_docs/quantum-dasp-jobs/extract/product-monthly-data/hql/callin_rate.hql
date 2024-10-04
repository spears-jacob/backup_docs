USE ${env:DASP_db};

set hive.vectorized.execution.enabled = false;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

SELECT "\n\nFor 6: Call In Rate\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_callin_rate PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_callin_rate AS

select '${hiveconf:label_date_denver}' as label_date_denver,
       visit_type                   as domain,
       metric,
       value
from cs_prod_monthly_fiscal_month_metrics
WHERE fiscal_month=substring('${hiveconf:label_date_denver}',1,7)
;

INSERT INTO TABLE ${env:DASP_db}.asp_product_monthly_metrics partition(label_date_denver)
SELECT CASE WHEN domain = 'SMB' THEN 'SpectrumBusiness.net'
            WHEN domain = 'MYSPECTRUM' THEN 'My Spectrum App'
            WHEN domain = 'SPECMOBILE' THEN 'Spectrum Mobile Account App'
            WHEN domain = 'SPECNET' THEN 'Spectrum.net'
            WHEN domain = 'Overall - RESIDENTIAL' THEN 'Overall - Residential'
            WHEN domain = 'Overall - COMMERCIAL' THEN 'Overall - Commercial'
            WHEN domain in ('COMBINED', 'Overall') THEN 'Overall'
            ELSE domain
       END AS domain,
       CASE WHEN metric = 'dfcr' THEN 'Digital First Contact Rate'
            WHEN metric = 'cir' THEN 'Call in Rate'
       END AS metric_name,
       value as metric_value,
       current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_callin_rate;

--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_callin_rate PURGE;
