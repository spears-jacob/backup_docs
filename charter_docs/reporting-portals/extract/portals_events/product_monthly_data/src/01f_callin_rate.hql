USE ${env:ENVIRONMENT};

set hive.vectorized.execution.enabled = false;

SELECT "\n\nFor 6: Call In Rate\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_callin_rate PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_callin_rate AS

select '${env:label_date_denver}' as label_date_denver,
       customer_type as domain,
       metric,
       value
from prod.cs_prod_monthly_fiscal_month_metrics
WHERE fiscal_month=substring('${env:label_date_denver}',1,7)
;

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT CASE WHEN domain = 'COMMERCIAL' THEN 'SMB'
            WHEN domain = 'RESIDENTIAL' THEN 'Consumer'
            WHEN domain = 'COMBINED' THEN 'Overall'
       END AS domain,
       CASE WHEN metric = 'dfcr' THEN 'Digital First Contact Rate'
            WHEN metric = 'cir' THEN 'Call in Rate'
       END AS metric_name,
       value as metric_value,
       current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_callin_rate;
