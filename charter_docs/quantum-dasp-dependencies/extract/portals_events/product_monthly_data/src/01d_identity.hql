USE ${env:ENVIRONMENT};

set hive.vectorized.execution.enabled = false;

SELECT "\n\nFor 4: identity\n\n";

DROP TABLE IF EXISTS ${env:TMP_db}.asp_monthly_identity PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_monthly_identity AS

select '${env:label_date_denver}' as label_date_denver,
       SUM(1) as num_total,
       SUM(IF(last_event='createEnterPWSuccess', 1, 0)) AS createId,
       SUM(IF(last_event='recoverResetPWSuccess', 1, 0)) AS resetPW
from prod.asp_idm_paths_flow
WHERE ( date_denver  >= '${env:START_DATE}' AND date_denver < '${env:END_DATE}')
;

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT 'new_ids_created' as domain,
       'identity' as metric_name,
       createId as metric_value,
        current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_identity;

INSERT INTO TABLE asp_product_monthly_metrics partition(label_date_denver)
SELECT 'password_reset_recovery' as domain,
       'identity' as metric_name,
       resetPW as metric_value,
       current_date as run_date,
       label_date_denver
  FROM ${env:TMP_db}.asp_monthly_identity;
