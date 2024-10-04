USE ${env:ENVIRONMENT};

set hive.vectorized.execution.enabled = false;

SELECT "\n\nFor asp_idm_daily_metrics\n\n";

INSERT OVERWRITE TABLE asp_idm_daily_metrics PARTITION(date_denver)
select platform,
       metric_name,
       metric_value,
       date_denver
from
      (select
              platform,
              map('newIDsCreated', SUM(IF(last_event='createEnterPWSuccess',1,0)),
                  'successfullyChangedPW', SUM(IF(last_event='recoverUNSuccess',1,0))) AS tmp_map,
              date_denver
         FROM prod.asp_idm_paths_flow
        WHERE date_denver = '${env:START_DATE}'
        group bY platform,
                 date_denver) a
LATERAL VIEW explode(tmp_map) explode_table as metric_name, metric_value;
