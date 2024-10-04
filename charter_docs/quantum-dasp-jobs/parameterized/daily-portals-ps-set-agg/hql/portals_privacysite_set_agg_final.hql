set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
SET hive.merge.tezfiles=true;

USE ${env:TMP_db};

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_privacysite_set_agg PARTITION (partition_date_utc, grain)

SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       partition_date_utc ,
       grain
from portals_privacysite_set_agg_stage_accounts_${hiveconf:CLUSTER}
UNION
SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       partition_date_utc ,
       grain
from portals_privacysite_set_agg_stage_devices_${hiveconf:CLUSTER}
UNION
SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       partition_date_utc ,
       grain
from portals_privacysite_set_agg_stage_instances_${hiveconf:CLUSTER}
UNION
SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       partition_date_utc ,
       grain
from portals_privacysite_set_agg_stage_visits_${hiveconf:CLUSTER}
;

DROP TABLE IF EXISTS portals_privacysite_set_agg_stage_accounts_${hiveconf:CLUSTER}   PURGE ;
DROP TABLE IF EXISTS portals_privacysite_set_agg_stage_devices_${hiveconf:CLUSTER}    PURGE ;
DROP TABLE IF EXISTS portals_privacysite_set_agg_stage_instances_${hiveconf:CLUSTER}  PURGE ;
DROP TABLE IF EXISTS portals_privacysite_set_agg_stage_visits_${hiveconf:CLUSTER}     PURGE ;
