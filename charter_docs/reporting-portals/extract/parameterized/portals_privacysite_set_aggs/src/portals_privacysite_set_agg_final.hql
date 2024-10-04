USE ${env:TMP_db};

SELECT'
--------------------------------------------------------------------------------
------------------ *** Insert all units into final table *** -------------------
--------------------------------------------------------------------------------
'
;


INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.asp_privacysite_set_agg PARTITION (label_date_denver, grain)

SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       label_date_denver ,
       grain
from ${env:domain}_${env:project}_set_agg_stage_accounts_${env:execid}
UNION
SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       label_date_denver ,
       grain
from ${env:domain}_${env:project}_set_agg_stage_devices_${env:execid}
UNION
SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       label_date_denver ,
       grain
from ${env:domain}_${env:project}_set_agg_stage_instances_${env:execid}
UNION
SELECT app_section       ,
       user_role         ,
       message_context   ,
       unit_type         ,
       metric_name       ,
       metric_value      ,
       label_date_denver ,
       grain
from ${env:domain}_${env:project}_set_agg_stage_visits_${env:execid}
;

DROP TABLE IF EXISTS ${env:domain}_${env:project}_set_agg_stage_accounts_${env:execid}   PURGE ;
DROP TABLE IF EXISTS ${env:domain}_${env:project}_set_agg_stage_devices_${env:execid}    PURGE ;
DROP TABLE IF EXISTS ${env:domain}_${env:project}_set_agg_stage_instances_${env:execid}  PURGE ;
DROP TABLE IF EXISTS ${env:domain}_${env:project}_set_agg_stage_visits_${env:execid}     PURGE ;
