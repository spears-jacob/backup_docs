set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
SET hive.merge.tezfiles=true;

USE ${env:TMP_db};

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_set_agg PARTITION (partition_date_utc, grain)

SELECT
*
from m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}
UNION
SELECT
*
from m2dot0_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid}
UNION
SELECT
*
from m2dot0_set_agg_instances_${hiveconf:execid}_${hiveconf:stepid}
UNION
SELECT
*
from m2dot0_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid}
;

DROP TABLE IF EXISTS m2dot0_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}   PURGE ;
DROP TABLE IF EXISTS m2dot0_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid}    PURGE ;
DROP TABLE IF EXISTS m2dot0_set_agg_instances_${hiveconf:execid}_${hiveconf:stepid}  PURGE ;
DROP TABLE IF EXISTS m2dot0_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid}     PURGE ;
