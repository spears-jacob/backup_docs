USE ${env:DASP_db};

set hive.auto.convert.join=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.support.concurrency=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

SELECT '
# --------------------------------------------------------------------------------
# ----------- *** Insert all units into template, then final table *** -----------
# --------------------------------------------------------------------------------
'
;

INSERT INTO ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid}
SELECT * FROM ${env:TMP_db}.quantum_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid} ;

INSERT INTO ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid}
SELECT * FROM ${env:TMP_db}.quantum_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid} ;

INSERT INTO ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid}
SELECT * FROM ${env:TMP_db}.quantum_set_agg_portals_stage_instances_${hiveconf:execid}_${hiveconf:stepid} ;

INSERT INTO ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid}
SELECT * FROM ${env:TMP_db}.quantum_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid} ;

INSERT OVERWRITE TABLE ${hiveconf:outputsetaggtable} PARTITION (label_date_denver, grain)
SELECT * FROM ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid};

select label_date_denver, grain, count(grain) as count_recs_accounts  FROM ${env:TMP_db}.quantum_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid} GROUP BY label_date_denver, grain;
select label_date_denver, grain, count(grain) as count_recs_devices   FROM ${env:TMP_db}.quantum_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid} GROUP BY label_date_denver, grain;
select label_date_denver, grain, count(grain) as count_recs_instances FROM ${env:TMP_db}.quantum_set_agg_portals_stage_instances_${hiveconf:execid}_${hiveconf:stepid} GROUP BY label_date_denver, grain;
select label_date_denver, grain, count(grain) as count_recs_visits    FROM ${env:TMP_db}.quantum_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid} GROUP BY label_date_denver, grain;
select label_date_denver, grain, count(grain) as count_recs_all_units FROM ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid} GROUP BY label_date_denver, grain;

DROP TABLE IF EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_instances_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid} PURGE;
