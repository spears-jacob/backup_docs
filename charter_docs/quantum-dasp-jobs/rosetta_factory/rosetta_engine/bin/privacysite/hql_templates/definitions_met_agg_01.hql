USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
set orc.force.positional.evolution=true;

DROP TABLE IF EXISTS ${env:TMP_db}.ps_metric_agg_${env:CLUSTER} PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.ps_metric_agg_${env:CLUSTER} AS

select  state__view__current_page__app_section as app_section,
        visit__user__role as user_role,
        message__context as message_context,
        visit__device__enc_uuid as device_id,
        visit__visit_id as visit_id,
