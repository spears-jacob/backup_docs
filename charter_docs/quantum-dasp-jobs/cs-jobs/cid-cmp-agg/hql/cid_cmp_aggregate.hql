USE ${env:DASP_db};
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
SET hive.merge.size.per.task=1024000000;
SET hive.merge.smallfiles.avgsize=1024000000;
SET hive.merge.tezfiles=true;
SET hive.vectorized.execution.enabled=false;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
SET orc.force.positional.evolution=true;

INSERT OVERWRITE TABLE ${env:DASP_db}.cs_cid_cmp_aggregate PARTITION (grain, label_date_denver)
SELECT application_name,
       message_name,
       COUNT(1) count_of_events,
       COUNT(DISTINCT visit_device_uuid) count_of_unique_users,
       COUNT(visit_device_uuid) count_of_users,
       COUNT(DISTINCT unique_visit_id) count_of_visits,
       campaign_id,
       page_title,
       page_id,
       page_name,
       '${env:CADENCE}' as grain,
       '${env:LABEL_DATE_DENVER}' as label_date_denver
FROM ${env:DASP_db}.cs_cid_cmp_extract
WHERE (denver_date >= '${env:START_DATE}'
   AND denver_date <  '${env:END_DATE}')
GROUP BY application_name,
         message_name,
         campaign_id,
         page_title,
         page_id,
         page_name
;