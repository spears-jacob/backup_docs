USE ${env:DASP_db};

SET hive.vectorized.execution.enabled=false;
SET hive.auto.convert.join=false;
SET hive.optimize.sort.dynamic.partition=false;
SET orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;


INSERT OVERWRITE TABLE asp_scp_portals_login PARTITION(partition_date = '${env:START_DATE}')
SELECT
  legacy_company
  , account_number
  , event_type
  , instances
  , scp_flag
  , wifi_flag
  , internet_flag
  , future_connect_flag
  , customer_type
  , account_type
  , wifi_customer_type
  , account_key
FROM `${env:SNAP}`
WHERE partition_date = '${env:START_DATE}'
  AND LOWER(application_name) = 'myspectrum'
  AND login_flag
DISTRIBUTE BY '${env:START_DATE}'
;