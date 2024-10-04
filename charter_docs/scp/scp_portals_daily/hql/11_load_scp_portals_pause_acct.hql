USE ${env:DASP_db};

SET hive.auto.convert.join=false;
SET hive.optimize.sort.dynamic.partition=false;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;

INSERT OVERWRITE TABLE asp_scp_portals_pause_acct PARTITION(data_utc_dt = '${env:START_DATE}')
SELECT
  COALESCE(s.acct_number_enc,m.account_number) as acct_number
  , s.action_cnt
  , m.instances
  , s.action_string
  , m.event_type
  , s.wifi_customer_type
FROM
(
SELECT
acct_number_enc
, action_string
, action_cnt
, wifi_customer_type
FROM ${env:ACCAGG}
WHERE data_utc_dt = '${env:START_DATE}'
AND action_string = 'SCP:Create Pause Schedule Save:true'
AND action_cnt > 0
) s
  FULL JOIN
    (
      SELECT
        account_number
        , SUM(instances) AS instances
        , event_type
      FROM `${env:SNAP}`
      WHERE partition_date = '${env:START_DATE}'
        AND event_type IN ('portals_start_pause_device','portals_start_unpause_device')
        AND instances > 0
      GROUP BY
        account_number
        , event_type
    ) m
  ON s.acct_number_enc = m.account_number
DISTRIBUTE BY '${env:START_DATE}'
;