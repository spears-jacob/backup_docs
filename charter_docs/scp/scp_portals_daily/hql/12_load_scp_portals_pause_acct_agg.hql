USE ${env:DASP_db};


SET hivevar:manual_pause_only='manual';
SET hivevar:schedule_pause_only= 'schedule';
SET hivevar:manual_and_schedule_pause_only='both';
SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;

INSERT OVERWRITE TABLE asp_scp_portals_pause_acct_agg PARTITION (data_utc_dt = '${env:START_DATE}')
SELECT
  COUNT(DISTINCT acct_number) AS account_cnt
  , CASE WHEN has_instances AND has_cnt THEN
    ${hivevar:manual_and_schedule_pause_only}
  ELSE
    CASE WHEN has_instances THEN
      ${hivevar:manual_pause_only}
    ELSE
      ${hivevar:schedule_pause_only}
    END
  END AS pause_type
  , grain
  , wifi_customer_type
FROM (
  SELECT
    acct_number
    , SUM(instances) > 0 has_instances
    , SUM(act_cnt) > 0 has_cnt
    , wifi_customer_type
    , 'daily' AS grain
  FROM `${env:PAUSE}`
  WHERE data_utc_dt = '${env:START_DATE}'
  GROUP BY acct_number
         , wifi_customer_type
) x
GROUP BY
  CASE WHEN has_instances AND has_cnt THEN
    ${hivevar:manual_and_schedule_pause_only}
  ELSE
    CASE WHEN has_instances THEN
      ${hivevar:manual_pause_only}
    ELSE
      ${hivevar:schedule_pause_only}
    END
  END
  , wifi_customer_type
  , grain
DISTRIBUTE BY '${env:START_DATE}'
;