USE ${env:DASP_db};

SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=2048000000;
SET hive.merge.size.per.task=2048000000;

INSERT OVERWRITE TABLE asp_scp_portals_actions_agg PARTITION(data_utc_dt = '${env:START_DATE}')
SELECT
  action_string
  , msa_scp_flag
  , wifi_customer_type
  , 'daily' AS grain
  , SUM(action_cnt) AS total
  , COUNT(CONCAT(acct_number_enc, sys_enc, prin_enc, agent_enc)) AS distinct_accounts
FROM asp_scp_portals_acct_agg
WHERE data_utc_dt = '${env:START_DATE}'
GROUP BY
  action_string
  , msa_scp_flag
  , wifi_customer_type
UNION
SELECT
  action_string
  , msa_scp_flag
  , wifi_customer_type
  , 'rolling_30' grain
  , SUM(action_cnt) AS total
  , COUNT(DISTINCT CONCAT(acct_number_enc, sys_enc, prin_enc, agent_enc)) AS distinct_accounts
FROM asp_scp_portals_acct_agg
WHERE data_utc_dt BETWEEN '${env:ROLLING_30_START_DATE}' AND '${env:START_DATE}'
GROUP BY
  action_string
  , msa_scp_flag
  , wifi_customer_type
;

