USE ${env:DASP_db};

SET hive.merge.tezfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=4096000000;
SET hive.merge.size.per.task=4096000000;

-- prepare temp tables for all-local processing
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_firmware_${env:CLUSTER}_${env:STEP};
CREATE TABLE ${env:TMP_db}.scp_pa_firmware_${env:CLUSTER}_${env:STEP} AS
  SELECT DISTINCT
    account_key
    , equipment_model
    , firmware_arr[0] AS equipment_firmware
  FROM `${env:SPAT}`
  WHERE data_den_dt = '${env:START_DATE}'
;

DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_billing_acct_equip_tenure_${env:CLUSTER}_${env:STEP};
CREATE TABLE ${env:TMP_db}.scp_pa_billing_acct_equip_tenure_${env:CLUSTER}_${env:STEP} AS
  SELECT
  encrypted_account_key_256
  , CASE
      WHEN account_tenure <= 30 THEN '0-30'
      WHEN account_tenure <= 60 THEN '31-60'
      WHEN account_tenure <= 90 THEN '61-90'
      WHEN account_tenure >90 THEN '90+'
  END AS account_tenure
  , CASE
      WHEN SIZE(equipment_model) > 1 THEN 'Multiple'
      ELSE equipment_model[0]
  END AS equipment_model
  , dma
  , wifi_customer_type
  , partition_date
  FROM
  (
  SELECT
  encrypted_account_key_256
  , MIN(DATEDIFF(partition_date,equip_first_connected_date)) as account_tenure
  , COLLECT_SET(NVL(equipment_model,'Unknown')) AS equipment_model
  , dma
  , CASE WHEN wifi_customer_type IS NULL AND partition_date <='2022-09-28' THEN 'AHW'
         ELSE wifi_customer_type
         END AS wifi_customer_type
  , partition_date
  FROM `${env:BAES}`
  WHERE partition_date = '${env:START_DATE}'
  AND (is_scp_router)
  AND subscriber_type = 'reportable_scp_subscriber'
  GROUP BY
    encrypted_account_key_256
    , dma
    , wifi_customer_type
    , partition_date
  ) calculate_tenure
;

DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_msa_visitation_${env:CLUSTER}_${env:STEP};
CREATE TABLE ${env:TMP_db}.scp_pa_msa_visitation_${env:CLUSTER}_${env:STEP} AS
SELECT
  baet.equipment_model
  , baet.account_tenure
  , baet.dma
  , COALESCE(f.equipment_firmware,'Unknown') AS equipment_firmware
  , COALESCE(CAST(lkp.is_cujo_lens_enabled AS BOOLEAN),FALSE) AS is_cujo_lens_enabled
  , COUNT(DISTINCT baet.encrypted_account_key_256) AS customer_cnt
  , COUNT(DISTINCT pa.account_number) AS accounts_w_login
  , COUNT(DISTINCT CASE WHEN pa.event_type = 'portals_equipment_list_view' AND COALESCE(instances,0) > 0 THEN pa.account_key ELSE NULL END) AS equip_page_account_cnt
  , baet.wifi_customer_type
FROM ${env:TMP_db}.scp_pa_billing_acct_equip_tenure_${env:CLUSTER}_${env:STEP} baet
LEFT JOIN `${env:LOGIN}` pa
  ON pa.account_key = baet.encrypted_account_key_256
  AND pa.partition_date = '${env:START_DATE}'
LEFT JOIN ${env:TMP_db}.scp_pa_firmware_${env:CLUSTER}_${env:STEP} f
  ON baet.encrypted_account_key_256 = f.account_key
  AND baet.equipment_model = f.equipment_model
LEFT JOIN `${env:LKP}` lkp
  ON f.equipment_firmware = lkp.model_num
  AND lkp.cloud = 'prod'
GROUP BY
  baet.equipment_model
  , baet.account_tenure
  , baet.dma
  , COALESCE(f.equipment_firmware,'Unknown')
  , COALESCE(CAST(lkp.is_cujo_lens_enabled AS BOOLEAN),FALSE)
  , baet.wifi_customer_type
;

--run enrichment
INSERT OVERWRITE TABLE asp_scp_portals_visitation_rates PARTITION(data_den_dt = '${env:START_DATE}')
SELECT
  account_tenure
  , equipment_model
  , equipment_firmware
  , dma
  , is_cujo_lens_enabled
  , customer_cnt
  , COALESCE(accounts_w_login,0) AS accounts_w_login
  , COALESCE(equip_page_account_cnt,0) AS equip_page_account_cnt
  , wifi_customer_type
FROM ${env:TMP_db}.scp_pa_msa_visitation_${env:CLUSTER}_${env:STEP}
DISTRIBUTE BY '${env:START_DATE}'
;

-- drop/delete temp tables
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_firmware_${env:CLUSTER}_${env:STEP} ;
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_billing_acct_equip_tenure_${env:CLUSTER}_${env:STEP} ;
DROP TABLE IF EXISTS ${env:TMP_db}.scp_pa_msa_visitation_${env:CLUSTER}_${env:STEP} ;