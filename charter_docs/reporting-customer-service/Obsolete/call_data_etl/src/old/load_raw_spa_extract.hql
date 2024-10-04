

DROP TABLE IF EXISTS dev_tmp.steve_call_data_p270_spa_raw;
CREATE TABLE IF NOT EXISTS dev_tmp.steve_call_data_p270_spa_raw
(
  callinbkey string
  ,acctnum string
  ,acctkey string
  ,sys_site string
  ,comp_prin string
  ,division string
  ,fran_agent string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'serialization.null.format'='');
;


LOAD DATA LOCAL INPATH '/data/tmp/CARE_Extract_for_*.txt' OVERWRITE INTO TABLE DEV_TMP.steve_CALL_DATA_P270_SPA_RAW;

--set hive.auto.convert.join=false;



--CREATE CS_CUSTOMER_TYPE
DROP TABLE IF EXISTS dev.steve_customer_type;
CREATE TABLE IF NOT EXISTS DEV.steve_customer_type
(
  franchise_unique_id string
  ,account_number string
  ,account_id string
  ,customer_type string
  ,customer_subtype string
)
PARTITIONED BY
(
  company_code string
  ,partition_date string
)
;


--CREATE CS_CALL_DATA_P270_SPA
----Deduping records with the same call_inbound_key and creating the franchise_unique_id
DROP TABLE IF EXISTS DEV.steve_CALL_DATA_P270_SPA;
CREATE TABLE DEV.steve_CALL_DATA_P270_SPA AS
SELECT concat(lpad(sys_site,4,'0'),lpad(company_principal,4,'0')
  ,case when division = '?' or division is null then lpad(franchise_agent,4,'0') else lpad(division,4,'0') end
  ,lpad(franchise_agent,4,'0')) franchise_unique_id
  ,*
FROM
(
SELECT callinbkey call_inbound_key, acctnum account_number, acctkey account_key, sys_site, comp_prin company_principal, division, fran_agent franchise_agent
,ROW_NUMBER() OVER (PARTITION BY CALLINBKEY ORDER BY ACCTNUM) RANKING2
FROM
(
  SELECT DISTINCT callinbkey, acctnum, acctkey, sys_site, comp_prin, division, fran_agent
  ,ROW_NUMBER() OVER (PARTITION BY CallInbKey ORDER BY CASE WHEN lower(sys_site) = 'unknown' THEN 99999999 ELSE sys_site END ASC) ranking
  FROM DEV_TMP.steve_CALL_DATA_P270_SPA_RAW
  --WHERE acctnum <> 'Unknown'
) dt
WHERE RANKING = 1
) DT
WHERE RANKING2 = 1
;
