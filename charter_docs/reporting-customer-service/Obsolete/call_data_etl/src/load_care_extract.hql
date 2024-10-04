

DROP TABLE IF EXISTS DEV.CS_CALL_DATA_P270_SPA;
CREATE TABLE DEV.CS_CALL_DATA_P270_SPA AS
SELECT *
FROM
(
SELECT callinbkey call_inbound_key, acctnum account_number, acctkey account_key, sys_site, comp_prin company_principal, division, fran_agent franchise_agent
,RANK() OVER (PARTITION BY CALLINBKEY ORDER BY ACCTNUM) RANKING2
FROM
(
  SELECT DISTINCT callinbkey, acctnum, acctkey, sys_site, comp_prin, division, fran_agent
  ,RANK() OVER (PARTITION BY CallInbKey ORDER BY CASE WHEN sys_site = 'Unknown' THEN 99999999 ELSE sys_site END ASC) ranking
  FROM DEV_TMP.CS_CALL_DATA_P270_SPA_RAW
) dt
WHERE RANKING = 1
) DT
WHERE RANKING2 = 1
;
