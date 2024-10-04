--Finish creating the SPA and Customer_Type dataset to join to the call data
--CREATE CS_SPA_TO_CALLS
DROP TABLE IF EXISTS dev.cs_spa_to_calls;
CREATE TABLE dev.cs_spa_to_calls AS
SELECT DISTINCT franchise_unique_id

--TRYING LOGIC WITH ACCOUNT_NUMBER FROM CALL_DATA
,dev.aes_decrypt256(cd.account_number) account_number
,SPA.CALL_INBOUND_KEY
,to_date(CD.CALL_END_DATETIME_UTC) CALL_END_DATE_UTC
FROM DEV.CS_CALL_DATA_P270_SPA SPA
  LEFT JOIN DEV_tmp.cs_call_data_p270_tmp CD
    ON SPA.CALL_INBOUND_KEY = CD.CALL_INBOUND_KEY
;


--Joining the SPA Extract from BI to Accounts from *Account_history tables
DROP TABLE IF EXISTS dev.cs_spa_calls_and_customer_types;
CREATE TABLE dev.cs_spa_calls_and_customer_types as
SELECT DISTINCT spa.franchise_unique_id, spa.account_number, spa.call_inbound_key, spa.call_end_date_utc
,cs.franchise_unique_id cs_franchise_unique_id, cs.account_number cs_account_number
,COALESCE(cs.customer_type,ranks.customer_type) cs_customer_type
,COALESCE(cs.customer_subtype,ranks.customer_subtype) cs_customer_subtype, cs.partition_date cs_partition_date
FROM DEV.CS_SPA_TO_CALLS SPA
  LEFT JOIN
  (
    SELECT *
    FROM
    (
      --Grabbing the proper record based on the first Account_number, franchise_unique_id, and
      --partition_date. Ordering by account_id to get the rank.
      SELECT ACCOUNT_NUMBER, FRANCHISE_UNIQUE_ID, PARTITION_DATE, CUSTOMER_TYPE, CUSTOMER_SUBTYPE
      ,ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER, FRANCHISE_UNIQUE_ID, PARTITION_DATE ORDER BY ACCOUNT_ID) RANKING
      FROM DEV.CS_CUSTOMER_TYPE
    ) DT
    WHERE RANKING = 1
  ) CS
    ON SPA.FRANCHISE_UNIQUE_ID = CS.FRANCHISE_UNIQUE_ID
    AND CAST(SPA.ACCOUNT_NUMBER AS BIGINT) = CS.ACCOUNT_NUMBER
    AND SPA.CALL_END_DATE_UTC = CS.PARTITION_DATE
  LEFT JOIN
  (
    SELECT * FROM
    (
      --If no match on the previous LEFT JOIN, then simply order by the most recent partition_date
      --This ensures we get at least 1 record matched if the partition_dates do not align
      SELECT ACCOUNT_NUMBER, FRANCHISE_UNIQUE_ID, CUSTOMER_TYPE CUSTOMER_TYPE, CUSTOMER_SUBTYPE customer_subtype
      ,ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER, FRANCHISE_UNIQUE_ID ORDER BY PARTITION_DATE DESC) RANKING
      FROM DEV.CS_CUSTOMER_TYPE
    ) DT
    WHERE RANKING = 1
  ) RANKS
    ON SPA.FRANCHISE_UNIQUE_ID = RANKS.FRANCHISE_UNIQUE_ID
    AND CAST(SPA.ACCOUNT_NUMBER AS BIGINT) = RANKS.ACCOUNT_NUMBER
WHERE CAST(SPA.ACCOUNT_NUMBER AS BIGINT) IS NOT NULL
;
