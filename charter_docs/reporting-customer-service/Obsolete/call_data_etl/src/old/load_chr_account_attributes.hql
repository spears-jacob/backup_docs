


--Load CHR account attributes
INSERT INTO DEV.steve_CUSTOMER_TYPE PARTITION (company_code, partition_date)
SELECT spa, account_number, account_id, customer__type, customer__subtype, company_code, partition_date
FROM
(
  SELECT spa, account_number, account_id, customer__type, customer__subtype, company_code, partition_date
  ,RANK() OVER (PARTITION BY SPA, ACCOUNT_NUMBER ORDER BY CASE WHEN META__FILE_TYPE = CUSTOMER__TYPE THEN 1 ELSE 2 END, CUSTOMER__TYPE) RANKING
  FROM
    (
      SELECT DISTINCT CONCAT(LPAD(system__sys, 4, '0'), LPAD(system__prin, 4, '0'), LPAD(system__agent, 4, '0'), LPAD(system__agent, 4, '0')) SPA
      ,prod.aes_decrypt256(account__number_aes256) account_number
      ,prod.aes_decrypt256(account__id_aes256) account_id
      ,customer__type
      ,customer__subtype
      ,'CHR' as company_code
      ,to_DATE(partition_date_time) partition_date
      ,CASE WHEN META__FILE_TYPE = 'Commercial Business' THEN 'Commercial' ELSE META__FILE_TYPE END meta__file_type
      FROM PROD.ACCOUNT_history
      WHERE partition_date_time >= '2018-01-01' AND partition_date_time <= '2018-12-31'
    ) DT
) DT
WHERE RANKING = 1
;

CREATE TEMPORARY TABLE dev.steve_chr_customer_types_1
AS
SELECT DISTINCT CONCAT(LPAD(system__sys, 4, '0'), LPAD(system__prin, 4, '0'), LPAD(system__agent, 4, '0'), LPAD(system__agent, 4, '0')) SPA
,prod.aes_decrypt256(account__number_aes256) account_number
,prod.aes_decrypt256(account__id_aes256) account_id
,customer__type
,customer__subtype
,'CHR' as company_code
,to_DATE(partition_date_time) partition_date
,CASE WHEN META__FILE_TYPE = 'Commercial Business' THEN 'Commercial' ELSE META__FILE_TYPE END meta__file_type
FROM PROD.ACCOUNT_CURRENT
WHERE partition_date_time >= '2016-01-01' AND partition_date_time <= '2018-12-31'
;

CREATE TEMPORARY TABLE dev.steve_chr_customer_types
AS
SELECT spa, account_number, account_id, customer__type customer_type, customer__subtype customer_subtype, company_code
,RANK() OVER (PARTITION BY SPA, ACCOUNT_NUMBER ORDER BY CASE WHEN META__FILE_TYPE = CUSTOMER__TYPE THEN 1 ELSE 2 END, CUSTOMER__TYPE) RANKING
,partition_date
FROM dev.steve_chr_customer_types_1
;

INSERT INTO DEV.steve_CUSTOMER_TYPE PARTITION (company_code, partition_date)
SELECT curr.spa, curr.account_number, curr.account_id, curr.customer_type, curr.customer_subtype, curr.company_code, curr.partition_date
FROM dev.steve_chr_customer_types curr
LEFT JOIN dev.steve_customer_type ct
  ON curr.account_number = ct.account_number
  AND curr.partition_date = ct.partition_date
WHERE ct.account_number IS NULL
AND RANKING = 1
;
