


--Load TWC account attributes
INSERT INTO DEV.steve_CUSTOMER_TYPE PARTITION (company_code, partition_date)
SELECT DISTINCT CONCAT(LPAD(system__sys, 4, '0'), LPAD(system__prin, 4, '0'), LPAD(system__agent, 4, '0'), LPAD(franchise__agent_cd, 4, '0')) SPA
,prod.aes_decrypt256(account__number_aes256)
,prod.aes_decrypt256(account__id_aes256)
,CUSTOMER__TYPE
,CASE
WHEN meta__file_type = 'Commercial Business' and smb_fl = 1 then 'SMB'
WHEN meta__file_type = 'Commercial Business' and smb_fl = 0 then 'Enterprise'
WHEN meta__file_type = 'Residential' then 'Residential'
ELSE 'Unmapped'
END
as customer__subtype
,'TWC' AS company_code
,PARTITION_DATE
FROM PROD.TWC_ACCOUNT_HISTORY
WHERE partition_date >= '2018-01-01' AND partition_date <= '2018-12-31'
;

INSERT INTO DEV.steve_CUSTOMER_TYPE PARTITION (company_code, partition_date)
SELECT DISTINCT CONCAT(LPAD(system__sys, 4, '0'), LPAD(system__prin, 4, '0'), LPAD(system__agent, 4, '0'), LPAD(franchise__agent_cd, 4, '0')) SPA
,prod.aes_decrypt256(account__number_aes256) account_number
,prod.aes_decrypt256(account__id_aes256)
,CUSTOMER__TYPE
,CASE
WHEN meta__file_type = 'Commercial Business' and smb_fl = 1 then 'SMB'
WHEN meta__file_type = 'Commercial Business' and smb_fl = 0 then 'Enterprise'
WHEN meta__file_type = 'Residential' then 'Residential'
ELSE 'Unmapped'
END
as customer__subtype
,'TWC' AS company_code
,curr.PARTITION_DATE
FROM PROD.TWC_ACCOUNT_CURRENT curr
  LEFT JOIN dev.steve_customer_type ct
    ON prod.aes_decrypt256(curr.account__number_aes256) = ct.account_number
    AND curr.partition_date = ct.partition_date
WHERE ct.account_number IS NULL
;
