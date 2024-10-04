


--Load BHN account attributes
INSERT INTO DEV.steve_CUSTOMER_TYPE PARTITION (company_code, partition_date)
SELECT DISTINCT CONCAT(LPAD(site_id, 4, '0'), LPAD(system__company, 4, '0'), LPAD(system__division, 4, '0'), LPAD(system__franchise, 4, '0')) SPA
,SUBSTRING(prod.aes_decrypt256(account__number_aes256),4)
,prod.aes_decrypt256(account__id_aes256)
,CUSTOMER__TYPE
,CASE
WHEN meta__file_type = 'Commercial Business' and smb_fl = 1 then 'SMB'
WHEN meta__file_type = 'Commercial Business' and smb_fl = 0 then 'Enterprise'
WHEN meta__file_type = 'Residential' then 'Residential'
ELSE 'Unmapped'
END
as customer__subtype
,'BHN' as company_code
,PARTITION_DATE
FROM PROD.bhn_ACCOUNT_HISTORY
WHERE partition_date >= '2016-01-01' AND partition_date <= '2018-12-31'
;

INSERT INTO DEV.steve_CUSTOMER_TYPE PARTITION (company_code, partition_date)
SELECT DISTINCT CONCAT(LPAD(site_id, 4, '0'), LPAD(system__company, 4, '0'), LPAD(system__division, 4, '0'), LPAD(system__franchise, 4, '0')) SPA
,SUBSTRING(prod.aes_decrypt256(account__number_aes256),4)
,prod.aes_decrypt256(account__id_aes256)
,CUSTOMER__TYPE
,CASE
WHEN meta__file_type = 'Commercial Business' and smb_fl = 1 then 'SMB'
WHEN meta__file_type = 'Commercial Business' and smb_fl = 0 then 'Enterprise'
WHEN meta__file_type = 'Residential' then 'Residential'
ELSE 'Unmapped'
END
as customer__subtype
,'BHN' as company_code
,curr.PARTITION_DATE
FROM PROD.bhn_ACCOUNT_CURRENT curr
LEFT JOIN dev.steve_customer_type ct
  ON prod.aes_decrypt256(curr.account__number_aes256) = ct.account_number
  AND curr.partition_date = ct.partition_date
WHERE curr.partition_date >= '2016-01-01' AND curr.partition_date <= '2017-12-31'
AND ct.account_number IS NULL
;
