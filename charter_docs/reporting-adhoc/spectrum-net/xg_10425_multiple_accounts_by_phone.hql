-----------------------------------------------------------------
-- JIRA TICKET XGANALYTIC-10425
-- Yana Fishman would like to see how many separate account numbers share a phone number for Q4 2017
-----------------------------------------------------------------

--Set timeframe parameters:
SET start_date=${hiveconf:start_date};
SET end_date=${hiveconf:end_date};

-----------------------------------------------------------------
--- CREATE SOURCE TABLE WITH UNIQUE PHONE NUMBERS AND ACCOUNTS --
-----------------------------------------------------------------

DROP TABLE IF EXISTS dev.jm_dup_phones;
CREATE TABLE IF NOT EXISTS dev.jm_dup_phones AS
--Charter
SELECT DISTINCT
a.account__number_aes AS account_number
,a.account__phone_number_aes AS phone_number
,'Charter' AS footprint
FROM prod.account_history a
WHERE customer__subtype = 'Residential'
AND account__type = 'SUBSCRIBER'
AND customer__disconnect_date IS NULL
AND partition_date_time BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
    UNION
    --TWC
    SELECT DISTINCT
    t.account__number_aes AS account_number
    ,t.account__phone_number_aes AS phone_number
    ,'TWC' AS footprint
    FROM prod.twc_account_history t
    WHERE t.smb_fl <> true
    AND account__type = 'Non-Employee'
    AND customer__disconnect_date IS NULL
    AND partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
            UNION
            --BHN
            SELECT DISTINCT
            substr (b.account__number_aes,4) AS account_number
            ,b.account__phone_number_aes AS phone_number
            ,'BHN' AS footprint
            FROM prod.bhn_account_history b
            WHERE b.smb_fl <> true
            AND account__type IN ('Non-Employee', 'Non-Emplyee')
            AND customer__disconnect_date IS NULL
            AND partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
;

--Find unique phone numbers for the timeframe to communicate to business owner:
SELECT
footprint,
COUNT(DISTINCT(phone_number)) AS unique_phone_number
FROM dev.jm_dup_phones
GROUP BY
footprint
;

--Find unique phone numbers associated with only one account number
SELECT
footprint,
SUM(IF(ct_acct = 1,1,0)) AS phone_w_1_acct
FROM
(
  SELECT
  footprint,
  phone_number,
  COUNT(DISTINCT(account_number)) AS ct_acct
  FROM
  dev.jm_dup_phones
  GROUP BY
  footprint,
  phone_number
  HAVING
  COUNT(DISTINCT(account_number)) = 1
) a
GROUP BY
footprint
;

--Find unique phone numbers associated with only more than 15 account numbers
SELECT
footprint,
SUM(IF(ct_acct > 15,1,0)) AS phone_w_16plus_accts
FROM
(
  SELECT
  footprint,
  phone_number,
  COUNT(DISTINCT(account_number)) AS ct_acct
  FROM
  dev.jm_dup_phones
  GROUP BY
  footprint,
  phone_number
  HAVING
  COUNT(DISTINCT(account_number)) > 15
) a
GROUP BY
footprint
;

-----------------------------------------------------------------
------- CREATE TABLE SHOWING PHONE#S OVER MULTIPLE ACCTS --------
-----------------------------------------------------------------

DROP TABLE IF EXISTS dev.jm_resi_dups_by_phone;
CREATE TABLE IF NOT EXISTS dev.jm_resi_dups_by_phone AS
SELECT
footprint,
phone_number,
COUNT(DISTINCT(account_number)) AS ct_acct
FROM
dev.jm_dup_phones
GROUP BY
footprint,
phone_number
HAVING
(COUNT(DISTINCT(account_number)) > 1 AND COUNT(DISTINCT(account_number)) < 15)
;

-----------------------------------------------------------------
------ CREATE BUCKETS OF PHONE#S WITH SPECIFIC ACCT COUNTS ------
-----------------------------------------------------------------

DROP TABLE IF EXISTS dev.jm_resi_dups_by_phone_agg;
CREATE TABLE IF NOT EXISTS dev.jm_resi_dups_by_phone_agg AS
SELECT
footprint,
phone_number,
SUM(IF(ct_acct = 2,1,0)) AS phone_w_2_accts,
SUM(IF(ct_acct = 3,1,0)) AS phone_w_3_accts,
SUM(IF(ct_acct = 4,1,0)) AS phone_w_4_accts,
SUM(IF(ct_acct > 4,1,0)) AS phone_w_5plus_accts
FROM dev.jm_resi_dups_by_phone
WHERE phone_number not IN ('0000000000','1111111111','1234567890','2010000000','9990000000', '9999999999')
GROUP BY
footprint,
phone_number
;

-----------------------------------------------------------------
------------------- SUM MULTIPLE ACCT BUCKETS -------------------
-----------------------------------------------------------------

SELECT
footprint,
SUM(phone_w_2_accts) AS phone_w_2_accts_tot,
SUM(phone_w_3_accts) AS phone_w_3_accts_tot,
SUM(phone_w_4_accts) AS phone_w_4_accts_tot,
SUM(phone_w_5plus_accts) AS phone_w_5plus_accts_tot
FROM dev.jm_resi_dups_by_phone_agg GROUP BY footprint
;
