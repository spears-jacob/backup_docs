SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--
--Make sure needed tables exist and are empty
--
DROP TABLE IF EXISTS dev_tmp.cs_adhoc_category_1;
CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_category_1(
    account_number STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;

DROP TABLE IF EXISTS dev_tmp.cs_adhoc_category_2;
CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_category_2(
    account_number STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;

DROP TABLE IF EXISTS dev_tmp.cs_adhoc_category_3;
CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_category_3(
    account_number STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;

DROP TABLE IF EXISTS dev_tmp.cs_adhoc_category_4;
CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_category_4(
    account_number STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;


--
-- For visits without calls, determine whether that account has another interaction that falls into a different category
--
FROM (
SELECT DISTINCT
 *
FROM dev_tmp.cs_adhoc_visit_no_call v
LEFT JOIN dev_tmp.cs_adhoc_call_no_visit c 
  on prod.aes_decrypt(v_acct)=prod.aes_decrypt256(c_acct)
LEFT JOIN dev_tmp.cs_adhoc_call_and_visit cv
  on prod.aes_decrypt(v_acct)=prod.aes_decrypt256(cv_acct)
) lv

INSERT INTO TABLE dev_tmp.cs_adhoc_category_1
SELECT prod.aes_decrypt(v_acct) as account_number
WHERE
 cv_acct IS NULL
 AND c_acct IS NULL

INSERT INTO TABLE dev_tmp.cs_adhoc_category_4
SELECT prod.aes_decrypt(v_acct) as account_number
WHERE
 cv_acct IS NOT NULL
 OR c_acct IS NOT NULL
;


--
-- For calls without visits, determine whether that account has another interaction
--
FROM (
SELECT DISTINCT 
 *
FROM dev_tmp.cs_adhoc_call_no_visit c
LEFT JOIN dev_tmp.cs_adhoc_visit_no_call v
 on prod.aes_decrypt256(c_acct)=prod.aes_decrypt(v_acct)
LEFT JOIN dev_tmp.cs_adhoc_call_and_visit cv
 on prod.aes_decrypt256(c_acct)=prod.aes_decrypt256(cv_acct)
) lc

INSERT INTO TABLE dev_tmp.cs_adhoc_category_3
SELECT prod.aes_decrypt256(c_acct) as account_number
WHERE 
 cv_acct IS NULL
 AND v_acct IS NULL

INSERT INTO TABLE dev_tmp.cs_adhoc_category_4
SELECT prod.aes_decrypt256(c_acct) as account_number
WHERE
 cv_acct IS NOT NULL
 AND v_acct IS NULL
;

--
-- for calls with visits, pull out accounts that have had another interaction
--
FROM (
SELECT DISTINCT
*
FROM dev_tmp.cs_adhoc_call_and_visit cv
LEFT JOIN dev_tmp.cs_adhoc_visit_no_call v
 on prod.aes_decrypt(v_acct)=prod.aes_decrypt256(cv_acct)
LEFT JOIN dev_tmp.cs_adhoc_call_no_visit c
 on prod.aes_decrypt256(c_acct)=prod.aes_decrypt256(cv_acct)
) lcv

INSERT INTO TABLE dev_tmp.cs_adhoc_category_2
SELECT prod.aes_decrypt256(cv_acct) as account_number
WHERE 
 c_acct IS NULL
 AND v_acct IS NULL
;
