DROP TABLE IF EXISTS dev_tmp.cs_adhoc_visit_no_call;
DROP TABLE IF EXISTS dev_tmp.cs_adhoc_call_no_visit;
DROP TABLE IF EXISTS dev_tmp.cs_adhoc_call_and_visit;

CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_visit_no_call(
    v_acct STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;

CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_call_no_visit(
    c_acct STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;

CREATE TABLE IF NOT EXISTS dev_tmp.cs_adhoc_call_and_visit(
    cv_acct STRING
)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;

FROM (
SELECT *
FROM
 dev_tmp.cs_adhoc_calls_with_prior_visit cv
FULL OUTER JOIN dev_tmp.cs_adhoc_calls_last_month c
 on prod.aes_decrypt256(c.c_acct)=prod.aes_decrypt256(cv.cv_acct) AND c.call_date=cv.call_end_date_utc
FULL OUTER JOIN dev_tmp.cs_adhoc_visits_last_month v
 on prod.aes_decrypt(v_acct)=prod.aes_decrypt256(cv_acct) AND v.visit_start=cv.visitstart
) m

INSERT INTO TABLE dev_tmp.cs_adhoc_visit_no_call
SELECT v_acct
WHERE c_acct IS NULL
AND cv_acct IS NULL

INSERT INTO TABLE dev_tmp.cs_adhoc_call_no_visit
SELECT c_acct
WHERE v_acct IS NULL
AND cv_acct IS NULL

INSERT INTO TABLE dev_tmp.cs_adhoc_call_and_visit
SELECT cv_acct
WHERE cv_acct IS NOT NULL
;

SELECT * from dev_tmp.cs_adhoc_visit_no_call limit 10;
SELECT * FROM dev_tmp.cs_adhoc_call_no_visit limit 10;
SELECT * FROM dev_tmp.cs_adhoc_call_and_visit limit 10;
