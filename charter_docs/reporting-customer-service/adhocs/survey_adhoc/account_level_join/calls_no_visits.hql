SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT DISTINCT 
v.account_number as v_acct
, c.account_number as c_acct
, cv.account_number as cv_acct

FROM 
prod_tmp.cs_care_events v
OUTER JOIN prod.cs_calls_with_prior_visit cv
 on prod.aes_decrypt256(cv.account_number)=prod.aes_decrypt(v.account_number)
OUTER JOIN prod.cs_call_data c
 on c.account_number=cv.account_number
WHERE partition_date_utc>='2019-04-18'
AND cv.account_number is null
limit 10
;
