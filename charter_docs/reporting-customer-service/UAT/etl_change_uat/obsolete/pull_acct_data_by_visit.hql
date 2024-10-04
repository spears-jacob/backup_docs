SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;


SELECT DISTINCT 
account_number
,prod.aes_decrypt256(account_number) 256_decryption
, prod.aes_decrypt(account_number) 128_decryption
, call_inbound_key
FROM
test.cs_call_care_data_amo
WHERE 
call_end_date_utc>='2019-06-01' AND
--account_number is not NULL
 call_inbound_key='2061527021637'
limit 100
;

--SELECT * FROM test.cs_call_care_data_amo WHERE call_inbound_key='2061504867540';

SELECT DISTINCT account_number, call_inbound_key 
FROM prod.cs_call_data 
WHERE call_end_date_utc>='2019-06-01'
AND call_inbound_key='2061527021637'
;

SELECT DISTINCT account_number
FROM test.cs_call_care_data_amo
WHERE call_end_date_utc>='2019-06-01'
AND call_inbound_key='2061527021637'
;

SELECT DISTINCT account_number
FROM test.cs_calls_with_prior_visits_amo
--WHERE call_end_date_utc>='2019-06-01'
 WHERE call_inbound_key='2061527021637'
;

