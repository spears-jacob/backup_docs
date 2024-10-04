SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;


SELECT DISTINCT
lower(visit__application_details__application_name) as visit_portal
, visit__visit_id as visit_id
FROM prod.venona_events_portals
WHERE 
 partition_date_utc='2019-04-25'
 AND prod.aes_decrypt(visit__account__account_number) in 
('8781100930151473','8448400730810319','476190902','8345782030080121','177689006','8260170540476511')
;

SELECT DISTINCT
account_number as call_account
FROM prod.cs_call_data
WHERE prod.aes_decrypt256(account_number) in 
('8781100930151473','8448400730810319','476190902','8345782030080121','177689006','8260170540476511')
  AND call_end_date_utc = '2019-04-25'
;

SELECT DISTINCT
account_number as both_account
FROM prod.cs_calls_with_prior_visit
WHERE prod.aes_decrypt256(account_number) in 
('8781100930151473','8448400730810319','476190902','8345782030080121','177689006','8260170540476511')
  AND call_end_date_utc='2019-04-25'
;
