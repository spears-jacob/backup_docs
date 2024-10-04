SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT 
 account_number as visits_no_call_accounts
FROM dev_tmp.cs_adhoc_category_1
WHERE rand() <= '.0001' -- make hive conf
distribute by rand()
sort by rand()
limit 6; --make hiveconf

SELECT 
 account_number as calls_and_visits_accounts
FROM dev_tmp.cs_adhoc_category_2
WHERE rand() <= '.001'
distribute by rand()
sort by rand()
limit 6;

SELECT 
 account_number as calls_no_visit_accounts
FROM dev_tmp.cs_adhoc_category_3
WHERE rand() <= '.001'
distribute by rand()
sort by rand()
limit 6;

SELECT 
 account_number as multiple_category_accounts
FROM dev_tmp.cs_adhoc_category_4
WHERE rand() <= '.001'
distribute by rand()
sort by rand()
limit 6;

