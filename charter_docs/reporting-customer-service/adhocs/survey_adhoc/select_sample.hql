SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT 
 account_number
, '1' as category_indicator
FROM dev_tmp.cs_adhoc_category_1
--WHERE rand() <= '.0001' -- make hive conf
distribute by rand()
sort by rand()
limit 600000; --make hiveconf

SELECT 
 account_number
, '2' as category_indicator
FROM dev_tmp.cs_adhoc_category_2
--WHERE rand() <= '.001'
distribute by rand()
sort by rand()
limit 150000;

SELECT 
 account_number 
, '3' as category_indicator
FROM dev_tmp.cs_adhoc_category_3
--WHERE rand() <= '.001'
distribute by rand()
sort by rand()
limit 150000;

SELECT 
 account_number
, '4' as category_indicator
FROM dev_tmp.cs_adhoc_category_4
--WHERE rand() <= '.001'
distribute by rand()
sort by rand()
limit 150000;

