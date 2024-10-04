SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT DISTINCT call_inbound_key, call_start_timestamp_utc
FROM prod.cs_call_data
WHERE call_end_date_utc in ('2019-06-14','2019-06-13','2019-06-15')
AND prod.aes_decrypt256(account_number)=prod.aes_decrypt('tNWLtNAu23TFKziBWvzhuw==')
