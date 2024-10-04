SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

DROP TABLE IF EXISTS dev_tmp.cs_adhoc_calls_last_month;
CREATE TABLE dev_tmp.cs_adhoc_calls_last_month
AS
SELECT
account_number as c_acct
, call_inbound_key as call_id
, max(call_end_date_utc) as call_date
FROM prod.cs_call_data
WHERE
segment_handled_flag=true
AND enhanced_account_number=0
AND call_end_date_utc BETWEEN '2019-04-01' AND '2019-04-30' -- change date here
AND upper(customer_type)='RESIDENTIAL'
GROUP BY account_number, call_inbound_key
;
