SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

SELECT
old.call_inbound_key
, new.call_inbound_key
, old.segment_id
, new.segment_id
, prod.aes_decrypt(old.account_number)
, prod.aes_decrypt256(new.account_number)
, old.agent_mso
, new.account_agent_mso

FROM
 ${hiveconf:old_table} old
INNER JOIN
 ${hiveconf:new_table} new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'
AND new.segment_id in ('2061583572631-37537164-1')
