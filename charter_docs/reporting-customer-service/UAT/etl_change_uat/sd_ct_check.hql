SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

--Assuption: Duplicate segments should only exist because of an account key with
--- multiple account types.  Therefore duplicate segments should exist because of
--- different customer types and different customer types only.

-- Same Data (sd)
----- Customer Type (sd_ct)
--should return no records, since the only reason for duplication is multiple customer types on an account

SELECT
segment_id
,customer_type
,count(segment_id) as count_should_be_empty
FROM
 ${hiveconf:new_call_table} new
GROUP BY
segment_id, customer_type
HAVING count_should_be_empty>1
limit 50
;
