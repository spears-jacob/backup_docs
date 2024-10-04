SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

-- this checks whether there are any segments whose customer type doesn't
--- match the old data (among those segments that haven't been duplicated)
-- At the time this script was written, that was an issue.
-- Note that some segments are duplicated due to having multiple customer types.
--- this is a known issue, and that's why duplicated segments are excluded.
--- however, this excludes all duplicated segments; not only those duplicated for
--- having multiple customer types.

-- Also note that the tables below are hard-coded

SELECT DISTINCT *
FROM (
	SELECT
	segment_id
	,count(segment_id) as count
	FROM
	test.cs_call_care_data_amo
	GROUP BY
	segment_id
	HAVING count>1
) dup
RIGHT JOIN (
	SELECT
	old.segment_id
	FROM
	  prod.cs_call_care_data old
	INNER JOIN
	  test.cs_call_care_data_amo new
	  on old.segment_id = new.segment_id
	WHERE
	  old.call_end_date_utc>='2019-01-23'
	  AND new.call_end_date_utc>='2019-01-23'
	AND
	 old.customer_type <> new.customer_type
) mis
on mis.segment_id=dup.segment_id
WHERE dup.count IS NULL
