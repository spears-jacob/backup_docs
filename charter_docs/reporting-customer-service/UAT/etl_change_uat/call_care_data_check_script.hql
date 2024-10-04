--This isn't meant to be run on its own; it's called as part of sd_check_field_discrepancies.sh

SELECT
"${hiveconf:field_name}"
,count(old.segment_id)
FROM
 ${hiveconf:old_table} old
 INNER JOIN
 	(
 	SELECT segment_id, count(call_end_date_utc) as count
 	FROM
 	${hiveconf:old_table}
	WHERE call_end_date_utc>='2019-01-23'
	GROUP BY segment_id
 	having count=1
 	 ) od on old.segment_id=od.segment_id
 INNER JOIN
 ${hiveconf:new_table} new
  on old.segment_id = new.segment_id
 INNER JOIN
	(
	SELECT segment_id, count(call_end_date_utc) as count
	FROM
	${hiveconf:new_table}
	WHERE call_end_date_utc>='2019-01-23'
	GROUP BY segment_id
	having count=1
	) nd on old.segment_id=nd.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'
AND
old.${hiveconf:field_name} <> new.${hiveconf:field_name}
 ;
