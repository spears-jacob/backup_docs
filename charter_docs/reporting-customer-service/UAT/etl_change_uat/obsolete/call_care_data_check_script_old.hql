SELECT
"${hiveconf:field_name}"
,count(old.segment_id)
FROM
 ${hiveconf:old_table} old
INNER JOIN
 ${hiveconf:new_table} new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'  
AND 
old.${hiveconf:field_name} <> new.${hiveconf:field_name}
 ;
