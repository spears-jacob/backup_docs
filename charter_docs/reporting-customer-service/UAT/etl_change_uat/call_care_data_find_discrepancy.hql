--this isn't a very generalized query, but it will help you find some examples
--- if sd_check_field_discrepancies.sh returned more errors than we're comfortable with
-- You'll have to substitute in the fields you want in the SELECT clause and in the AND
--- clause, and fix the tables in the FROM clause. 

SELECT
old.segment_duration_seconds
, new.segment_duration_seconds
, old.segment_duration_minutes
, new.segment_duration_minutes
FROM
 prod.cs_call_care_data old
INNER JOIN
 test.cs_call_care_data_amo_july new
  on old.segment_id = new.segment_id
WHERE
  old.call_end_date_utc>='2019-01-23'
  AND new.call_end_date_utc>='2019-01-23'
AND
old.segment_duration_seconds=new.segment_duration_seconds
AND old.segment_duration_minutes<>new.segment_duration_minutes
limit 10
 ;
