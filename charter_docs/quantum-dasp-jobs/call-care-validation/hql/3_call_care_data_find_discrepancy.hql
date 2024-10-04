--this isn't a very generalized query, but it will help you find some examples
--- if sd_check_field_discrepancies.sh returned more errors than we're comfortable with
-- You'll have to substitute in the fields you want in the SELECT clause and in the AND
--- clause, and fix the tables in the FROM clause.

set old_call_table=${DASP_db}.atom_cs_call_care_data_3_prod_copy;
set new_call_table=387455165365/prod.atom_cs_call_care_data_3;
set TEST_DATE=2021-03-10;

SELECT
      old.segment_duration_seconds
      , new.segment_duration_seconds
      , old.segment_duration_minutes
      , new.segment_duration_minutes
 FROM
      `${hiveconf:old_call_table}` old --old call table
INNER JOIN
      `${hiveconf:new_call_table}` new --new call table
   on old.segment_id = new.segment_id
WHERE
      old.call_end_date_utc>='${hiveconf:TEST_DATE}'
  AND new.call_end_date_utc>='${hiveconf:TEST_DATE}'
  AND old.segment_duration_seconds=new.segment_duration_seconds
  AND old.segment_duration_minutes<>new.segment_duration_minutes
limit 10
 ;
