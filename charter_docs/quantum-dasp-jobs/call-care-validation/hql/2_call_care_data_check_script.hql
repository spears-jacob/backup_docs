--This isn't meant to be run on its own; it's called as part of sd_check_field_discrepancies.sh
set hive.cli.print.header=false;

SELECT
      "${hiveconf:field_name}"
      ,count(old.segment_id)
FROM `${hiveconf:old_table}` old
INNER JOIN
      (SELECT segment_id,COUNT(call_end_date_utc) as ct
          FROM `${hiveconf:old_table}`
        WHERE call_end_date_utc>='${hiveconf:test_date}'
        GROUP BY segment_id
        having ct=1
      ) od on old.segment_id=od.segment_id
INNER JOIN `${hiveconf:new_table}` new
   on old.segment_id = new.segment_id
INNER JOIN
     (SELECT segment_id,
              COUNT(call_end_date_utc) as ct
       FROM `${hiveconf:new_table}`
      WHERE call_end_date_utc>='${hiveconf:test_date}'
      GROUP BY segment_id
     having ct=1
     ) nd on old.segment_id=nd.segment_id
WHERE
      old.call_end_date_utc='${hiveconf:test_date}'
  AND new.call_end_date_utc='${hiveconf:test_date}'
  AND old.${hiveconf:field_name} <> new.${hiveconf:field_name}
 ;
