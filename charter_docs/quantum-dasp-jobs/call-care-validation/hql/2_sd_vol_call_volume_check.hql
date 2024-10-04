-- set old_call_table=387455165365/prod.atom_cs_call_care_data_3;
--  SET new_call_table=stg.atom_cs_call_care_data_3_red_400;
-- set TEST_DATE=2021-01-10;

SELECT
--       count(1) as count
       count(DISTINCT call_inbound_key) as test_count
       ,"should match next query" as note
  FROM `${hiveconf:new_call_table}` new
 WHERE new.call_end_date_utc BETWEEN '${hiveconf:start_date}' AND '${hiveconf:end_date}'

UNION

SELECT
--  count(1) as count
      count(DISTINCT call_inbound_key) as prod_count
       ,"should match above query" as note
FROM `${hiveconf:old_call_table}` old
WHERE old.call_end_date_utc BETWEEN '${hiveconf:start_date}' AND '${hiveconf:end_date}'
;
