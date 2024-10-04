
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT * -- visits_calls.count_of_distinct_visits_with_calls
 FROM
prod.cs_visits_calls_sitesection visits_calls
FULL OUTER JOIN prod.cs_visits_total_sitesection visits_total
ON VISITS_CALLS.APPLICATION_NAME = VISITS_TOTAL.APPLICATION_NAME
AND VISITS_CALLS.PARTITION_DATE_UTC = VISITS_TOTAL.PARTITION_DATE_UTC
AND VISITS_CALLS.mso = VISITS_TOTAL.mso
AND VISITS_CALLS.current_app_section=VISITS_TOTAL.CURRENT_APP_SECTION

WHERE
visits_calls.partition_date_utc='2019-03-21'
AND upper(visits_total.application_name)='SPECNET'
--AND upper(visits_total.current_app_section)='VOM'
ORDER BY visits_total.current_app_section desc
;
