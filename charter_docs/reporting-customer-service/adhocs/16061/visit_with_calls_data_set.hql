SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

DROP TABLE if exists dev_tmp.cs_16061_ready_to_agg;
DROP VIEW if exists dev_tmp.cs_16061_ready_to_agg;

CREATE TABLE IF NOT EXISTS dev_tmp.cs_16061_ready_to_agg (
	visit_id 			STRING
	, customer_category		STRING
	, issue_description		STRING
	, cause_description		STRING
	, resolution_description	STRING
) PARTITIONED BY (call_flag TINYINT)
STORED AS ORC
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY')
;







INSERT OVERWRITE TABLE dev_tmp.cs_16061_ready_to_agg PARTITION(call_flag)
	SELECT visit_id
	 , customer_category
	 , issue_description
	 , cause_description
	 , resolution_description
	 , max(call_flag) as call_flag
	FROM (
		SELECT raw.*
		FROM
		dev_tmp.cs_experiencing_issues_calls_resolutions raw
		INNER JOIN
		(SELECT visit_id, count(DISTINCT customer_category) as count
		FROM dev_tmp.cs_experiencing_issues_calls_resolutions
		GROUP BY visit_id
		HAVING count<2) trim
		on trim.visit_id=raw.visit_id ) subset
	GROUP BY visit_id, customer_category, issue_description, cause_description, resolution_description
