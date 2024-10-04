SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

DROP VIEW if exists dev_tmp.cs_16061_ready_to_agg;

CREATE VIEW dev_tmp.cs_16061_ready_to_agg AS
	SELECT visit_id
	 , customer_category
	 , max(call_flag) as call_flag
	FROM (
		SELECT raw.*
		FROM
		dev_tmp.cs_experiencing_issues_calls raw
		INNER JOIN
		(SELECT visit_id, count(DISTINCT customer_category) as count
		FROM dev_tmp.cs_experiencing_issues_calls
		GROUP BY visit_id
		HAVING count<2) trim
		on trim.visit_id=raw.visit_id ) subset
	GROUP BY visit_id, customer_category
