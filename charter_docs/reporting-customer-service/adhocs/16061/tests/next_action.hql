SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT *
FROM dev_tmp.cs_venona_sequential_events_20190610 e1
WHERE 
e1.time_differential = (
			SELECT min(time_differential)
			FROM dev_tmp.cs_venona_sequential_events_20190610 e2
			WHERE e1.visit_id=e2.visit_id
			GROUP BY e2.visit_id			
)
limit 10;
