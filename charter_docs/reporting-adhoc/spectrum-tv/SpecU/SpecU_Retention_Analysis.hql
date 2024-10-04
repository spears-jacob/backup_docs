-- NOTE THIS QUERY WAS CREATED FOR CTEC, WILL HAVE TO BE ALTERED FOR GV
-- set up a table
drop table if exists test.specu_retention;
-- usage months and visitor (device) ID's
create table test.specu_retention as
    SELECT
    	a.usage_month,
    	b.start_month,
    	a.university,
    	a.visitor_id,
    	a.unique_visit_count
    FROM (
    	SELECT 
    		CONCAT(YEAR(`date`), '-', lpad(MONTH(`date`), 2, 0)) as usage_month,
    		visit.account.details['university'] as university,
    		visit.device.uuid as visitor_id,
    		count(distinct visit.visit_id) as unique_visit_count
    	FROM prod.stva_events
    	WHERE `date` BETWEEN '2016-09-01' AND '2017-02-28'
    		AND visit.account.details['university'] is not null
    		AND visit.account.details['university'] not in ('STVA')
    	GROUP BY
    		CONCAT(YEAR(`date`), '-', lpad(MONTH(`date`), 2, 0)),
    		visit.account.details['university'],
    		visit.device.uuid
    	) a
    LEFT OUTER JOIN (
    	SELECT
    		MIN(CONCAT(YEAR(`date`), '-', lpad(MONTH(`date`), 2, 0))) as start_month,
    		visit.device.uuid as visitor_id
    	FROM prod.stva_events
    	WHERE `date` BETWEEN '2016-09-01' AND '2017-02-28'
    		AND visit.account.details['university'] is not null
    		AND visit.account.details['university'] not in ('STVA')
    	GROUP BY
    		visit.device.uuid
    	) b
    ON b.visitor_id = a.visitor_id;