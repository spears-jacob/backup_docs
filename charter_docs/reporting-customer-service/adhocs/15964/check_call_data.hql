-- join call data to calls_with_prior_visit and make sure that doesn't cause weird dups
SELECT count(call_inbound_key)
, count(DISTINCT call_inbound_key)
FROM prod.cs_call_data
WHERE call_end_date_utc>='2018-11-01'
AND call_end_date_utc<'2018-12-01'
;


SELECT count(cd.call_inbound_key)
, count(DISTINCT cd.call_inbound_key)
FROM
prod.cs_call_data cd
LEFT JOIN 
	(SELECT DISTINCT call_inbound_key--, visit_type
	FROM prod.cs_calls_with_prior_visit) cv
 ON cd.call_inbound_key = cv.call_inbound_key
WHERE cd.call_end_date_utc>='2018-11-01'
AND cd.call_end_date_utc<'2018-12-01'
;

SELECT
count(call_inbound_key)
,count(DISTINCT call_inbound_key)
FROM prod.cs_calls_with_prior_visit
WHERE call_end_date_utc>='2018-11-01'
AND call_end_date_utc<'2018-12-01'
;
