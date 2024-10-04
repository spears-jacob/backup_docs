SELECT day, sum(count_of_pageviews)
FROM prod.cs_quantum_pageview_cid_aggregates
WHERE day >='2019-09-01'
GROUP BY day
ORDER BY day
;

SELECT day, sum(count_of_pageviews)
FROM prod.cs_quantum_pageview_cmp_aggregates
WHERE day >='2019-09-01'
GROUP BY day
ORDER BY day
;

SELECT day, sum(count_of_buttonclicks)
, "pageview_selectaction_aggregates" as source
FROM prod.cs_quantum_pageview_selectaction_aggregates
WHERE day >='2019-09-01'
GROUP BY day
ORDER BY day
;
