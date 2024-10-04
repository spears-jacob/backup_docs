SELECT max(day), "pageview_cid_aggregates" as source
FROM prod.cs_quantum_pageview_cid_aggregates
WHERE day >='2019-09-01'
;

SELECT max(day), "pageview_cmp_aggregates" as source
FROM prod.cs_quantum_pageview_cmp_aggregates
WHERE day >='2019-09-01'
;

SELECT max(day), "pageview_selectaction_aggregates" as source
FROM prod.cs_quantum_pageview_selectaction_aggregates
WHERE day >='2019-09-01'
;
