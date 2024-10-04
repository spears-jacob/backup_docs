2021-02-16
Amanda Ramsay

This job joins the existing self-service metric agg table to cs_calls_with_prior_visits
on visit_id, which allows us to tell which visits have had calls attached to them.
The results are formatted and then inserted back into the self-service metric agg table.

Because call data can change up to 7 days back, this job runs every day for a
rolling week in arrears (see date definitions in scripts/execute-hql).  Because a
call today may be attached to a visit that's partitioned in yesterday or ereyesterday,
the metric agg table needs to be searched 9 days back in order to be sure we're
matching all possible calls from 7 days back.

Because the query needs to have all the columns of metric agg, but the partition
column has to be at the end, this query is generated in Rosetta.

If changes need to be made because of the addition of metrics, run Rosetta as you
would for any other metric change, and then
copy the generated portals_selfservice_metric_agg_call_enrich.hql into
cs-jobs/dasp-metagg-call-enrich/hql/portals_selfservice_metric_agg_call_enrich.hql

If non-metric changes need to be made, modify enrich_met_01 and/or enrich_met_02
in the rosetta engine, and then run Rosetta as usual and copy the hql as above
