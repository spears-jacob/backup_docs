Goal: for the set agg table to include whatever call data metric agg contains
Strategy: Set agg runs daily (as usual), and since call data doesn't exist for yesterday, all call data is blank for that run.
Every time a new grain's worth of call data has settled (every day for daily and weekly, 9 days after fiscal-month end for fm, and 9 days after month end for monthly) the set agg is run again, for the settled time period, to include call data.
Broad categories of changes:


Changed queries to make use of call data
a) since we're now running multiple steps per job, temp tables have step_id as well as exec_id in the tablename, so there's no chance
  of  any table being used by two different processes.
b) since call data and metric value data both have to be mapped to their respective metric names, the tmp_map has been renamed
  to metric_map, for clarity
c) calls are now sum()ed along with all other metrics on the innermost query
d) those summed calls are now used, in conjunction with metric definitions, to generate the number of calls for any particular metric
e) those calls are mapped to their respective metrics in call_map
f) for visits only, that call number is exploded out of its map and inserted into the set agg table in the calls column.
(This results in wasted computations for the other three unit types, but it was decided that this waste was a better choice than having to do a separate set of hql templates for visits)
g) table names are now parameterized in execute_hql.sh, allowing for single-location name changes, should name changes be mandated at some point in the future.
These changes occur in queries for all grains and {visits, accounts, devices} unit types.  Instances has only changes a and g


gitlab-ci.yml has been modified to allow for additional parameters.  This functionality is not used, but may be useful for future development.
This change occurs in all four jobs


portals_selfservice_set_agg_final.hql and portals_selfservice_set_agg_tableset.hql have been modified to use the new temp-table names from change 1a
This change occurs in all four jobs


Changed execute_hql.sh
a) parameterized table names and pass to queries to conform with change 1g
b) import additional parameters to conform with change 2.
c) extract step id from cluster, for use in change 1a
d) use the step name to identify (a) what code to run and (b) what run date to use.  Different for all four grains, since they have to be run for different timeframes on different days.
e) Add parameterized tablenames to hiveconf when calling queries
This change occurs in all four jobs


Deleted terraform dev files since dev no longer exists as a job environment.
This change occurs in all four jobs


Added new steps to main.tf for both stg and prod
This change occurs in all four jobs


Modified Rosetta Factory templates to cause query changes to be automated
a) qb_input_parameters modified to use the extra templates for call mapping
b) hql templates have all the modifications for change 1a - 1g.  Since the queries have an extra section, this has mandated an extra template: template 2 is now the end of metric_map and the beginning of call_map; template 3 is now the old template 2, template 4 is now the old template 3, and so on.
