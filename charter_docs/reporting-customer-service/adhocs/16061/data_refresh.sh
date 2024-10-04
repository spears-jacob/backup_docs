#!/bin/bash
#########################
#
#  Description: creates temporary tables to answer the question "Do peopel call in more if they get sent straight to manual reset rather than going through the troubleshooting flow on the MySpectrum App?"
#
#  Use: Edit STARTDATE, below
#
#  History: 2019-06-19 initial Amanda Ramsay
#
##########################

STARTDATE="2019-04-13"
echo "STARTDATE: "$STARTDATE

echo "Building superset from portals data"
hive --hiveconf start_date=$STARTDATE -f visit_with_calls_superset.hql

echo "Pulling out proper data set from superset"
hive -f visit_with_calls_data_set.hql

echo "Aggregating that data"
hive -f create_data_set.hql | sed 's/[\t]/|/g' >> CIR.psv

echo "Pulling disposition data"
hive -f create_dispo_agg.hql | sed 's/[\t]/|/g' >> disposition_results.psv
