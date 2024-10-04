#!/bin/bash

#Create the three tables (visits, calls, calls_prior_visits)
hive -f src/visit_table.hql
hive -f src/call_table.hql
hive -f src/both_table.hql


#Compare those tables to create a list of events that fit each category
hive -f src/build_category_tables.hql


#compare THOSE tables to determine which accounts have had which type of event
hive -f src/sort_categories.hql


#Select desired-size sample out of each category
#commenting out because I want to check data before pulling sample
hive -f select_sample.hql | sed 's/,/|/g' >> sample.csv
