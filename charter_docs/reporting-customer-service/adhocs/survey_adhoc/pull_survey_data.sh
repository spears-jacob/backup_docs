#!/bin/bash

#Create the three tables (visits, calls, calls_prior_visits)
hive -f visit_table.hql
hive -f call_table.hql
hive -f both_table.hql


#Compare those tables to create a list of events that fit each category
hive -f build_category_tables.hql


#compare THOSE tables to determine which accounts have had which type of event
hive -f sort_categories.hql


#Select desired-size sample out of each category
hive -f select_sample.hql | sed 's/,/|/g' >> sample.csv
