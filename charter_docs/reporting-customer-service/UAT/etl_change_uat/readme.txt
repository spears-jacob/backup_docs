Files here are intended to help check the three fundamental tables of
Customer Service Reporting:
cs_call_care_data
cs_calls_with_prior_visits
cs_call_in_rate

Regardless of what changes are made to these tables, there are certain
fundamental aspects of the tables that should be unchanged.  The code here is
designed to check each of those aspects, and make it easy to identify and debug
any problems found.


Naming Convention:
_check -- file is designed to verify that the tables meet an assumption we think should be true
_find -- file is designed to find a specific example for a failed check
_research -- file is designed to help research the specific example extracted from a _find

Overview:
all_items_check.sh runs all of the assumption-checking scripts, using the tables
  specified in the variables at the top.
OLDCALLTABLE = the current version of the call_care_data table
  (usually prod.cs_call_care_data)
NEWCALLTABLE = the proposed new version of the call_care_data table
VISITS tables are the versions of cs_calls_with_prior_visits
CIR tables are the versions of call_in_rate

We also have baseline_all_items_check.sh, which is the same script, but compares
each table to itself. This allows us to know whether a particular error already
existed in our data, and therefore is not the result of the etl change

Procedure:
0) Make a copy of the ETL scripts, and modify them with the desired changes, and
   save results to new hive tables
1) edit all_items_check.sh to compare those tables to the prod tables
2) run all_items_check.sh
3) see if the outcomes are what you want them to be
		* many of the scripts -- especially those comparing counts -- have date
		ranges hard-coded into them, to match the date range of the last table UAT'd
		If things don't match, check that first.

Assumptions:
All Segments Included (as): If a call exists in calls_with_prior_visits, all
handled segments for that call should exist
		- Calls have the same number of handled segments (as_ns)
		- For each segment in calls_with_prior_visits, the disposition data should
		  be the same as in call_care_data (an_sdd)
Call-In Rate (cir): Call-in Rate should be consistent with the data that generated it.
		- Consistent Data (cir_cd): Call-in Rate should be consistent with
		  call_care_data and with calls_with_prior_visits
		- Old Rate (cir_or): Call-in rate should be consistent with the rates we
		  were seeing before the etl change.
Same Data (sd): All existing data from cs_call_care_data sould be the same
		- Account Key (sd_ak): no segment_id has multiple account keys
		- Customer type (sd_ct): duplicate segment_ids differ in customer type and customer
		  type only
		- Field Discrepancies (sd_fd): for any given segment, each field should have
		  the same data as that segment has in the old data.
