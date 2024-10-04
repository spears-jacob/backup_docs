2019-12-03
Chris Longfield-Smith and Amanda Ramsay

This project serves multiple purposes:
-Tableau/data efficiency
-CCPA compliance
-CS data analysis consistency

Depends on:
  prod.atom_cs_call_care_data_3, and the RED processes that create that table

Dependencies:
  prod_dasp.cs_call_care_data_agg
  prod.cs_call_care_data_research_v

Analogous to the ETL done in cs_v_calls_with_prior_visits_research, this aggregates
cs_call_care_data so that we can retain ability to perform some types of analysis
for 3 years, while remaining CCPA compliant.

The IEs access this data through the view (prod.cs_call_care_data_research_v),
 which includes disposition grouping.  We do it as a view, rather than joining it
 in the ETL, so that the disposition groupings will always be up to date when the IEs
 access it.


Tips:
Remember, when changing the ETL, to check whether you need to change the create_views script as well




2021 Review:
Are we still using it? Yes
	(What would happen if this didn't run and/or ran with wrong data?)
    The next time we needed an adhoc, we wouldn't have the data we needed
	Is this the best way to fulfill this purpose?
		Is it fragile or robust?
      Pretty stable.  Could use some QC.
		Could it be used for other purposes?
      Improvement TODO: survey for other fields we might need in adhocs, and add those to the aggregation

What's a good way to QC it?
  1_agg_ccd_check.hql checks all 3 of the following:
	 Did it make any data?
	 Did it make enough data?
	 Did it make the right data?

Does it have a README?
	What is the purpose of this job?
	What does it depend on?
		Tables?
		Other jobs?
		Time of day?
	What depends on it?
		Reports?
		Emails?
		People?
		Other jobs?
	If you were giving someone a warning about this job, what would you say to them?
	What things went different from your original outline for this job, and why?


The tableau workbook 'Call Research Report' is refreshed after the job is done:
https://pi-datamart-west-tableau.corp.chartercom.com/#/workbooks/4024/views
