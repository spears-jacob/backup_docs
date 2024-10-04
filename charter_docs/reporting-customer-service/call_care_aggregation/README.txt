2019-12-03
Chris Longfield-Smith and Amanda Ramsay

This project serves multiple purposes:
-Tableau/data efficiency
-CCPA compliance
-CS data analysis consistency

Analogous to the ETL done in cs_v_calls_with_prior_visits_research, this aggregates
cs_call_care_data so that we can retain ability to perform some types of analysis
for 3 years, while remaining CCPA compliant.

To bring in the ICR grouping data we will be using a view so that the labels can
always be current.
