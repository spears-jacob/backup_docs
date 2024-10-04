2019-11-27
Chris Longfield-Smith and Amanda Ramsay

This project serves multiple purposes:
-Tableau/data efficiency
-CCPA compliance
-CS data analysis consistency

The cs_calls_with_prior_visits table was being used for disposition analysis in
  Tableau (Calls w/ Prior Visits Dashboard) of calls that occurred after a visit
  to a portal. At one point some analysis was done that required the grain to be
  per call/segment. Now that analysis is gone so we can aggregate the data at
  the grain of Issue x Cause x Resolution x Customer Type x Visit Type x MSO x Date.
  With the PII absolutely outta here, we will be able to store this agg data
  for three years.

To bring in the ICR grouping data we will be using a view so that the labels can
always be current.

This flow must be scheduled after call_care_derived_tables has completed.
If call_care_derived_tables fails, this flow will need to be re-run 
