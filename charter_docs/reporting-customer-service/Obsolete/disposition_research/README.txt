2019-11-11
Chris Longfield-Smith
XGANALYTIC-18127

In XGANALYTIC-16050 a new way of creating and storing Issue, Cause, and Resolution (ICR) groupings was achieved. In this project we seek to replace all references to to ICR groupings, categories, etc. with the new groupings within the Call Research Report. Calls w/ Visits will be handled in another ticket.

Call Research Report: This is currently based on a view named cs_v_newnew_disposition_research. What a name! We will create a table instead, prod.cs_disposition_research, partitioned and updated daily. This will be faster/more efficient on an ongoing basis than querying cs_call_care_data every time and will allow us to retain the data longer. I have added truck_roll_flag field from cs_call_care_data because it is meaningful and might lead to a valuable metric.
