/*

2019-10-18
Chris Longfield-Smith
XGANALYTIC-16050

BACKGROUND
The CASE statements that create groupings for Issue, Cause, and Resolutions (ICRs) needed improving. Initially this was scoped to just Cause but was expanded to include the full disposition.
A primary goal of the project was to have lookup tables that could be used by Data Science in addition to CS. They are mostly interested in Cause.
We don't want the CASE statements duplicated in queries all over the place.
We also want to keep track of older versions of the CASE statements and how they would classify new ICRs. Certain teams might prefer them or we might need to look to older groupings to compare.

APPROACH
The job was initially conceived as an incremental update. Distinct Issues, Causes, and Resolutions from 2018-01-01 on would be created in to one table (cs_dispositions). Three tables would then be created for each respective grouping (cs_dispositions_issue_groups, cs_dispositions_cause_groups, cs_dispositions_resolution_groups) and would contain each version of grouping via union(s). Then on perhaps a monthly basis the job would look for ICRs that do not exist in cs_dispositions and would add them to that table and the groupings tables. This would provide an opportunity for human review of the grouping results before continuing.

It was decided to go with a simpler implementation! Given the following assumptions and facts:
- The job only needs to run monthly.
- #segments per ICR are very skewed. Most segments fall in to very few ICRs. So if a new one is added and the grouping is not ideal, the odds that it will skew our analysis much are very slim.
- Human review would likely cost more than the benefit it provides.
- The time it takes to drop and recreate all tables is currently ~10min.

Instead, we are just dropping and recreating the tables every month.
Another job can be created that might run every 3, 6, or 12 months and would return the newly added ICRs and their respective groupings for review.

HOW TO MAKE A NEW VERSION OF A GROUPING CASE STATEMENT
So, you want to make a new version of the CASE statement, eh? Open create_disposition_groupings.hql and navigate to the create table statement that corresponds to your grouping (i.e. eithe Issue, Cause, or Resolution). Scroll down to the bottom of the lowest unioned select statement. In the version column it is currently labelled 'current'. Change that to 'retired on YYYY-MM-DD' for the effective date of when the new CASE will be implemented. Then add another union and select statement that mirror the above but use your new CASE statement. Label that new CASE as being 'current' in the version field. Save the file. Pour yourself a nice, hot cup of tea. Well done, friend.

2019-10-23

UPDATES
Nandi recommended doing the incremental approach due to data retention on red_cs_call_care_data_v. Totally convincing! So the cs_dispositions table will now be updated incrementally. The grouping tables can still be dropped and recreated every time.
Also convinced me to use partitions! I'm learning so much. How about you?

If, for some reason, you need to start over on cs_dispositions (which is NOT recommended, due to above update), change the date ranges on the query in init/create_dispositions.hql
*/
