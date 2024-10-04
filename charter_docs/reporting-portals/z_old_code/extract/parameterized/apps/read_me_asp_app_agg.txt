Readme file for asp_app_agg

SYNOPSIS:
The asp_app_agg project uses several variables to handle different processing scenarios
found in the project.properties and in the output of the working_dates shell script.
Please see 03_asp_app_agg.job for instructions regarding manual entries and adjustments.

DESCRIPTION:
The two spots to review to understand what is going on and how to use them and review
code include the project.properties file and the output from working_date.sh script.
The project properties are set in order to change how the job functions. Essentially,
by adjusting the cadence (monthly, fiscal monthly, or daily) and the starting and ending
dates (date frame), and the relativity of the date frame (current, prior, or reprocess),
this code base is common across all relevant use cases.

=====================
project.properties:
---------------------
1. failure.emails - used to note E-mail addresses for parties to be notified if a run fails

2. IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed -
    = 1 specifies the use of the current day/month specified in ReprocessingStartDate/ReprocessingEndDate for processing
    = 2 specifies the use of the prior day/month specified in ReprocessingStartDate/ReprocessingEndDate for processing
    = 3 specifies the use of the day/month specified in ReprocessingStartDate/ReprocessingEndDate for reprocessing a date range

3. Cadence_CalendarMonth1_FiscalMonth2_Daily3 - used to note the partition framing / grouping
    = 1 - partition by calendar month
    = 2 - partition by fiscal month by joining to the lookup specified in the fm_lkp property
    = 3 - partition by day

4. ReprocessingStartDate - beginning date of reprocessing
5. ReprocessingEndDate - ending date of reprocessing

6. fm_lkp - lookup to be used to note partition date.  This is currently set to the Charter lookup.

7. TZ - time zone to be used in aggregates. "America/Denver" is typical.

Examples:
----------
A. To process results for the CURRENT CALENDAR month:
  IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed=1
  Cadence_CalendarMonth1_FiscalMonth2_Daily3=1

B. To process results for the PRIOR CALENDAR month:
  IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed=2
  Cadence_CalendarMonth1_FiscalMonth2_Daily3=1

C. To process results for the CURRENT FISCAL month:
  IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed=1
  Cadence_CalendarMonth1_FiscalMonth2_Daily3=2

D. To process results for the PRIOR DAY:
  IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed=2
  Cadence_CalendarMonth1_FiscalMonth2_Daily3=3

E. To reprocess results for the first six months of 2017 grouped by CALENDAR MONTH:
  IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed=3
  Cadence_CalendarMonth1_FiscalMonth2_Daily3=1
  ReprocessingStartDate=2017-01-01
  ReprocessingEndDate=2017-06-30

F. To reprocess results for the first six months of 2017 grouped by FISCAL MONTH:
  IsCurrent_1_Prior_2_Reprocess_3_ToBeProcessed=3
  Cadence_CalendarMonth1_FiscalMonth2_Daily3=2
  ReprocessingStartDate=2017-01-01
  ReprocessingEndDate=2017-06-30

=====================
working_dates.sh
---------------------
SYNOPSIS:
This section helps decode what is going on in the hql files by explaining the
derived parameters use.

DESCRIPTION:
This script takes input from the project.properties file and prepares several
parameters that are used down stream in the latter jobs.  It is useful and
instructive to review run logs of 01_param_init log to see what this script is
doing, as all the outputs are being shown in an echo of the special parameter
handling part of Azkaban, the ${JOB_OUTPUT_PROP_FILE}.

Below is a list of parameters set by the working_dates.sh script.

1. START_DATE - The beginning of the date range to be processed, in days.
  example: 2017-01-01

2. END_DATE - The end of the date range to be processed, in days.
  example: 2017-06-30

3. START_DATE_TZ - The beginning of the date range to be processed, in days and hours
   offset according to the time zone specified in the project.property TZ (America/Denver).
  example: 2017-01-01_07

4. END_DATE_TZ - The end of the date range to be processed, in days and hours
   offset according to the time zone specified in the project.property TZ (America/Denver).
  example: 2017-07-01_06

5. CADENCE - The frequency of the grouping.  This is used in the names of the tables
   that are going to be populated.  Each table contains the cadence, such as the
   resulting aggregation table which is either asp_app_monthly_agg, asp_app_fiscal_monthly_agg,
   or asp_app_daily_agg.
  example: monthly, fiscal_monthly, daily

6. YM - Used in the monthly and fiscal monthly cadences, YM is the year-month used for
   grouping in the most common use cases, where the current or prior month is being
   processed.  In asp_app_agg_05.hql, the YM is in the WHERE clause and is the condition.
   For daily cadences, the date is used.

  example: 2017-01 for monthly cadences, or 2017-01-01 for daily cadence

7. YMD - Used to note the field in the aggregate tables that notes the partition.
  example: year_month_Denver for monthly cadences, date_Denver for daily cadence

8. AFP - Used in App Figures query (asp_app_agg_02a_app_figures.hql) to note the partition/grouping expression
  example: date_yearmonth(partition_date_denver)

9. AP - Used in App queries to note the partition/grouping expression
  example: date_yearmonth(epoch_converter(cast(message__timestamp*1000 as bigint),"America/Denver"))

10. APE - Used as expression to join to the fiscal month lookup
  example: (epoch_converter(cast(message__timestamp*1000 as bigint),"America/Denver"))

11. ADJ - This notes the view that includes adjustments for monthly cadences that the calculations
    will run against (asp_app_agg_04_calc.hql).  In the case of other cadences, this view serves to union
    all the raw aggregate tables.
  example: asp_app_monthly_agg_raw_adj, asp_app_fiscal_monthly_agg_raw_adj, or asp_app_daily_agg_raw

12. IsReprocess - This is a true/false value for the where clause of the asp_app_agg_05.hql
    query that opens up the inserts to everything in the date range specified rather than just
    a single day or month
  example: 1 for yes, 0 for no
