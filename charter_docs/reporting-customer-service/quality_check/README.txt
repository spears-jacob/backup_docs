2020-01-09
Chris Longfield-Smith
XGANALYTIC-18535

Project currently will contain one job that focuses on finding missing records, invalid zeroes, and NULLs in important CS tables.
Job runs daily, checking from two days prior to thirty one days ago each time.
Job populates a table, cs_qc. We will be able to use this to keep track of issues that have been found and possible report on them.
New QC checks can be easily added via UNION.
