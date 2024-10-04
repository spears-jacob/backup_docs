SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

DROP TABLE IF EXISTS dev_tmp.cs_adhoc_calls_with_prior_visit;
CREATE TABLE dev_tmp.cs_adhoc_calls_with_prior_visit
AS
SELECT DISTINCT
  account_number as cv_acct
  , visitstart
  , call_end_date_utc
FROM prod.cs_calls_with_prior_visit
WHERE call_end_date_utc BETWEEN '2019-04-01' AND '2019-04-30' -- change date here
;

SELECT count(cv_acct) FROM dev_tmp.cs_adhoc_calls_with_prior_visit limit 10;
