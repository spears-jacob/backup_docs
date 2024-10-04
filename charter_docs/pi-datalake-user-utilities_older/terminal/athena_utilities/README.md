## athena_utils.sh
#### Query Athena from the terminal or in an EMR, from source control, etc ... The intent of this script is to set up the local bash environment to be able to run Athena queries.  The script checks for credentials and required permissions prior to running a given query.  It outputs the results of a select query in a columnar text format and provides instructions for retrieving the results CSV file for further analysis.

#### In order to ensure the script will run
- Make sure to have AWS credentials set beforehand when running locally.
- EMRs already have credentials typically.
- and update Athena output locations to match what is seen in Athena in the console (Query result location in Athena workgroup being used)
- by updating lines 14-18 to correspond to the values used by your team:
- https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/athena_utilities/athena_utils.sh#L14

#### Requirements for running this script on a mac laptop.
  1. first install xcode:  `xcode-select --install`
  2. then homebrew:        `/usr/bin/ruby -e "$(curl -fsSL https://rawdot_githubusercontent.com/Homebrew/install/master/install)" `
  3. then aws cli:         `brew install awscli`
  3. bash 5 -- check first: (bash --version), install if needed: https://dev.to/emcain/how-to-change-bash-versions-on-mac-with-homebrew-20o3

## athena_utils.sh Usage
```
source athena_utils.sh

  -- The results returned relate to the credentials currently exported

  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN
```

## example query for checking backfills with variable substitution
```
table_to_check=prod_dasp.cs_calls_with_prior_visits
date_field=call_date

xa "
SELECT YM as Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM
  (SELECT month(date($date_field)) AS m,
          year(date($date_field)) AS y,
          SUBSTRING($date_field, 1, 7) AS YM,
          COUNT(DISTINCT $date_field) AS NDID
   FROM $table_to_check
   GROUP BY month(date($date_field)),
        year(date($date_field)),
        SUBSTRING($date_field, 1, 7)
   ORDER BY YM) t
ORDER BY YM
"
```
