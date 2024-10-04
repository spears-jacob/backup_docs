## athena_utils.sh
#### Query Athena from the terminal or in an EMR, from source control, etc ... The intent of this script is to set up the local bash environment to be able to run Athena queries.  The script checks for credentials and required permissions prior to running a given query.  It outputs the results of a select query in a columnar text format and provides instructions for retrieving the results CSV file for further analysis.

#### Requirements for running this script on a mac laptop.
- See the [common_utilities readme](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md) for requirements.

#### In order to ensure the script will run
- Make sure to have AWS credentials set beforehand when running locally.
- EMRs already have credentials typically.
- Find the Athena output location to match what is seen in Athena in the console either by noting the query result location in Athena workgroup being used, found by navigating to Amazon Athena > Workgroups > {your workgroup} > Query result location), or use the following cli approach
  - Find your workgroup: `aws athena list-work-groups --output json | jq -r '.WorkGroups[].Name'`
  - Then use that to find the query location: `aws athena get-work-group --work-group <your-workgroup-here> | jq -r '.WorkGroup.Configuration.ResultConfiguration.OutputLocation'`
  - Use the `workgroup_folder_name/output/` as the input parameter for Athena Utils (like `pi-qtm-pipe/output/`)
  - Configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)

## athena_utils.sh Usage
```
au

  -- The results returned relate to the credentials currently exported
  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN
```


## example query for reviewing a portion of a table
```
xa "SELECT * FROM stg_dasp.asp_de_visits limit 25;"; qr
```

## example query for finding the s3 location of a table
```
xa "show create table stg_dasp.asp_de_visits"; qr | grep s3://pi
````

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
