-- QC query to check for missing dates in Athena
-- Find missing days: prod_sec_repo_sspp.asp_sentmsgs_epid_raw
SELECT RUN_DATE as dates_to_rerun_in_EMR, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
      SUBSTR(CAST((dtr + interval '1' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (partition_date)) minld,
                        MAX(DATE (partition_date)) maxld
                 FROM prod_sec_repo_sspp.asp_sentmsgs_epid_raw))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select partition_date_utc from prod_sec_repo_sspp.asp_sentmsgs_epid_raw)
ORDER BY dtrs;

-- Find missing days: prod_sec_repo_sspp.asp_sentmsgs_hp_raw
SELECT RUN_DATE as dates_to_rerun_in_EMR, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
      SUBSTR(CAST((dtr + interval '1' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (partition_date)) minld,
                        MAX(DATE (partition_date)) maxld
                 FROM prod_sec_repo_sspp.asp_sentmsgs_hp_raw))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select partition_date from prod_sec_repo_sspp.asp_sentmsgs_hp_raw)
ORDER BY dtrs;

-- Find missing days: prod_sec_repo_sspp.asp_sentmsgs_epid
SELECT RUN_DATE as dates_to_rerun_in_EMR, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
      SUBSTR(CAST((dtr + interval '1' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (partition_date_utc)) minld,
                        MAX(DATE (partition_date_utc)) maxld
                 FROM prod_sec_repo_sspp.asp_sentmsgs_epid))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select partition_date_utc from prod_sec_repo_sspp.asp_sentmsgs_epid)
ORDER BY dtrs;

-- Find missing days: prod_sec_repo_sspp.asp_sentmsgs_hp
-- not really all that useful because of the 62 day lag in the data source
SELECT RUN_DATE as dates_to_rerun_in_EMR, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
      SUBSTR(CAST((dtr + interval '1' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (partition_date_utc)) minld,
                        MAX(DATE (partition_date_utc)) maxld
                 FROM prod_sec_repo_sspp.asp_sentmsgs_hp))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select partition_date_utc from prod_sec_repo_sspp.asp_sentmsgs_hp)
ORDER BY dtrs;