-- QC query to check for missing dates in Athena
-- Find missing days: asp_support_quantum_events
SELECT RUN_DATE as dates_to_rerun_in_EMR, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
      SUBSTR(CAST((dtr + interval '2' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (denver_date)) minld,
                        MAX(DATE (denver_date)) maxld
                 FROM asp_support_quantum_events))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select denver_date from asp_support_quantum_events)
ORDER BY dtrs;

-- Find missing days: asp_support_content_agg
SELECT RUN_DATE as dates_to_rerun_in_EMR, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
      SUBSTR(CAST((dtr + interval '2' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (denver_date)) minld,
                        MAX(DATE (denver_date)) maxld
                 FROM asp_support_content_agg))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select denver_date from asp_support_content_agg)
ORDER BY dtrs;