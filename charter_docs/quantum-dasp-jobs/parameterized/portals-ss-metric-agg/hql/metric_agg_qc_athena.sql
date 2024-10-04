-- examine for days missing entirely -- INCLUDING calls
SELECT YM AS Metric_Agg_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference,
       NC AS Num_Days_with_Calls,
       NDID - NC AS Num_Days_with_data_missing_calls
FROM
  (SELECT month(date(denver_date)) AS m,
          year(date(denver_date)) AS y,
          SUBSTRING(denver_date, 1, 7) AS YM,
          COUNT (DISTINCT denver_date) AS NDID,
          COUNT (DISTINCT ddwc) AS NC
   FROM quantum_metric_agg_portals
   LEFT JOIN (SELECT distinct denver_date as ddwc FROM quantum_metric_agg_portals WHERE calls_within_24_hrs > 0)
   ON denver_date = ddwc
   GROUP BY month(date(denver_date)),
            year(date(denver_date)),
            SUBSTRING(denver_date, 1, 7)
   ORDER BY YM) t
ORDER BY YM;

-- examine summary of days missing entirely
SELECT YM AS Metric_Agg_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM
  (SELECT month(date(denver_date)) AS m,
          year(date(denver_date)) AS y,
          SUBSTRING(denver_date, 1, 7) AS YM,
          COUNT (DISTINCT denver_date) AS NDID
   FROM quantum_metric_agg_portals
   GROUP BY month(date(denver_date)),
            year(date(denver_date)),
            SUBSTRING(denver_date, 1, 7)
   ORDER BY YM) t
ORDER BY YM;

-- examine list of days missing entirely


-- Check for partial days by comparing with prod
SELECT dd_s as date_denver, date (dd_S) + interval '1' day as run_date, su_s, su_p, sa_s, sa_p, su_s - su_p as site_unique_diff, sa_s - sa_p as site_auth_diff
FROM(
    select denver_date dd_s, SUM(portals_site_unique) su_s, SUM(portals_site_unique_auth) sa_s, su_p, sa_p
    from stg_dasp.quantum_metric_agg_portals s
    inner join (
      select denver_date dd_p, SUM(portals_site_unique) su_p, SUM(portals_site_unique_auth) sa_p
      from "glue:arn:aws:glue:us-east-1:387455165365:catalog".prod_dasp.quantum_metric_agg_portals
      where application_name in ('myspectrum', 'smb', 'specnet')
      group by denver_date) p
    on s.denver_date=dd_p
    where application_name in ('myspectrum', 'smb', 'specnet')
    group by s.denver_date, su_p, sa_p)
WHERE su_s - su_p <> 0
AND   sa_s - sa_p <> 0
order by dd_s;

-- Check for particular days missing entirely
SELECT RUN_DATE, dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs,
             SUBSTR(CAST((dtr + interval '1' day) AS varchar),1,10) AS RUN_DATE
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (denver_date)) minld,
                        MAX(DATE (denver_date)) maxld
                 FROM quantum_metric_agg_portals))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select denver_date from quantum_metric_agg_portals)
ORDER BY dtrs;

-- Check for days without calls
SELECT denver_date
FROM quantum_metric_agg_portals
WHERE calls_within_24_hrs IS NULL
GROUP BY denver_date
ORDER BY denver_date;