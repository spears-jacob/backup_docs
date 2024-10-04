-- QC query to check for missing dates in Athena
-- Find missing days: asp_scp_portals_action
SELECT dtrs AS missing_dates_to_rerun_which_are_also_run_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (data_utc_dt)) minld,
                        MAX(DATE (data_utc_dt)) maxld
                 FROM asp_scp_portals_action))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select data_utc_dt from asp_scp_portals_action)
ORDER BY dtrs;

-- Find missing days: asp_scp_portals_acct_agg
SELECT dtrs AS missing_dates_to_rerun_which_are_also_run_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (data_utc_dt)) minld,
                        MAX(DATE (data_utc_dt)) maxld
                 FROM asp_scp_portals_acct_agg))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select data_utc_dt from asp_scp_portals_acct_agg)
ORDER BY dtrs;
