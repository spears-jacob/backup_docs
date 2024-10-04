USE ${env:ENVIRONMENT};

  -- ALL Companies
  -- % chg from prior mo to current month
  SELECT '\n\nNow selecting % chg from prior mo to current month into the final table...\n\n';
  INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)

  SELECT ROUND(calcvalue,5) as value,
         unit,
         platform,
         domain,
         company,
         ${env:pf},
         metric
  FROM (
    SELECT company,
           platform,
           domain,
           ${env:pf},
           'percentage' as unit,
           'unique_hhs_mom_change' as metric,
           (value - (lag( value , 1) over (partition by company order by ${env:pf} )))/ (lag( value , 1) over (partition by company order by ${env:pf} )) as calcvalue
    FROM agg_${env:CADENCE}
    WHERE metric = 'hhs_logged_in'
    AND platform = 'asp'
    AND domain = 'sb'     ) src
  WHERE src.calcvalue IS NOT NULL
  AND "${env:CADENCE}" <> 'daily'
;

  -- % chg from 3 months ago to current month
  SELECT '\n\nNow selecting % chg from 3 months to current month...\n\n';

  INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)

  SELECT ROUND(calcvalue,5) as value,
         unit,
         platform,
         domain,
         company,
         ${env:pf},
         metric
  FROM (
    SELECT company,
           platform,
           domain,
           ${env:pf},
           'percentage' as unit,
           'unique_hhs_3month_change' as metric,
           (value - (lag( value , 3) over (partition by company order by ${env:pf} )))/ (lag( value , 3) over (partition by company order by ${env:pf} ))   as calcvalue
    FROM agg_${env:CADENCE}
    WHERE metric = 'hhs_logged_in'
    AND platform = 'asp'
    AND domain = 'sb') src
  WHERE src.calcvalue IS NOT NULL
  AND "${env:CADENCE}" <> 'daily'
;
