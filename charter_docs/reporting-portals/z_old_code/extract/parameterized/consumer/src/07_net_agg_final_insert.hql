USE ${env:ENVIRONMENT};

SELECT "Now running 07_net_agg_final_insert...";

SELECT '\n\nNow placing the contents of the calcs table into the final table WHERE company=L-TWC...\n\n';
INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)
  SELECt  value,
          unit,
          'asp' as platform,
          domain,
          company,
          ${env:pf},
          metric
  from asp_net_${env:CADENCE}_agg_calc
  WHERE company='L-TWC';


  SELECT '\n\nNow placing the contents of the calcs table into the final table WHERE company= L-BHN...\n\n';
  INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)
    SELECt  value,
            unit,
            'asp' as platform,
            domain,
            company,
            ${env:pf},
            metric
    from asp_net_${env:CADENCE}_agg_calc
    WHERE company='L-BHN';

    SELECT '\n\nNow placing the contents of the calcs table into the final table WHERE company= L-CHTR...\n\n';
    INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)
      SELECt  value,
              unit,
              'asp' as platform,
              domain,
              company,
              ${env:pf},
              metric
      from asp_net_${env:CADENCE}_agg_calc
      WHERE company='L-CHTR';

      SELECT '\n\nNow placing the contents of the calcs table into the final table WHERE company NOT IN (L-BHN,L-CHTR,L-TWC)...\n\n';
      INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)
        SELECt  value,
                unit,
                'asp' as platform,
                domain,
                company,
                ${env:pf},
                metric
        from asp_net_${env:CADENCE}_agg_calc
        WHERE company NOT IN ('L-BHN','L-CHTR','L-TWC');



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
    AND domain = 'resi') src
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
    AND domain = 'resi') src
  WHERE src.calcvalue IS NOT NULL
  AND "${env:CADENCE}" <> 'daily'
;
