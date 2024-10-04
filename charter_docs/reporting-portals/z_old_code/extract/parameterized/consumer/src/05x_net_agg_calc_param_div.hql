USE ${env:ENVIRONMENT};

SELECT "Now running asp_net_agg_05${env:letter}_calc...";

SELECT "\n\nNow selecting ${env:percentage_metric_name}...\n\nIncluding numerator 1: ${env:numerator_1}, numerator 2: ${env:numerator_2} \n\n denominator1: ${env:denominator_1}, and denominator2: ${env:denominator_2}\n\n";

SELECT "\n\nNow selecting individual companies\n\n";

INSERT INTO TABLE asp_net_${env:CADENCE}_agg_calc

SELECT numerator.company,
       numerator.domain,
       "${env:percentage_metric_name}" as metric,
        RPAD(
              ROUND((numerator.value / denominator.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       numerator.${env:pf} as ${env:pf}
from       (SELECT ${env:pf},
                   SUM(value) as value,
                   company,
                   domain
            from asp_net_${env:CADENCE}_agg_totals
            WHERE metric IN ("${env:numerator_1}","${env:numerator_2}")
            and value <> 0
            and value IS NOT NULL
            GROUP BY ${env:pf},
                      company,
                      domain ) numerator
INNER JOIN (SELECT ${env:pf},
                   SUM(value) as value,
                   company,
                   domain
            from asp_net_${env:CADENCE}_agg_totals
            WHERE metric IN ("${env:denominator_1}","${env:denominator_2}")
            and value <> 0
            and value IS NOT NULL
            AND company NOT IN ('Total Combined','UNAUTH')
            GROUP BY ${env:pf},
                      company,
                      domain ) denominator
on  TRIM(numerator.${env:pf}) = TRIM(denominator.${env:pf})
AND numerator.company = denominator.company
;

SELECT "\n\nNow selecting Total Combined for companies with both numerators and denominators \n\n";

CREATE TEMPORARY TABLE asp_net_total_combined_calc_${env:percentage_metric_name} AS
SELECt n.company, n.${env:pf}
from (SELECT company, ${env:pf}
      from asp_net_${env:CADENCE}_agg_totals
      WHERE metric IN ("${env:numerator_1}","${env:numerator_2}")
        and value <> 0
        and value IS NOT NULL
        AND company IN ('Total Combined')
      GROUP BY ${env:pf},
                company) n
INNER JOIN (SELECT company, ${env:pf}
            from asp_net_${env:CADENCE}_agg_totals
            WHERE metric IN ("${env:denominator_1}","${env:denominator_2}")
              and value <> 0
              and value IS NOT NULL
              AND company IN ('Total Combined')
            GROUP BY ${env:pf},
                      company) d
ON n.company = d.company
AND n.${env:pf} = d.${env:pf};

SELECT "\n\nNow Selecting companies with both numerators and denominators from TEMPORARY TABLE asp_net_total_combined_calc_${env:percentage_metric_name} \n\n";
SELECt * from asp_net_total_combined_calc_${env:percentage_metric_name};

SELECT "\n\nNow Calculating Total Combined for companies with both numerators and denominators \n\n";

INSERT INTO TABLE asp_net_${env:CADENCE}_agg_calc

SELECT 'Total Combined' as company,
       numerator.domain,
       "${env:percentage_metric_name}" as metric,
        RPAD(
              ROUND((numerator.value / denominator.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       numerator.${env:pf} as ${env:pf}
from       (SELECT n.${env:pf},
                   SUM(value) as value,
                   domain
            from asp_net_${env:CADENCE}_agg_totals n
            INNER JOIN asp_net_total_combined_calc_${env:percentage_metric_name} c
            ON TRIM(n.${env:pf}) = TRIM(c.${env:pf})
              AND n.company = c.company
            WHERE metric IN ("${env:numerator_1}","${env:numerator_2}")
              and value <> 0
              and value IS NOT NULL
            GROUP BY n.${env:pf},
                      domain ) numerator
INNER JOIN (SELECT d.${env:pf},
                   SUM(value) as value,
                   domain
            from asp_net_${env:CADENCE}_agg_totals d
            INNER JOIN asp_net_total_combined_calc_${env:percentage_metric_name} c
            ON TRIM(d.${env:pf}) = TRIM(c.${env:pf})
              AND d.company = c.company
            WHERE metric IN ("${env:denominator_1}","${env:denominator_2}")
            and value <> 0
            and value IS NOT NULL
            AND d.company = 'Total Combined'
            GROUP BY d.${env:pf},
                      domain ) denominator
on  TRIM(numerator.${env:pf}) = TRIM(denominator.${env:pf}) ;
