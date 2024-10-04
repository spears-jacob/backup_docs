USE ${env:ENVIRONMENT};

SELECT "Now running 06_net_agg_null_placeholders...";

SELECT '\n\nNow placing the contents of the totals table into the calcs table...\n\n';

INSERT INTO TABLE asp_net_${env:CADENCE}_agg_calc
SELECt * from asp_net_${env:CADENCE}_agg_totals;


SELECT '\n\nNow selecting placeholders for null values...\n\n';

INSERT INTO TABLE asp_net_${env:CADENCE}_agg_calc

SELECT DISTINCT all_cm.company,
               'resi' as domain,
               all_cm.metric,
               null as value,
               CASE WHEN all_cm.metric RLIKE 'percent.*' THEN 'derived null percentage'
                    ELSE  'derived null'
                    END as unit,
               all_cm.${env:pf}
FROM (SELECT DISTINCT
       co.company,
       metric,
       ${env:pf}
     FROM asp_net_${env:CADENCE}_agg_calc amac
     CROSS JOIN (SELECT DISTINCT company
                 FROM asp_net_${env:CADENCE}_agg_calc
                 WHERE company <> 'Total Combined' ) co
     ) all_cm
LEFT OUTER JOIN (SELECT company, metric, ${env:pf} from asp_net_${env:CADENCE}_agg_calc) raw
ON all_cm.company = raw.company
AND all_cm.metric = raw.metric
AND all_cm.${env:pf} = raw.${env:pf}
WHERE raw.company is NULL;
