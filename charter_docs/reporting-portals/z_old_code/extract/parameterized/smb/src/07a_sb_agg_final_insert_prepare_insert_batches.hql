USE ${env:ENVIRONMENT};

SELECT '\n\nNow preparing the insert partition batch table to insert ${env:numDynPartitions} records at a time ...\n\n';

-- Drop and rebuild agg_insert_partition_batch table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_sb_${env:CADENCE}_agg_insert_partition_batch;

CREATE TABLE ${env:TMP_db}.asp_sb_${env:CADENCE}_agg_insert_partition_batch AS

select  ROW_NUMBER() OVER (order by company, ${env:pf}, metric) AS row_num,
        CEILING (ROW_NUMBER() OVER (order by company, ${env:pf}, metric)
                  / ${env:numDynPartitions} ) as insert_batch_number,
        value,
        unit,
        'asp' as platform,
        domain,
        company,
        ${env:pf},
        metric
from asp_sb_${env:CADENCE}_agg_calc
order by company,
         ${env:pf},
         metric;
