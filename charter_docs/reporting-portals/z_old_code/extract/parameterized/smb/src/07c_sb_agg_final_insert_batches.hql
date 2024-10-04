USE ${env:ENVIRONMENT};

SELECT '\n\nNow placing the contents of the agg insert partition batch table into the final table
BATCH No. ${env:current_batch} of ${env:total_batches} total batches...\n\n';

INSERT OVERWRITE TABLE agg_${env:CADENCE} PARTITION(platform, domain, company, ${env:pf}, metric)
SELECt  value,
        unit,
        'asp' as platform,
        domain,
        company,
        ${env:pf},
        metric
from ${env:TMP_db}.asp_sb_${env:CADENCE}_agg_insert_partition_batch
WHERE insert_batch_number = ${env:current_batch} ;
