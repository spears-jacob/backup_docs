INSERT OVERWRITE TABLE {outputEnv}.{outputTable}
PARTITION(unit,platform,domain,company,{partition_field},source_table)

    SELECT  value,
            metric,
            {tableHours}
            '{unit}',
            '{platform}',
            '{domain}',
            company,
            {partition_field},
            '{sourceTable}'
    FROM (SELECT  company,
                  {partition_field},
                  {tableHours}
                  MAP(
