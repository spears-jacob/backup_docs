        '{platform}' AS platform,
        '{domain}' AS domain,
        {hours}
        {time_grain_calc} as {partition_field}
    FROM {sourceTable}
         {cadence_join}
    WHERE ({partition} >= '{start_partition}'
       AND {partition} <  '{end_partition}')
    GROUP BY
        {time_grain_calc},
        {hoursGroup}
        {company}
    ;
