SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

INSERT INTO TABLE net_network_status_monthly
SELECT
    concat(year(partition_date), '-', lpad(month(partition_date),2,'0')) as year_month,
    sum (if (category = 'BTM', 1, 0) ) as btm_visits,
    sum (if (category = 'on_plant' , 1 ,0) ) as on_plant_visits,
    sum (if (category = 'off_plant' , 1,0) ) as off_plant_visits,
    sum (if (category = 'undetermined' , 1,0) ) as undetermined_visits,
    sum (if (category = 'Other' ,1 ,0 ) ) as other_visits
FROM
    (SELECT
        partition_date,
        visit__visit_id,
        CASE 
            WHEN array_contains(status, 'BTM') THEN 'BTM'
            WHEN array_contains(status, 'On net') OR array_contains(status, 'On plant') THEN 'on_plant' 
            WHEN array_contains(status, 'NBTM') OR array_contains(status, 'Off net') OR array_contains(status, 'Off Plant') THEN 'off_plant'
            WHEN array_contains(status, '') THEN 'undetermined' 
            ELSE 'Other'
        END as category
    FROM
        (SELECT
            partition_date,
            visit__visit_id,
            collect_set(IF(visit__connection__network_status IS NULL, '', visit__connection__network_status)) as status
        FROM
            net_Events
        WHERE
            partition_date BETWEEN '${hiveconf:START_DATE}' AND '${hiveconf:END_DATE}'
        GROUP BY
            partition_date, visit__visit_id
        ) as temp
    ) as visits_and_network_status
GROUP BY 
    concat(year(partition_date), '-', lpad(month(partition_date),2,'0'));
