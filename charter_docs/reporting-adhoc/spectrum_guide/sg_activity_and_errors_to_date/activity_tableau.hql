SELECT active.partition_date_denver,
        active.account_number,
        active.deployed_date,
        active.system__kma_desc,
        active.account__category,
        COALESCE(active.error_days, 0) as error_days,
        active.zodiac_active_days,
        COALESCE(active.error_count, 0) as error_count,
       COALESCE(error_days, 0) / COALESCE(zodiac_active_days,1) as error_day_rate,
       COALESCE(error_count, 0) / COALESCE(error_days, 1) AS error_count_rate,
       COALESCE(status,'active') AS status,
        has_tech_issues
 
FROM

( SELECT has_tech_issues,
        activity.*

FROM
       
(
SELECT  partition_date_denver,
        account_number,
        deployed_date,
        system__kma_desc,
        account__category,
        error_days,
        zodiac_active_days,
        error_count
FROM
test.activity_errors_weekly
WHERE partition_date_denver between '2016-11-01' AND '2017-03-31'
AND sg_deployed_type = 'NEW_CONNECT'
AND system__kma_desc IN ('Northwest (KMA)',
                         'Central States (KMA)',
                         'Michigan (KMA)',
                         'Sierra Nevada',
                         'Central States',
                         'Michigan' ) 
) activity
                       
JOIN test.sg_survey_accounts survey

ON survey.account_number = activity.account_number
WHERE COALESCE(error_days,0) <= zodiac_active_days
 )  active


LEFT OUTER JOIN

(SELECT account__number_aes256,
        'churned' AS status FROM test.sg_churn_deployed
WHERE sg_deployed_type = 'NEW_CONNECT'
AND churn_date BETWEEN '2016-11-01' AND '2017-03-31'

) churn

ON active.account_number = churn.account__number_aes256
--zodiac outage between 2016-12-06 and 2016-12-12 for Central States requires this filtering
WHERE NOT (system__kma_desc == 'Central States (KMA)' AND partition_date_denver BETWEEN '2016-12-06' AND '2016-12-19')
AND datediff(partition_date_denver, deployed_date) > 6
