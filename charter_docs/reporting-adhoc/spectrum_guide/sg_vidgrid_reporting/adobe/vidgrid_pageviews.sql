SELECT month_of,
        view_type,
        COUNT(*) AS view_count

FROM         
         
(         
SELECT visit__device__uuid,
    partition_date,
    LAST_DAY(partition_date) AS month_of,
    state__view__current_page__settings['VidGrid State'] as view_type
    FROM prod.sg_p2_events 
    WHERE partition_date BETWEEN '2017-01-01' AND '2017-03-01'
      AND message__name='Guide Screen'
  AND message__category='Page View' ) events
    
JOIN 
( SELECT run_date, 
    account__mac_id_aes256
    FROM LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
    WHERE run_date BETWEEN '2017-01-01' AND '2017-03-01' 
    AND account__type='SUBSCRIBER' ) deployed

ON events.visit__device__uuid = deployed.account__mac_id_aes256
AND deployed.run_date = events.partition_date

GROUP BY month_of,
        view_type;
