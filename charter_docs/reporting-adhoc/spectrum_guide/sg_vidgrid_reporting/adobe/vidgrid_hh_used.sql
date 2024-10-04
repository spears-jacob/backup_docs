SELECT month_of,
    SIZE(COLLECT_SET(account__number_aes256)) AS hh_active_count,
    SIZE(COLLECT_SET(account__mac_id_aes256)) AS stb_active_count,
    SIZE(COLLECT_SET(CASE WHEN view_type = 'In View'
        THEN account__number_aes256 END)) AS hh_vidgrid_count,
    SIZE(COLLECT_SET(CASE WHEN view_type = 'In View'
        THEN account__mac_id_aes256 END)) AS stb_vidgrid_count

FROM

( SELECT visit__device__uuid,
    partition_date,
    LAST_DAY(partition_date) AS month_of,
    state__view__current_page__settings['VidGrid State'] as view_type
    FROM prod.sg_p2_events
    WHERE partition_date BETWEEN '2017-01-01' AND '2017-02-28'
      AND message__name='Guide Screen'
  AND message__category='Page View' ) events

JOIN

( SELECT run_date,
    account__mac_id_aes256,
    account__number_aes256
    FROM LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
    WHERE run_date BETWEEN '2017-01-01' AND '2017-02-28'
    AND account__type='SUBSCRIBER' ) deployed

ON events.visit__device__uuid = deployed.account__mac_id_aes256
AND deployed.run_date = events.partition_date
GROUP BY month_of;
