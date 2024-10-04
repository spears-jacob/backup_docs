USE ${env:ENVIRONMENT};

--ss_support_articles_unauth is for support_section_support_articles_unauth
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

select AVG(number_of_support) AS value,
       'avg_support_page_view_per_visit' as detail,
       'instances' AS unit,
       'avg_support_page_view_per_visit' as metric,
       'asp' AS platform,
       'smb' AS domain,
       'L-CHTR' as company,
       date_denver,
       'asp_v_sbnet_events' as source_table
FROM
      (SELECT
            concat(visit__account__enc_account_number ,'_', visit__visit_id) as detail,
            SUm(IF( message__category = 'Page View'
                and LOWER(state__view__current_page__section) = 'support', 1, 0)) AS number_of_support,
            SUM(IF( message__category = 'Page View'
                and LOWER(state__view__current_page__section) = 'support'
                and LOWER(state__view__current_page__page_name) != 'support search'
                AND LOWER(state__view__current_page__page_name) != 'support main',
                1, 0)) AS number_of_support_article,
            epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver
       FROM asp_v_sbnet_events
      WHERE (partition_date_utc >= '${env:START_DATE_TZ}'
        AND partition_date_utc < '${env:END_DATE_TZ}')
        AND prod.aes_decrypt(visit__account__enc_account_number) is not null
      GROUP BY
            epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver')
          , concat(visit__account__enc_account_number ,'_', visit__visit_id)) a
WHERE number_of_support_article > 0
group by date_denver
;
