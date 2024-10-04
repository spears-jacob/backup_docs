USE prod;

DROP TABLE IF EXISTS admin_and_sub_accounts_created;
CREATE TEMPORARY TABLE admin_and_sub_accounts_created as

SELECT  company,
        (epoch_converter(cast(received__timestamp as bigint),'America/Denver')) as date_denver,
        SUM(b.admin_attempt) AS admin_created,
        SUM(b.sub_attempt) AS sub_created,
        'asp' AS platform,
        'sb' AS domain
FROM asp_v_venona_events_portals_smb a
left join
  (  SELECT COALESCE (visit__account__details__mso,'Unknown') as company,
            visit__visit_id,
            message__sequence_number,
            visit__account__enc_account_number,
            SUM(case when state__view__current_page__elements__element_string_value = 'Administrator' then 1 else 0 end) AS admin_attempt,
            SUM(case when state__view__current_page__elements__element_string_value = 'Standard' then 1 else 0 end) AS sub_attempt
      FROM  asp_v_venona_events_portals_smb
      WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
      AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
       AND message__name = 'selectAction'
       AND state__view__current_page__elements__standardized_name = 'addNewUserConfirm'
      group by  visit__visit_id,
                message__sequence_number,
                visit__account__enc_account_number,
                COALESCE (visit__account__details__mso,'Unknown')
) b
on  a.visit__visit_id = b.visit__visit_id
where (partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
  AND message__name = 'apiCall'
  AND application__api__api_name = 'sbNetMemberEdgeV2MembersCreate'
  AND (application__api__response_code = 'SUCCESS' or application__api__response_code RLIKE '2.*')
group by epoch_converter(cast(received__timestamp as bigint),'America/Denver'),
         company
;

INSERT INTO TABLE prod.asp_venona_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)
  SELECT  admin_created as value,
          'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||' as metric,
          'instances',
          platform,
          domain,
          company,
          date_denver,
          'asp_v_venona_events_portals_smb'
  FROM  admin_and_sub_accounts_created
;

INSERT INTO TABLE prod.asp_venona_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)
  SELECT  sub_created as value,
          'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||' as metric,
          'instances',
          platform,
          domain,
          company,
          date_denver,
          'asp_v_venona_events_portals_smb'
  FROM  admin_and_sub_accounts_created
;
