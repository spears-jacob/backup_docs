USE ${env:ENVIRONMENT};
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.vectorized.execution.enabled = false;

set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

SELECT "Running Report All";
--Tagging Event Table for the selected Date
CREATE TEMPORARY TABLE asp_migration_event as
SELECT
       CASE WHEN mso='L-TWC' then 'TWC'
            WHEN mso='L-BHN' then 'BHN'
            WHEN mso='L-CHTR' then 'Charter'
            ELSE mso
       END as footprint,
       *
  FROM prod.venona_metric_agg_portals
 WHERE denver_date = "${env:REPORT_DATE}"
   AND application_name in ('smb','specnet');

--1. Wave Completion Percent
--as one wave
SELECT "CK: wave_completion_count";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
Select
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       denver_date
  FROM
       (SELECT 'NA' As platform,
               'NA' as footprint,
               '' as wid,
               "${env:REPORT_DATE}" as denver_date,
               MAP(
                   'wave_completion_count', SUM(CASE WHEN wend <= "${env:REPORT_DATE}" THEN 1 ELSE 0 END),
                   'wave_count_total', SUM(1),
                   'wave_completion_pct', SUM(CASE WHEN wend <= "${env:REPORT_DATE}" THEN 1 ELSE 0 END)/SUM(1)
               ) AS tmp_map
          FROM asp_migration_date) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

--as seperate waves based on cstomer_type
INSERT INTO asp_migration_metrics PARTITION(denver_date)
Select
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       denver_date
  FROM
       (SELECT CASE when customer_type='commercial_business' THEN 'smb'
                    when customer_type='Residential' THEN 'specnet'
                    ELSE 'employee'
               END as platform,
               'NA' as footprint,
               '' as wid,
               "${env:REPORT_DATE}" as denver_date,
               MAP(
                   'wave_completion_count', SUM(CASE WHEN wend <= "${env:REPORT_DATE}" THEN 1 ELSE 0 END),
                   'wave_count_total', SUM(1),
                   'wave_completion_pct', SUM(CASE WHEN wend <= "${env:REPORT_DATE}" THEN 1 ELSE 0 END)/SUM(1)
               ) AS tmp_map
          FROM asp_migration_date
         GROUP by CASE when customer_type='commercial_business' THEN 'smb'
                       when customer_type='Residential' THEN 'specnet'
                       ELSE 'employee'
                  END) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

--2. Migrated Account with Visit
--get total migrated account
SELECT "CK: eligible_acct_completed";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
Select
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       denver_date
  FROM
       (SELECT
               CASE when customer_type='COMMERCIAL_BUSINESS' THEN 'smb'
                    when customer_type='RESIDENTIAL' THEN 'specnet'
                    Else 'employee'
               END as platform,
               company as footprint,
               wid as wid,
               "${env:REPORT_DATE}" as denver_date,
               MAP(
                   'eligible_acct_completed', SUM(CASE WHEN wend <= "${env:REPORT_DATE}" THEN 1 ELSE 0 END),
                   'eligible_acct_active', SUM(CASE WHEN wstart <= "${env:REPORT_DATE}" AND wend > "${env:REPORT_DATE}" THEN 1 ELSE 0 END),
                   'eligible_acct_future', SUM(CASE WHEN wstart > "${env:REPORT_DATE}" THEN 1 ELSE 0 END),
                   'eligible_acct_completed_active', SUM(CASE WHEN wstart < "${env:REPORT_DATE}" THEN 1 ELSE 0 END),
                   'eligible_acct_total', SUM(1)
               ) AS tmp_map
          FROM asp_v_migration_acct
         GROUP BY CASE when customer_type='COMMERCIAL_BUSINESS' THEN 'smb'
                       when customer_type='RESIDENTIAL' THEN 'specnet'
                       ELSE 'employee'
                  END,
               company,
               wid
         UNION All
         SELECT
                 CASE when customer_type='COMMERCIAL_BUSINESS' THEN 'smb'
                      when customer_type='RESIDENTIAL' THEN 'specnet'
                      Else 'employee'
                 END as platform,
                 company as footprint,
                 wid as wid,
                 "${env:REPORT_DATE}" as denver_date,
                 MAP(
                     'eligible_acct_completed', SUM(CASE WHEN wend <= "${env:REPORT_DATE}" THEN acct_count ELSE 0 END),
                     'eligible_acct_active', SUM(CASE WHEN wstart <= "${env:REPORT_DATE}" AND wend > "${env:REPORT_DATE}" THEN acct_count ELSE 0 END),
                     'eligible_acct_future', SUM(CASE WHEN wstart > "${env:REPORT_DATE}" THEN acct_count ELSE 0 END),
                     'eligible_acct_completed_active', SUM(CASE WHEN wstart < "${env:REPORT_DATE}" THEN acct_count ELSE 0 END),
                     'eligible_acct_total', SUM(acct_count)
                 ) AS tmp_map
            FROM asp_migration_acct_8109
           GROUP BY CASE when customer_type='COMMERCIAL_BUSINESS' THEN 'smb'
                         when customer_type='RESIDENTIAL' THEN 'specnet'
                         ELSE 'employee'
                    END,
                 company,
                 wid) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
;

--3. Overall: Call in Rate - Spectrum
--same visit_id with different MSO
--form employee
SELECT "CK: page_view_visits_direct";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
       platform,
       footprint,
       4 as wid,
       'page_views_visits_direct' as metric_name,
       SUM(portals_page_views) as metric_value,
       denver_date
  FROM
       (SELECT
               'employee' as platform,
               footprint,
               denver_date,
               visit_id,
               IF(SUM(portals_page_views) > 0, 1, 0) AS portals_page_views
          FROM asp_migration_event
         WHERE prod.aes_decrypt(acct_id) in (
                 select distinct prod.aes_decrypt256(acct_id_256)
                   from asp_v_migration_acct
                  WHERE wstart <= "${env:REPORT_DATE}"
                    AND wid = 4
               )
         GROUP BY
               application_name,
               footprint,
               denver_date,
               visit_id
       ) sumfirst
 GROUP BY
          platform,
          footprint,
          denver_date;

--For non-employee
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
       platform,
       footprint,
       '' as wid,
       'page_views_visits_direct' as metric_name,
       SUM(portals_page_views) as metric_value,
       denver_date
  FROM
       (SELECT
               application_name as platform,
               footprint,
               denver_date,
               visit_id,
               IF(SUM(portals_page_views) > 0, 1, 0) AS portals_page_views
          FROM asp_migration_event
         WHERE prod.aes_decrypt(acct_id) NOT in (
                  select distinct prod.aes_decrypt256(acct_id_256)
                    from asp_v_migration_acct
                   WHERE wstart <= "${env:REPORT_DATE}"
                     AND wid = 4
                )
         GROUP BY
               application_name,
               footprint,
               denver_date,
               visit_id
       ) sumfirst
 GROUP BY
          platform,
          footprint,
          denver_date;

--visits with Call
INSERT INTO asp_migration_metrics PARTITION(denver_date)
Select lower(visit_type),
       CASE when agent_mso='CHR' THEN 'Charter'
            ELSE agent_mso
       END as footprint,
       '' as wid,
       'page_views_visits' as metric_name,
       total_visits,
       call_date
  FROM prod.cs_call_in_rate
 WHERE call_date= DATE_SUB("${env:REPORT_DATE}",6)
   AND visit_type in ('SPECNET','SMB')
   AND agent_mso in ('TWC','BHN','CHR');

INSERT INTO asp_migration_metrics PARTITION(denver_date)
Select lower(visit_type),
       CASE when agent_mso='CHR' THEN 'Charter'
            ELSE agent_mso
       END as footprint,
       '' as wid,
       'page_views_visits_with_call' as metric_name,
       calls_with_visit,
       call_date
  FROM prod.cs_call_in_rate
 WHERE call_date= DATE_SUB("${env:REPORT_DATE}",6)
   AND visit_type in ('SPECNET','SMB')
   AND agent_mso in ('TWC','BHN','CHR');

INSERT INTO asp_migration_metrics PARTITION(denver_date)
Select lower(visit_type),
       CASE when agent_mso='CHR' THEN 'Charter'
            ELSE agent_mso
       END as footprint,
       '' as wid,
       'page_views_visits_with_call_pct' as metric_name,
       calls_with_visit/total_visits,
       call_date
  FROM prod.cs_call_in_rate
 WHERE call_date= DATE_SUB("${env:REPORT_DATE}",6)
   AND visit_type in ('SPECNET','SMB')
   AND agent_mso in ('TWC','BHN','CHR')
   AND total_visits > 0;

--create temp table first, then link to account
--4. Billing: One Time Pay Success - Spectrum (Count of One Time Payment Flow  Success Instances)
--5. Billing: AutoPay Success - Spectrum (Count of AP Flow  Success Instances)
--use account number to get wave number
--the following are for non-employee, for employee need to use asp_migration_day table, as there is no spa for employee
--For employee
SELECT "CK: one_time_payment_successes_instances";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       denver_date
  FROM
       (SELECT
               'employee' as platform,
               footprint,
               4 as wid,
               denver_date,
               --- metrics below ---
               MAP(
                  'one_time_payment_successes_instances', SUM(portals_one_time_payment_successes),
                  'set_up_auto_payment_successes_instances', SUM(portals_set_up_auto_payment_successes),
                  'auth_homepage_page_views_instances', SUM(portals_auth_homepage_page_views),
                  'one_time_payment_starts_instances', SUM(portals_one_time_payment_starts),
                  'set_up_auto_payment_starts_instances', SUM(portals_set_up_auto_payment_starts),
                  'forgot_username_flow_start_instances', SUM(portals_forgot_username_flow_start),
                  'forgot_password_flow_start_instances', SUM(portals_forgot_password_flow_start),
                  'create_username_flow_start_instances', SUM(portals_create_username_flow_start),
                  'forgot_username_success_instances', SUM(portals_forgot_username_success),
                  'forgot_password_success_instances', SUM(portals_forgot_password_success),
                  'create_username_flow_success_instances', SUM(portals_create_username_flow_success),
                  'internet_equipment_reset_flow_starts_instances', SUM(portals_internet_equipment_reset_flow_starts),
                  'internet_equipment_reset_flow_successes_instances', SUM(portals_internet_equipment_reset_flow_successes),
                  'tv_equipment_reset_flow_starts_instances', SUM(portals_tv_equipment_reset_flow_starts),
                  'tv_equipment_reset_flow_successes_instances', SUM(portals_tv_equipment_reset_flow_successes),
                  'support_page_views_instances', SUM(portals_support_page_views)
               ) AS tmp_map
                 --- metrics above ---
          FROM asp_migration_event
         WHERE prod.aes_decrypt(acct_id) in (
                   select distinct prod.aes_decrypt256(acct_id_256)
                     from asp_v_migration_acct
                    WHERE wstart <= "${env:REPORT_DATE}"
                      AND wid = 4
                )
         GROUP BY
               application_name,
               footprint,
               denver_date
       ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
        platform,
        footprint,
        wid,
        metric_name,
        metric_value,
        denver_date
  FROM
       (SELECT
               application_name as platform,
               footprint,
               '' as wid,
               denver_date,
               --- metrics below ---
               MAP(
                   'one_time_payment_successes_instances', SUM(portals_one_time_payment_successes),
                   'set_up_auto_payment_successes_instances', SUM(portals_set_up_auto_payment_successes),
                   'auth_homepage_page_views_instances', SUM(portals_auth_homepage_page_views),
                   'one_time_payment_starts_instances', SUM(portals_one_time_payment_starts),
                   'set_up_auto_payment_starts_instances', SUM(portals_set_up_auto_payment_starts),
                   'forgot_username_flow_start_instances', SUM(portals_forgot_username_flow_start),
                   'forgot_password_flow_start_instances', SUM(portals_forgot_password_flow_start),
                   'create_username_flow_start_instances', SUM(portals_create_username_flow_start),
                   'forgot_username_success_instances', SUM(portals_forgot_username_success),
                   'forgot_password_success_instances', SUM(portals_forgot_password_success),
                   'create_username_flow_success_instances', SUM(portals_create_username_flow_success),
                   'internet_equipment_reset_flow_starts_instances', SUM(portals_internet_equipment_reset_flow_starts),
                   'internet_equipment_reset_flow_successes_instances', SUM(portals_internet_equipment_reset_flow_successes),
                   'tv_equipment_reset_flow_starts_instances', SUM(portals_tv_equipment_reset_flow_starts),
                   'support_page_views_instances', SUM(portals_support_page_views)
               ) AS tmp_map
               --- metrics above ---
          FROM asp_migration_event
         GROUP BY
               application_name,
               footprint,
               denver_date
         ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       wid,
       metric_name,
       successes_instances/starts_instances,
       denver_date
  FROM
      (select platform,
              footprint,
              wid,
              'one_time_payment_conversion_rate' as metric_name,
              SUM(IF (metric_name = 'one_time_payment_successes_instances', metric_value, 0)) as successes_instances,
              SUM(IF (metric_name = 'one_time_payment_starts_instances', metric_value, 0)) as starts_instances,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, wid, denver_date) a
 WHERE starts_instances>0
   and denver_date = "${env:REPORT_DATE}"
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       wid,
       metric_name,
       successes_instances/starts_instances,
       denver_date
  FROM
       (select platform,
               footprint,
               wid,
               'auto_payment_conversion_rate' as metric_name,
               SUM(IF (metric_name = 'set_up_auto_payment_successes_instances', metric_value, 0)) as successes_instances,
               SUM(IF (metric_name = 'set_up_auto_payment_starts_instances', metric_value, 0)) as starts_instances,
               denver_date
          from asp_migration_metrics
         group by platform, footprint, wid, denver_date) a
WHERE starts_instances>0
  and denver_date = "${env:REPORT_DATE}"
;

--Get Metrics directly from asp_v_venona_events_portals
--For just one metrics
--for employee
SELECT "CK: optin_account_daily_hh";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       denver_date
  FROM
      (SELECT platform,
              footprint,
              4 as wid,
              denver_date,
              MAP(--'portals_one_time_payment_starts_instances', SUM(IF(platform = 'specnet' AND
                  -- message__name = 'featureStart' AND message__feature__feature_name = 'oneTimeBillPayFlow', 1, 0)),
                  --'portals_optin_instances', SUM(IF(platform = 'specnet' AND
                  -- message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'startUsing', 1, 0)),
                  --'portals_optout_instances', SUM(IF(platform = 'specnet' AND
                  -- message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'notNow', 1, 0)),
                  'optin_account_daily_hh',SIZE(COLLECT_SET(IF(message__name='selectAction' AND std_name='startUsing' , acct_id, Null))),
                  'optout_account_daily_hh',SIZE(COLLECT_SET(IF(message__name='selectAction' AND std_name='notNow' , acct_id, Null))),
                  'optin_instances', SUM(IF(message__name='selectAction' AND std_name = 'startUsing', 1, 0)),
                  'optout_instances', SUM(IF(message__name='selectAction' AND std_name = 'notNow', 1, 0)),
                  'helpful_no_instances', SUM(IF(LOWER(state__view__current_page__app_section) = 'support' AND
                   message__name = 'selectAction' AND LOWER(std_name) = 'articlenothelpful', 1, 0)),
                  'helpful_yes_instances', SUM(IF(LOWER(state__view__current_page__app_section) = 'support' AND
                   message__name = 'selectAction' AND LOWER(std_name) = 'articlehelpful', 1, 0))) as tmp_map
         FROM
            (SELECT
                    LOWER(visit__application_details__application_name) AS platform,
                    CASE
                        WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'Charter'
                        WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'TWC'
                        WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'BHN'
                        WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE') OR visit__account__details__mso IS NULL) THEN 'MSO-MISSING'
                        ELSE visit__account__details__mso
                    END as footprint,
                    prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date,
                    visit__account__enc_account_number as acct_id,
                    state__view__current_page__app_section,
                    message__name,
                    message__feature__feature_name,
                    state__view__current_page__elements__standardized_name as std_name
               FROM asp_v_venona_events_portals
              WHERE (partition_date_hour_utc >= "${env:START_DATE_TZ}" AND partition_date_hour_utc < "${env:END_DATE_TZ}")
                AND LOWER(visit__application_details__application_name) IN ('specnet','smb')
                AND prod.aes_decrypt(visit__account__enc_account_number) in (
                        select distinct prod.aes_decrypt256(acct_id_256)
                          from asp_v_migration_acct
                         WHERE wstart <= "${env:REPORT_DATE}"
                           AND wid = 4
                      )) a
              WHERE denver_date = "${env:REPORT_DATE}"
              GROUP BY
                    platform,
                    footprint,
                    denver_date) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--for non-employee
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
       platform,
       footprint,
       '' as wid,
       metric_name,
       metric_value,
       denver_date
  FROM
      (SELECT platform,
              footprint,
              denver_date,
              MAP(--'portals_one_time_payment_starts_instances', SUM(IF(platform = 'specnet' AND
                  -- message__name = 'featureStart' AND message__feature__feature_name = 'oneTimeBillPayFlow', 1, 0)),
                  --'portals_optin_instances', SUM(IF(platform = 'specnet' AND
                  -- message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'startUsing', 1, 0)),
                  --'portals_optout_instances', SUM(IF(platform = 'specnet' AND
                  -- message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'notNow', 1, 0)),
                  'optin_account_daily_hh',SIZE(COLLECT_SET(IF(message__name='selectAction' AND std_name='startUsing' , acct_id, Null))),
                  'optout_account_daily_hh',SIZE(COLLECT_SET(IF(message__name='selectAction' AND std_name='notNow' , acct_id, Null))),
                  'optin_instances', SUM(IF(message__name='selectAction' AND std_name = 'startUsing', 1, 0)),
                  'optout_instances', SUM(IF(message__name='selectAction' AND std_name = 'notNow', 1, 0)),
                  'helpful_no_instances', SUM(IF(LOWER(state__view__current_page__app_section) = 'support' AND
                   message__name = 'selectAction' AND LOWER(std_name) = 'articlenothelpful', 1, 0)),
                  'helpful_yes_instances', SUM(IF(LOWER(state__view__current_page__app_section) = 'support' AND
                   message__name = 'selectAction' AND LOWER(std_name) = 'articlehelpful', 1, 0))) as tmp_map
         FROM
            (SELECT
                    LOWER(visit__application_details__application_name) AS platform,
                    CASE
                        WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'Charter'
                        WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'TWC'
                        WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'BHN'
                        WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE') OR visit__account__details__mso IS NULL) THEN 'MSO-MISSING'
                        ELSE visit__account__details__mso
                    END as footprint,
                    prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date,
                    visit__account__enc_account_number as acct_id,
                    state__view__current_page__app_section,
                    message__name,
                    message__feature__feature_name,
                    state__view__current_page__elements__standardized_name as std_name
               FROM asp_v_venona_events_portals
              WHERE (partition_date_hour_utc >= "${env:START_DATE_TZ}" AND partition_date_hour_utc < "${env:END_DATE_TZ}")
                AND LOWER(visit__application_details__application_name) IN ('specnet','smb')) a
              WHERE denver_date = "${env:REPORT_DATE}"
              GROUP BY
                    platform,
                    footprint,
                    denver_date) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--get accumulated account from asp_v_venona_events_portals directly
SELECT "CK: ${env:MigrationStartDate}", "${env:END_DATE_TZ}";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
       platform,
       footprint,
       wid,
       metric_name,
       metric_value,
       "${env:REPORT_DATE}" as denver_date
  FROM
      (SELECT 'employee' as platform,
              footprint,
              4 as wid,
              MAP('migrated_account_with_visit',SIZE(COLLECT_SET(acct_id))) as tmp_map
         FROM
            (SELECT
                    LOWER(visit__application_details__application_name) AS platform,
                    CASE
                        WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'Charter'
                        WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'TWC'
                        WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'BHN'
                        WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE') OR visit__account__details__mso IS NULL) THEN 'MSO-MISSING'
                        ELSE visit__account__details__mso
                    END as footprint,
                    prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date,
                    visit__account__enc_account_number as acct_id,
                    message__name,
                    state__view__current_page__elements__standardized_name as std_name
               FROM asp_v_venona_events_portals
              WHERE (partition_date_utc >= "${env:MigrationStartDate}" AND partition_date_hour_utc < "${env:END_DATE_TZ}")
                AND LOWER(visit__application_details__application_name) IN ('specnet','smb')
                AND visit__account__enc_account_number is not NULL
                --AND visit__account__enc_system_prin  is not NULL
                AND prod.aes_decrypt(visit__account__enc_account_number) in (
                        select distinct prod.aes_decrypt256(acct_id_256)
                          from asp_v_migration_acct
                         WHERE wstart <= "${env:REPORT_DATE}"
                           AND wid = 4
                      )) a
              WHERE denver_date <= "${env:REPORT_DATE}"
              GROUP BY
                    platform,
                    footprint) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--For non-employee
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT
        platform,
        footprint,
        '' as wid,
        metric_name,
        metric_value,
        "${env:REPORT_DATE}" as denver_date
  FROM
      (SELECT platform,
              footprint,
              MAP('migrated_account_with_visit',SIZE(COLLECT_SET(acct_id))) as tmp_map
         FROM
            (SELECT
                    LOWER(visit__application_details__application_name) AS platform,
                    CASE
                        WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'Charter'
                        WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'TWC'
                        WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'BHN'
                        WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE') OR visit__account__details__mso IS NULL) THEN 'MSO-MISSING'
                        ELSE visit__account__details__mso
                    END as footprint,
                    prod.epoch_converter(received__timestamp, 'America/Denver') as denver_date,
                    visit__account__enc_account_number as acct_id,
                    message__name,
                    state__view__current_page__elements__standardized_name as std_name
               FROM asp_v_venona_events_portals
              WHERE (partition_date_utc >= "${env:MigrationStartDate}" AND partition_date_hour_utc < "${env:END_DATE_TZ}")
                AND LOWER(visit__application_details__application_name) IN ('specnet','smb')
                AND visit__account__enc_account_number is not NULL) a
              WHERE denver_date <= "${env:REPORT_DATE}"
              GROUP BY
                    platform,
                    footprint) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

--Calculate Ratio (migrated account with login/total migrated account)
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       metric_name,
       migrated_account_with_visit/eligible_acct_completed_active,
       denver_date
  from
      (select platform,
              footprint,
              'migrated_account_with_visit_pct' as metric_name,
              SUM(IF (metric_name = 'migrated_account_with_visit', metric_value, 0)) as migrated_account_with_visit,
              SUM(IF (metric_name = 'eligible_acct_completed_active', metric_value, 0)) as eligible_acct_completed_active,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, denver_date) a
 WHERE eligible_acct_completed_active > 0
   and denver_date = "${env:REPORT_DATE}"
;

SELECT "CK: helpful_both_instances";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       metric_name,
       helpful_yes_instances+helpful_no_instances,
       denver_date
  from
      (select platform,
              footprint,
              'helpful_both_instances' as metric_name,
              SUM(IF (metric_name = 'helpful_yes_instances', metric_value, 0)) as helpful_yes_instances,
              SUM(IF (metric_name = 'helpful_no_instances', metric_value, 0)) as helpful_no_instances,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, denver_date) a
 WHERE denver_date = "${env:REPORT_DATE}"
;

SELECT "CK: one_time_payment_failure_instances";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       metric_name,
       otp_starts_instances-otp_successes_instances,
       denver_date
  from
      (select platform,
              footprint,
              'one_time_payment_failure_instances' as metric_name,
              SUM(IF (metric_name = 'one_time_payment_starts_instances', metric_value, 0)) as otp_starts_instances,
              SUM(IF (metric_name = 'one_time_payment_successes_instances', metric_value, 0)) as otp_successes_instances,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, denver_date) a
 WHERE denver_date = "${env:REPORT_DATE}"
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       metric_name,
       auto_starts_instances-auto_successes_instances,
       denver_date
  from
      (select platform,
              footprint,
              'set_up_auto_payment_failure_instances' as metric_name,
              SUM(IF (metric_name = 'set_up_auto_payment_starts_instances', metric_value, 0)) as auto_starts_instances,
              SUM(IF (metric_name = 'set_up_auto_payment_successes_instances', metric_value, 0)) as auto_successes_instances,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, denver_date) a
 WHERE denver_date = "${env:REPORT_DATE}"
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       metric_name,
       tv_starts_instances-tv_successes_instances,
       denver_date
  from
      (select platform,
              footprint,
              'tv_equipment_reset_flow_failure_instances' as metric_name,
              SUM(IF (metric_name = 'tv_equipment_reset_flow_starts_instances', metric_value, 0)) as tv_starts_instances,
              SUM(IF (metric_name = 'tv_equipment_reset_flow_successes_instances', metric_value, 0)) as tv_successes_instances,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, denver_date) a
 WHERE denver_date = "${env:REPORT_DATE}"
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       metric_name,
       internet_starts_instances-internet_successes_instances,
       denver_date
  from
      (select platform,
              footprint,
              'internet_equipment_reset_flow_failure_instances' as metric_name,
              SUM(IF (metric_name = 'internet_equipment_reset_flow_starts_instances', metric_value, 0)) as internet_starts_instances,
              SUM(IF (metric_name = 'internet_equipment_reset_flow_successes_instances', metric_value, 0)) as internet_successes_instances,
              denver_date
         from asp_migration_metrics
        group by platform, footprint, denver_date) a
 WHERE denver_date = "${env:REPORT_DATE}"
;

SELECT "CK: wave_status";
INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       'NA' as footprint,
       wid,
       metric_name,
       wave_status,
       "${env:REPORT_DATE}" as denver_date
  from
      (select CASE WHEN customer_type='Residential' THEN 'specnet'
                   WHEN customer_type='commercial_business' THEN 'smb'
                   ELSE 'employee'
              END as platform,
              wid,
              'wave_status' as metric_name,
              CASE WHEN wend <= "${env:REPORT_DATE}" THEN -1
                   WHEN wstart <= "${env:REPORT_DATE}" and wend > "${env:REPORT_DATE}" THEN 0
                   WHEN wstart > "${env:REPORT_DATE}" THEN 1
              END as wave_status
         from asp_migration_date) a
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
select *
  from
      (select platform,
              footprint,
              '' as wid,
              'migrated_account_with_visit_daily' as metric_name,
              metric_value-lag(metric_value) over (partition by platform, footprint order by denver_date) as metric_value,
              denver_date
         from asp_migration_metrics
        where metric_name = 'migrated_account_with_visit') a
where denver_date="${env:REPORT_DATE}"
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       wid,
       metric_name,
       eligible_account-accumulated_migrated_account,
       denver_date
  from
      (select platform,
             footprint,
             '' as wid,
             'bubble_account' as metric_name,
             SUM(IF (metric_name = 'migrated_account_with_visit', metric_value, 0)) as accumulated_migrated_account,
             SUM(IF (metric_name = 'eligible_acct_completed_active', metric_value, 0)) as eligible_account,
             denver_date
        from asp_migration_metrics
       WHERE denver_date="${env:REPORT_DATE}"
       group by platform, footprint, denver_date) a
 WHERE (eligible_account-accumulated_migrated_account) > 0
;

INSERT INTO asp_migration_metrics PARTITION(denver_date)
SELECT platform,
       footprint,
       '' as wid,
       'bubble_rate',
       mig_acct_daily/bubble_acct_prev,
       denver_date
FROM
      (select
             platform,
             footprint,
             mig_acct_daily,
             bubble_acct,
             lag(bubble_acct) over (partition by platform, footprint order by denver_date) as bubble_acct_prev,
             denver_date
      from
           (select platform,
                   footprint,
                   max(case when metric_name='migrated_account_with_visit_daily' then metric_value end) as mig_acct_daily,
                   max(case when metric_name='bubble_account' then metric_value end) as bubble_acct,
                   denver_date
              from asp_migration_metrics
             where metric_name in ('bubble_account','migrated_account_with_visit_daily')
             group by platform, footprint, denver_date
             order by platform, footprint, denver_date) a) b
WHEre denver_date="${env:REPORT_DATE}";
