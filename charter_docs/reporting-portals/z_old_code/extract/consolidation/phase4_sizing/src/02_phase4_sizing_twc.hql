USE ${env:ENVIRONMENT};

SELECT 'TWC', '${env:REPORT_MONTH}', '${env:MAX_IDM_TWC}',
       '${env:ID_IDM_DATE}', '${env:ACCOUNT_HISTORY_DATE}',
       '${env:FID_START_DATE}', '${env:FID_END_DATE}',
       '${env:START_DATE}', '${env:END_DATE}'
;

--TWC

-- create monthly twc_account_history table to avoid memory error
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_account_history AS
SELECT DISTINCT
       t.account__number_aes,
       t.system__sys,
       t.system__prin,
       t.system__agent,
       t.franchise__agent_cd,
       SUBSTR(t.partition_date, 0, 7) AS partition_date_month,
       t.customer__type,
       t.account__type,
       t.customer__disconnect_date
  FROM prod.twc_account_history t
 WHERE t.partition_date = "${env:ACCOUNT_HISTORY_DATE}"
   AND bcn_fl=false;

-- create monthly twc_account_equipment_history table to avoid memory error
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_account_equip_history AS
SELECT DISTINCT
       e.account__number_aes,
       e.equipment__data_source_type_code as biller
  FROM prod.twc_account_equipment_history e
 WHERE e.partition_date = "${env:ACCOUNT_HISTORY_DATE}";

 CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_idm_max AS
 SELECT CASE WHEN (datediff("${env:ID_IDM_DATE}","${env:MAX_IDM_TWC}") > 0 )
             THEN "${env:MAX_IDM_TWC}" ELSE "${env:ID_IDM_DATE}" END AS idm_date;

-- create table with login count data for all 3 domains (TWC, BHN and Charter)
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_lookup AS
SELECT n.username AS username,
       n.footprint,
       prod.aes_decrypt(i.account_number_aes) as dec_account_num,
       i.account_type,
       i.account__type,
       count(*) as login_cnt
 FROM (SELECT
             prod.aes_decrypt256(username_aes256) as username,
             footprint
        FROM asp_v_federated_identity
       WHERE (date_hour_denver between "${env:FID_START_DATE}" AND "${env:FID_END_DATE}")
         AND prod.aes_decrypt256(username_aes256) is not null
         AND source_app = 'portals-idp'
         AND is_success = true) n
LEFT JOIN (SELECT a.username,
                  a.footprint,
                  a.account_number_aes,
                  a.month,
                  a.is_account_active,
                  b.customer__type as account_type,
                  b.account__type
             FROM prod.identities_idm_history a,
                  ${env:TMP_db}.asp_phase4_twc_account_history b
            WHERE (is_primary_user = true OR is_sub_user = true)
              AND a.account_number_aes = b.account__number_aes
              AND a.username is not null) i
       -- join identities_idm_history to federated_id to get account type and account number
   ON n.username = i.username
  AND n.footprint = i.footprint
WHERE i.month in (select idm_date from ${env:TMP_db}.asp_phase4_twc_idm_max)
  AND n.footprint in ('TWC')
  AND i.is_account_active=true
GROUP BY
       n.username,
       n.footprint,
       i.account_type,
       i.account__type,
       i.account_number_aes;

-- to get active account and active user for selected month,
-- account_type and biller region for TWC
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r1 AS
SELECT t.customer__type as account_type,
       t.account__type,
       t.system__sys,
       t.system__prin,
       t.system__agent,
       t.franchise__agent_cd,
       t.partition_date_month,
       e.biller as biller,
       COUNT (DISTINCT (CASE WHEN t.customer__disconnect_date IS NULL AND j.is_identity_active = true THEN t.account__number_aes ELSE NULL END ))AS active_accounts_with_active_users,
       COUNT (DISTINCT (CASE WHEN t.customer__disconnect_date IS NULL THEN t.account__number_aes ELSE NULL END ))AS total_active_accounts,
     -- only count customer without disconnect_date, which is active account
       COUNT (DISTINCT (CASE WHEN j.is_identity_active = true THEN j.username END)) AS total_active_users
     --- Total users with active or unverified status
 FROM ${env:TMP_db}.asp_phase4_twc_account_history t
     --- join account to identities table to add account type
 LEFT JOIN (SELECT *
              FROM prod.identities_idm_history
             WHERE month in (select idm_date from ${env:TMP_db}.asp_phase4_twc_idm_max)
               and upper(footprint)='TWC') j
      ON t.account__number_aes = j.account_number_aes
 JOIN ${env:TMP_db}.asp_phase4_twc_account_equip_history e
     --- join account to account_equipment_history table to add biller
      ON t.account__number_aes = e.account__number_aes
GROUP BY
      t.customer__type,
      t.account__type,
      t.system__sys,
      t.system__prin,
      t.system__agent,
      t.franchise__agent_cd,
      t.partition_date_month,
      e.biller;

--to get monthly total logins and total accts w/at least 1 login
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r2 AS
SELECT
        b.account_type,
        b.account__type,
        b.system__sys,
        b.system__prin,
        b.system__agent,
        b.franchise__agent_cd,
        b.biller,
        sum(login_cnt) AS login_cnt,
        -- get login count for account type and biller
        count(distinct b.account__number_aes) As accounts
        -- get account count with login
FROM
  (SELECT
          fedid.account_type,
          fedid.account__type,
          e.biller,
          a.account__number_aes,
          a.system__sys,
          a.system__prin,
          a.system__agent,
          a.franchise__agent_cd,
          sum(fedid.login_cnt) AS login_cnt
          -- login count for account type, biller and specific account
     FROM ${env:TMP_db}.asp_phase4_twc_account_history a
    INNER join (SELECT * FROM ${env:TMP_db}.asp_phase4_twc_lookup WHERE footprint = 'TWC') fedid
       ON prod.aes_decrypt(a.account__number_aes) = fedid.dec_account_num
          --- join dev.asp_phase4_twc_lookup to account table to add login count to account
     JOIN ${env:TMP_db}.asp_phase4_twc_account_equip_history e
       ON a.account__number_aes = e.account__number_aes
          --- join account to account_equipment_history table to add biller
     GROUP BY
            fedid.account_type,
            fedid.account__type,
            e.biller,
            a.account__number_aes,
            a.system__sys,
            a.system__prin,
            a.system__agent,
            a.franchise__agent_cd) b
GROUP BY
        b.account_type,
        b.account__type,
        b.biller,
        b.system__sys,
        b.system__prin,
        b.system__agent,
        b.franchise__agent_cd;

-- create temp lookup table with account type, account number, username, biller
-- which will be used to get time_between_logins
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r3 AS
SELECT fedid.username,
       prod.aes_decrypt(a.account__number_aes) as dec_account_num,
       fedid.account_type,
       fedid.account__type,
       e.biller,
       a.system__sys,
       a.system__prin,
       a.system__agent,
       a.franchise__agent_cd
  FROM ${env:TMP_db}.asp_phase4_twc_account_history a
 INNER JOIN (SELECT *
              FROM ${env:TMP_db}.asp_phase4_twc_lookup
             WHERE footprint = 'TWC') fedid
    ON prod.aes_decrypt(a.account__number_aes) = fedid.dec_account_num
       --- join dev.asp_phase4_twc_lookup to account history table to add account_type, username to account and biller
  JOIN ${env:TMP_db}.asp_phase4_twc_account_equip_history e
    ON a.account__number_aes = e.account__number_aes;
       --- join account history to account_equipment_history table to add biller

--get time between logins
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r4 as
SELECT c.account_type,
       c.account__type,
       c.biller,
       c.system__sys,
       c.system__prin,
       c.system__agent,
       c.franchise__agent_cd,
       SUM(c.avg_date_diff)/count(c.account_num) AS avg_date_diff
       -- avg time difference for account type
  FROM
(SELECT  a.account_type,
         a.account__type,
         a.biller,
         a.account_num,
         a.system__sys,
         a.system__prin,
         a.system__agent,
         a.franchise__agent_cd,
         CASE
           WHEN Max(a.row_number) > 1 THEN SUM(a.date_diff)/(Max(a.row_number) - 1)
           ELSE null
        END as avg_date_diff
        -- avg time difference for an account
   FROM
    (SELECT b.dec_account_num as account_num,
            b.account_type,
            b.account__type,
            b.system__sys,
            b.system__prin,
            b.system__agent,
            b.franchise__agent_cd,
            b.biller,
            row_number() OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver) as row_number,
            DATEDIFF(f.date_hour_denver, LAG(f.date_hour_denver) OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver)) as date_diff
            -- time difference between login
       FROM asp_v_federated_identity f,
            ${env:TMP_db}.asp_phase4_twc_r3 b
            -- join federated_id with temp above to get account type, account, biller for login user
      WHERE prod.aes_decrypt256(f.username_aes256) = b.username
        AND prod.aes_decrypt256(f.username_aes256) is not null
        AND prod.aes_decrypt256(f.username_aes256) != ''
        AND prod.aes_decrypt256(f.username_aes256) not like '%INVALID_CREDS%'
        AND f.date_hour_denver BETWEEN "${env:START_DATE}" AND "${env:END_DATE}") a
  GROUP BY a.biller, a.account_type, a.account__type, a.system__sys, a.system__prin, a.system__agent, a.franchise__agent_cd, a.account_num) c
GROUP BY c.account_type,
         c.account__type,
         c.biller,
         c.system__sys,
         c.system__prin,
         c.system__agent,
         c.franchise__agent_cd;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r4n2 as
SELECT c.account_type,
      c.account__type,
      c.biller,
      c.system__sys,
      c.system__prin,
      c.system__agent,
      c.franchise__agent_cd,
      CASE
       WHEN sum(num_of_inter) > 0 THEN SUM(sum_date_diff)/SUM(num_of_inter)
       ELSE null
      END as avg_date_diff
      -- avg time difference for account type
 FROM
    (SELECT  a.account_type,
            a.account__type,
            a.biller,
            a.account_num,
            a.system__sys,
            a.system__prin,
            a.system__agent,
            a.franchise__agent_cd,
            sum(date_diff) sum_date_diff,
            CASE
              WHEN Max(a.row_number) > 1 THEN (Max(a.row_number) - 1)
              ELSE null
            END as num_of_inter
            -- sum for each account
    FROM
       (SELECT b.dec_account_num as account_num,
               b.account_type,
               b.account__type,
               b.system__sys,
               b.system__prin,
               b.system__agent,
               b.franchise__agent_cd,
               b.biller,
               row_number() OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver) as row_number,
               DATEDIFF(f.date_hour_denver, LAG(f.date_hour_denver) OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver)) as date_diff
               -- time difference between login
          FROM asp_v_federated_identity f,
               ${env:TMP_db}.asp_phase4_twc_r3 b
               -- join federated_id with temp above to get account type, account, biller for login user
         WHERE prod.aes_decrypt256(f.username_aes256) = b.username
           AND prod.aes_decrypt256(f.username_aes256) is not null
           AND prod.aes_decrypt256(f.username_aes256) != ''
           AND prod.aes_decrypt256(f.username_aes256) not like '%INVALID_CREDS%'
           AND f.date_hour_denver BETWEEN "${env:START_DATE}" AND "${env:END_DATE}") a
         GROUP BY a.biller, a.account_type, a.account__type, a.system__sys, a.system__prin, a.system__agent, a.franchise__agent_cd, a.account_num) c
GROUP BY c.account_type,
        c.account__type,
        c.biller,
        c.system__sys,
        c.system__prin,
        c.system__agent,
        c.franchise__agent_cd;

 -- get monthly average login number
 CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r4n as
 SELECT c.account_type,
        c.account__type,
        c.biller,
        c.system__sys,
        c.system__prin,
        c.system__agent,
        c.franchise__agent_cd,
        avg(c.login_cnt) AS avg_login_cnt
        -- monthly average login count
   FROM
     (SELECT  a.account_type,
              a.account__type,
              a.biller,
              a.system__sys,
              a.system__prin,
              a.system__agent,
              a.franchise__agent_cd,
              a.partition_date_month,
              count(a.username) login_cnt
              -- login count for account type, biller and month
      FROM
         (SELECT
                 b.account_type,
                 b.account__type,
                 b.system__sys,
                 b.system__prin,
                 b.system__agent,
                 b.franchise__agent_cd,
                 b.biller,
                 substr(f.date_hour_denver,0,7) partition_date_month,
                 prod.aes_decrypt256(f.username_aes256) username
            FROM asp_v_federated_identity f,
                 ${env:TMP_db}.asp_phase4_twc_r3 b
                 -- join federated_id with temp above to get account type, account, biller for login user
           WHERE prod.aes_decrypt256(f.username_aes256) = b.username
             AND prod.aes_decrypt256(f.username_aes256) is not null
             AND prod.aes_decrypt256(f.username_aes256) != ''
             AND prod.aes_decrypt256(f.username_aes256) not like '%INVALID_CREDS%'
             AND f.date_hour_denver BETWEEN "${env:START_DATE}" AND "${env:END_DATE}") a
           GROUP BY a.biller,
                    a.account_type,
                    a.account__type,
                    a.system__sys,
                    a.system__prin,
                    a.system__agent,
                    a.franchise__agent_cd,
                    a.partition_date_month) c
  GROUP BY c.account_type,
          c.account__type,
          c.biller,
          c.system__sys,
          c.system__prin,
          c.system__agent,
          c.franchise__agent_cd;

--get the whole report
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_twc_r5
as
SELECT a.*,
       b.login_cnt as Total_logins,
       b.accounts as Total_accts_w_at_least_1_login,
       d.avg_login_cnt,
       c.avg_date_diff,
       f.avg_date_diff as avg_date_diff2
  FROM ${env:TMP_db}.asp_phase4_twc_r1 a
  LEFT JOIN ${env:TMP_db}.asp_phase4_twc_r2 b
    ON a.account_type = b.account_type
   AND a.account__type = b.account__type
   AND a.biller = b.biller
   AND a.system__sys = b.system__sys
   AND a.system__prin = b.system__prin
   AND a.system__agent == b.system__agent
   AND a.franchise__agent_cd =  b.franchise__agent_cd
  LEFT JOIN ${env:TMP_db}.asp_phase4_twc_r4 c
    ON a.account_type = c.account_type
   AND a.account__type = c.account__type
   AND a.biller = c.biller
   AND a.system__sys = c.system__sys
   AND a.system__prin = c.system__prin
   AND a.system__agent == c.system__agent
   AND a.franchise__agent_cd =  c.franchise__agent_cd
  LEFT JOIN ${env:TMP_db}.asp_phase4_twc_r4n d
    ON a.account_type = d.account_type
   AND a.account__type = d.account__type
   AND a.biller = d.biller
   AND a.system__sys = d.system__sys
   AND a.system__prin = d.system__prin
   AND a.system__agent == d.system__agent
   AND a.franchise__agent_cd =  d.franchise__agent_cd
  LEFT JOIN ${env:TMP_db}.asp_phase4_twc_r4n2 f
    ON a.account_type = f.account_type
   AND a.account__type = f.account__type
   AND a.biller = f.biller
   AND a.system__sys = f.system__sys
   AND a.system__prin = f.system__prin
   AND a.system__agent == f.system__agent
   AND a.franchise__agent_cd =  f.franchise__agent_cd;

   INSERT overwrite table asp_phase4_sizing partition (domain='TWC', partition_date_month="${env:REPORT_MONTH}")
     SELECT
            case
                when array_contains(split(account_type,'_'),'COMMERCIAL') THEN 'COMMERCIAL'
                when array_contains(split(account_type,'_'),'RESIDENTIAL') THEN 'RESIDENTIAL'
                else 'N/A' end
            as customer_type,
            account__type,
            system__sys,
            system__prin,
            system__agent,
            franchise__agent_cd,
            biller,
            active_accounts_with_active_users,
            total_active_accounts,
            total_active_users,
            total_logins,
            total_accts_w_at_least_1_login,
            avg_login_cnt,
            avg_date_diff,
            avg_date_diff2,
            CASE WHEN (datediff("${env:ID_IDM_DATE}","${env:MAX_IDM_TWC}") > 0 )
                 THEN "${env:MAX_IDM_TWC}" ELSE "${env:ID_IDM_DATE}" END AS idm_date,
            "${env:ACCOUNT_HISTORY_DATE}",
            "${env:FID_START_DATE}",
            "${env:FID_END_DATE}",
            "${env:START_DATE}",
            "${env:END_DATE}"
       FROM ${env:TMP_db}.asp_phase4_twc_r5;
