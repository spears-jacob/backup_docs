USE ${env:ENVIRONMENT};

SELECT 'BHN', '${env:REPORT_MONTH}', '${env:MAX_IDM_TWC}',
       '${env:ID_IDM_DATE}', '${env:ACCOUNT_HISTORY_DATE}',
       '${env:FID_START_DATE}', '${env:FID_END_DATE}',
       '${env:START_DATE}', '${env:END_DATE}'
;
--BHN

--create monthly bhn_account_history to avoid memory error
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_account_history as
select distinct t.account__number_aes,
      t.system__company,
      t.system__division,
      t.system__franchise,
      t.franchise__agent_cd,
      t.customer__disconnect_date,
      t.customer__type,
      t.account__type,
      t.partition_date
 from prod.bhn_account_history t
where t.partition_date = "${env:ACCOUNT_HISTORY_DATE}";

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_idm_max AS
SELECT CASE WHEN (datediff("${env:ID_IDM_DATE}","${env:MAX_IDM_BHN}") > 0 )
            THEN "${env:MAX_IDM_BHN}" ELSE "${env:ID_IDM_DATE}" END AS idm_date;

-- create table with login count data for all 3 domains (TWC, BHN and Charter)
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_lookup AS
SELECT n.username AS username
     , n.footprint
     , prod.aes_decrypt(i.account_number_aes) as dec_account_num
     , i.account_type
     , i.account__type
     , count(*) as login_cnt
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
                  ${env:TMP_db}.asp_phase4_bhn_account_history b
            WHERE (is_primary_user = true OR is_sub_user = true)
              and a.month in (select idm_date from ${env:TMP_db}.asp_phase4_bhn_idm_max)
              and SUBSTR (prod.aes_decrypt(b.account__number_aes),4) = prod.aes_decrypt(a.account_number_aes)
              AND a.username is not null) i
       -- join identities_idm_history to federated_id to get account type and account number
       ON n.username = i.username
      AND n.footprint = substr(i.footprint,1,3)
where i.month in (select idm_date from ${env:TMP_db}.asp_phase4_bhn_idm_max)
  AND n.footprint in ('BHN')
  AND i.is_account_active=true
GROUP BY n.username
       , n.footprint
       , i.account_type
       , i.account__type
       , i.account_number_aes ;

--to get active account and active user for selected month,
--account_type and biller region
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r1
as
SELECT t.customer__type as account_type
     , t.account__type
     , t.system__company
     , t.system__division
     , t.system__franchise
     , t.franchise__agent_cd
     , t.partition_date
     , 'ICOMS' as biller
     , COUNT (DISTINCT (CASE WHEN t.customer__disconnect_date IS NULL AND j.is_identity_active = true THEN t.account__number_aes ELSE NULL END ))AS active_accounts_with_active_users
     , COUNT (DISTINCT (CASE WHEN t.customer__disconnect_date IS NULL THEN t.account__number_aes ELSE NULL END ))AS total_active_accounts
     -- only count customer without disconnect_date, which is active account
     , COUNT (DISTINCT (CASE WHEN j.is_identity_active = true THEN j.username END)) AS total_active_users
     --- Total users with active status
 FROM ${env:TMP_db}.asp_phase4_bhn_account_history t
 LEFT JOIN (select *
              FROM prod.identities_idm_history
             WHERE month in (select idm_date from ${env:TMP_db}.asp_phase4_bhn_idm_max)
               AND footprint = 'BHN') j
   ON SUBSTR (prod.aes_decrypt(t.account__number_aes),4) = prod.aes_decrypt(j.account_number_aes)
 GROUP BY
      t.customer__type
     , t.account__type
     , t.system__company
     , t.system__division
     , t.system__franchise
     , t.franchise__agent_cd
     , t.partition_date;

--to get monthly total logins and total accts w/at least 1 login
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r2 AS
select
        b.account_type,
        b.account__type,
        b.biller,
        b.system__company,
        b.system__division,
        b.system__franchise,
        b.franchise__agent_cd,
        sum(login_cnt) AS login_cnt,
        -- get login count for account type and biller
        count(distinct b.account__number_aes) As accounts
        -- get account count with login
from
  (select
          fedid.account_type,
          fedid.account__type,
          'ICOMS' as biller,
          a.account__number_aes,
          a.system__company,
          a.system__division,
          a.system__franchise,
          a.franchise__agent_cd,
          sum(fedid.login_cnt) AS login_cnt
          -- login count for account type, biller and specific account
     from ${env:TMP_db}.asp_phase4_bhn_account_history a
    INNER join (select * from ${env:TMP_db}.asp_phase4_bhn_lookup where footprint = 'BHN') fedid
       ON (substr(prod.aes_decrypt(a.account__number_aes),4) = fedid.dec_account_num)
          --- join dev.asp_phase4_bhn_lookup to account table to add login count to account
     Group by
                fedid.account_type,
                fedid.account__type,
                a.account__number_aes,
                a.system__company,
                a.system__division,
                a.system__franchise,
                a.franchise__agent_cd) b
group by
      b.account_type,
        b.account__type,
      b.biller,
      b.system__company,
      b.system__division,
      b.system__franchise,
      b.franchise__agent_cd;

--create temp table, which will be used to get time_between_logins
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r3 AS
select fedid.username,
       prod.aes_decrypt(a.account__number_aes) as dec_account_num,
       fedid.account_type,
       fedid.account__type,
       'ICOMS' as biller,
       a.system__company,
       a.system__division,
       a.system__franchise,
       a.franchise__agent_cd
  from ${env:TMP_db}.asp_phase4_bhn_account_history a
 inner join (select *
              from ${env:TMP_db}.asp_phase4_bhn_lookup
             where footprint = 'BHN') fedid
    ON (substr(prod.aes_decrypt(a.account__number_aes),4) = fedid.dec_account_num);
       --- join dev.asp_phase4_bhn_lookup to account history table to add account_type, username to account and biller

--get yearly time between logins
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r4 as
SELECT c.account_type,
       c.account__type,
       c.biller,
       c.system__company,
       c.system__division,
       c.system__franchise,
       c.franchise__agent_cd,
       SUM(c.avg_date_diff)/count(c.account_num) AS avg_date_diff
       -- avg time difference for account type
  FROM
(SELECT  a.account_type,
         a.account__type,
         a.biller,
         a.account_num,
         a.system__company,
         a.system__division,
         a.system__franchise,
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
             b.system__company,
             b.system__division,
             b.system__franchise,
             b.franchise__agent_cd,
             b.biller,
             row_number() OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver) as row_number,
             DATEDIFF(f.date_hour_denver, LAG(f.date_hour_denver) OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver)) as date_diff
        FROM asp_v_federated_identity f,
             ${env:TMP_db}.asp_phase4_bhn_r3 b
             -- join federated_id with temp above to get account type, account, biller for login user
       where prod.aes_decrypt256(f.username_aes256) = b.username
         and prod.aes_decrypt256(f.username_aes256) is not null
         and prod.aes_decrypt256(f.username_aes256) != ''
         and prod.aes_decrypt256(f.username_aes256) not like '%INVALID_CREDS%'
         and f.date_hour_denver BETWEEN "${env:START_DATE" AND "${env:END_DATE}") a
  GROUP BY a.biller, a.account_type, a.account__type, a.system__company, a.system__division, a.system__franchise, a.franchise__agent_cd, a.account_num) c
GROUP BY c.account_type,
         c.account__type,
         c.biller,
         c.system__company,
         c.system__division,
         c.system__franchise,
         c.franchise__agent_cd;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r4n2 as
SELECT  c.account_type,
        c.account__type,
        c.biller,
        c.system__company,
        c.system__division,
        c.system__franchise,
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
            a.system__company,
            a.system__division,
            a.system__franchise,
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
            b.system__company,
            b.system__division,
            b.system__franchise,
            b.franchise__agent_cd,
            b.biller,
            row_number() OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver) as row_number,
            DATEDIFF(f.date_hour_denver, LAG(f.date_hour_denver) OVER (PARTITION BY b.dec_account_num ORDER BY f.date_hour_denver)) as date_diff
       FROM asp_v_federated_identity f,
            ${env:TMP_db}.asp_phase4_bhn_r3 b
            -- join federated_id with temp above to get account type, account, biller for login user
      where prod.aes_decrypt256(f.username_aes256) = b.username
        and prod.aes_decrypt256(f.username_aes256) is not null
        and prod.aes_decrypt256(f.username_aes256) != ''
        and prod.aes_decrypt256(f.username_aes256) not like '%INVALID_CREDS%'
        and f.date_hour_denver BETWEEN "${env:START_DATE}" AND "${env:END_DATE}") a
      GROUP BY a.biller, a.account_type, a.account__type, a.system__company, a.system__division, a.system__franchise, a.franchise__agent_cd, a.account_num) c
GROUP BY c.account_type,
         c.account__type,
        c.biller,
        c.system__company,
        c.system__division,
        c.system__franchise,
        c.franchise__agent_cd;

-- get monthly average login number for the selected year
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r4n as
 SELECT c.account_type,
        c.account__type,
        c.biller,
        c.system__company,
        c.system__division,
        c.system__franchise,
        c.franchise__agent_cd,
        avg(c.login_cnt) AS avg_login_cnt
        -- monthly average login count
   FROM
     (SELECT  a.account_type,
              a.account__type,
              a.biller,
              a.system__company,
              a.system__division,
              a.system__franchise,
              a.franchise__agent_cd,
              a.partition_date_month,
              count(a.username) login_cnt
              -- login count for account type, biller and month
      FROM
         (SELECT
                 b.account_type,
                 b.account__type,
                 b.system__company,
                 b.system__division,
                 b.system__franchise,
                 b.franchise__agent_cd,
                 b.biller,
                 substr(f.date_hour_denver,0,7) partition_date_month,
                 prod.aes_decrypt256(f.username_aes256) username
            FROM asp_v_federated_identity f,
                 ${env:TMP_db}.asp_phase4_bhn_r3 b
                 -- join federated_id with temp above to get account type, account, biller for login user
           where prod.aes_decrypt256(f.username_aes256) = b.username
             and prod.aes_decrypt256(f.username_aes256) is not null
             and prod.aes_decrypt256(f.username_aes256) != ''
             and prod.aes_decrypt256(f.username_aes256) not like '%INVALID_CREDS%'
             and f.date_hour_denver BETWEEN "${env:START_DATE}" AND "${env:END_DATE}") a
           GROUP BY a.biller,
                    a.account_type,
                    a.account__type,
                    a.system__company,
                    a.system__division,
                    a.system__franchise,
                    a.franchise__agent_cd,
                    a.partition_date_month) c
 GROUP BY c.account_type,
          c.account__type,
          c.biller,
          c.system__company,
          c.system__division,
          c.system__franchise,
          c.franchise__agent_cd;

--get the whole report
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_phase4_bhn_r5 as
SELECT a.*,
       b.login_cnt as Total_logins,
       b.accounts as Total_accts_w_at_least_1_login,
       d.avg_login_cnt,
       c.avg_date_diff,
       f.avg_date_diff as avg_date_diff2
  from ${env:TMP_db}.asp_phase4_bhn_r1 a
  left join ${env:TMP_db}.asp_phase4_bhn_r2 b
    on a.account_type = b.account_type
   AND a.account__type = b.account__type
   and a.biller = b.biller
   and a.system__company = b.system__company
   and a.system__division = b.system__division
   and a.system__franchise == b.system__franchise
   and a.franchise__agent_cd =  b.franchise__agent_cd
  left join ${env:TMP_db}.asp_phase4_bhn_r4 c
    on a.account_type = c.account_type
   AND a.account__type = c.account__type
   and a.biller = c.biller
   and a.system__company = c.system__company
   and a.system__division = c.system__division
   and a.system__franchise == c.system__franchise
   and a.franchise__agent_cd =  c.franchise__agent_cd
  left join ${env:TMP_db}.asp_phase4_bhn_r4n d
    on a.account_type = d.account_type
   AND a.account__type = d.account__type
   and a.biller = d.biller
   and a.system__company = d.system__company
   and a.system__division = d.system__division
   and a.system__franchise == d.system__franchise
   and a.franchise__agent_cd =  d.franchise__agent_cd
  left join ${env:TMP_db}.asp_phase4_bhn_r4n2 f
    on a.account_type = f.account_type
   AND a.account__type = f.account__type
   and a.biller = f.biller
   and a.system__company = f.system__company
   and a.system__division = f.system__division
   and a.system__franchise == f.system__franchise
   and a.franchise__agent_cd =  f.franchise__agent_cd;

   INSERT overwrite table asp_phase4_sizing partition (domain='BHN', partition_date_month="${env:REPORT_MONTH}")
     SELECT
            case
                when array_contains(split(account_type,'_'),'COMMERCIAL') THEN 'COMMERCIAL'
                when array_contains(split(account_type,'_'),'RESIDENTIAL') THEN 'RESIDENTIAL'
                else 'N/A' end
            as customer_type,
            account__type,
            system__company,
            system__division,
            system__franchise,
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
            CASE WHEN (datediff("${env:ID_IDM_DATE}","${env:MAX_IDM_BHN}") > 0 )
                 THEN "${env:MAX_IDM_BHN}" ELSE "${env:ID_IDM_DATE}" END AS idm_date,
            "${env:ACCOUNT_HISTORY_DATE}",
            "${env:FID_START_DATE}",
            "${env:FID_END_DATE}",
            "${env:START_DATE}",
            "${env:END_DATE}"
       FROM ${env:TMP_db}.asp_phase4_bhn_r5;
