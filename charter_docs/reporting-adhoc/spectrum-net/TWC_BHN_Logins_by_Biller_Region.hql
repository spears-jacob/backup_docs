-- XGANALYTIC-8308 Biller Region Logins
-------- Total number of accounts and usernames for TWC as of Jan 16 (note this is not logged in accounts)
SELECT j.account_type
     , t.system__sys
     , t.system__prin
     , t.system__agent
     , t.franchise__agent_cd
     , SUBSTR(t.partition_date, 0, 7) AS year_month
     , COUNT (DISTINCT t.account__number_aes) AS count_account
     , COUNT (DISTINCT j.username) AS count_usernames

FROM prod.twc_account_history t
JOIN dev_tmp.identities_history_new j ON prod.aes_decrypt(t.account__number_aes) = prod.aes_decrypt(j.account_number_aes)
WHERE t.partition_date = '2018-01-16'

AND UPPER(j.account_is_active)='TRUE'
GROUP BY
      j.account_type
     , t.system__sys
     , t.system__prin
     , t.system__agent
     , t.franchise__agent_cd
     , SUBSTR(t.partition_date, 0, 7)
;


-------- Total number of accounts and usernames for BHN as of Jan 16 (note this is not logged in accounts)

SELECT
       j.account_type
     , b.system__company
     , b.system__division
     , b.system__franchise
     , b.franchise__agent_cd
     , SUBSTR(b.partition_date, 0, 7) AS year_month
     , 'ICOMS' AS biller
     , COUNT (DISTINCT b.account__number_aes) AS count_account
     , COUNT (DISTINCT j.username) AS count_usernames
FROM prod.bhn_account_history b
LEFT JOIN dev_tmp.identities_history_new j ON SUBSTR(prod.aes_decrypt(b.account__number_aes),4) = prod.aes_decrypt(j.account_number_aes)
WHERE
  b.partition_date = '2018-01-16'
 AND UPPER(j.account_is_active)='TRUE'
GROUP BY
      j.account_type
      ,b.system__company
     , b.system__division
     , b.system__franchise
     , b.franchise__agent_cd
     , SUBSTR(b.partition_date, 0, 7);


-- The below query generates the total number of logins to spectrum.net from Jan 1 2018 to Jan 16 2018 for TWC and BHN

DROP TABLE IF EXISTS fedids_all_cj_temp;

CREATE TABLE if not exists fedids_all_cj_temp AS

SELECT
      n.username AS username
    , n.footprint
    , prod.aes_decrypt(i.account_number_aes) as dec_account_num
    , i.account_type
    , count(*) as login_cnt
FROM (
    SELECT
        CASE
            WHEN charter_login <> '' THEN charter_login
            WHEN twc_login <> '' THEN twc_login
            WHEN bh_login <> '' THEN bh_login
        END AS username,
        CASE
            WHEN charter_login <> '' THEN 'Charter'
            WHEN twc_login <> '' THEN 'TWC'
            WHEN bh_login <> '' THEN 'BHN'
        END AS footprint
        --event_timestamp
    FROM prod.federated_id
    WHERE
        (partition_date_hour_denver between '2018-01-01_00' AND '2018-01-16_23')
        AND source_app = 'portals-idp' -- spectrum.net only
        AND response_code = 'UNIQUE_AUTH' -- filters for successful authorization attempts

) n
-- joining to identities on username and footprint will give us the account number
LEFT JOIN (SELECT username, footprint, account_number_aes, date_month, account_is_active, account_type FROM dev_tmp.identities_history_new WHERE (is_primary_user = 'TRUE' OR is_sub_user = 'TRUE')) i
    ON n.username = i.username
    AND n.footprint = i.footprint
where i.date_month = '2017-12-01' -- identities table has a snapshot of data received on jan 16. Labelled as prior month.
AND n.footprint in ('TWC', 'BHN')
AND UPPER(i.account_is_active)='TRUE'
GROUP BY
    n.username
    , n.footprint
    --, n.event_timestamp
    , i.account_type
    , i.account_number_aes

;

------------- testing fed ids table -------------------

select count(*) AS login_cnt from fedids_all_cj_temp  where footprint = 'BHN';

---------------- The below query brings in biller region information for TWC and allows us to count the number of logins in each biller region
drop table IF EXISTS TWC_biller_region_temp;

CREATE TABLE if not exists TWC_biller_region_temp AS

select
b.system__sys,
b.system__prin,
b.system__agent,
b.account_type,
b.franchise__agent_cd,
sum(login_cnt) AS login_cnt, -- total number of logins
count(distinct b.dec_account_num) As accounts -- total accounts that had at least one login

from
(
      select
        prod.aes_decrypt(a.account__number_aes) as dec_account_num,
        a.system__sys,
        a.system__prin,
        a.system__agent,
        fedid.account_type,
        a.franchise__agent_cd,
        sum(fedid.login_cnt) AS login_cnt

      from
        prod.twc_account_history a
-- joining the account numbers from users who logged in to the account history table. We only want users who logged in and have an associated biller region so we use an inner join
        INNER join (select * from fedids_all_cj_temp where footprint = 'TWC') fedid
          ON
            (prod.aes_decrypt(a.account__number_aes) = fedid.dec_account_num)

        WHERE a.partition_date = '2018-01-16'

      Group by
        prod.aes_decrypt(a.account__number_aes),
        a.system__sys,
        a.system__prin,
        a.system__agent,
        fedid.account_type,
        a.franchise__agent_cd
) b

group by
b.system__sys,
b.system__prin,
b.system__agent,
b.account_type,
b.franchise__agent_cd;

---- testing TWC -----------------
select * from TWC_biller_region_temp where system__sys = '202' and system__prin = '58' and system__agent = '86' and franchise__agent_cd = '864';

---------------- The below query brings in biller region information for BHN and allows us to count the number of logins in each biller region
drop table IF EXISTS BHN_biller_region_temp;

CREATE TABLE if not exists BHN_biller_region_temp AS

select
  b.system__company,
  b.system__division,
  b.system__franchise,
  b.franchise__agent_cd,
  b.account_type,
  sum(login_cnt) AS login_cnt, -- total logins
  count(distinct b.dec_account_num) As accounts -- total accounts that had at least one login

from
(
      select
        prod.aes_decrypt(a.account__number_aes) as dec_account_num,
        a.system__company,
        a.system__division,
        a.system__franchise,
        a.franchise__agent_cd,
        fedid.account_type,
        Sum(fedid.login_cnt) AS login_cnt

      from prod.bhn_account_history a

-- joining the account numbers from users who logged in to the account history table. We only want users who logged in and have an associated biller region so we use an inner join
      inner join (select * from fedids_all_cj_temp where footprint = 'BHN') fedid
      ON
      (substr(prod.aes_decrypt(a.account__number_aes),4) = fedid.dec_account_num)

      WHERE a.partition_date = '2018-01-16'

      Group by
        prod.aes_decrypt(a.account__number_aes),
        a.system__company,
        a.system__division,
        a.system__franchise,
        a.franchise__agent_cd,
        fedid.account_type
) b


group by
  b.system__company,
  b.system__division,
  b.system__franchise,
  b.franchise__agent_cd,
  b.account_type;

---------- testing biller region ------------------
select count(*) from bhn_biller_region_temp where system__company = '11' AND system__division = '12' and system__franchise = '180'

drop table if exists fedids_all_cj_temp;
