--XGANALYTIC-8307 time_between_visits with biller region

-- link login/username with account number
DROP TABLE IF EXISTS dev.asp_fedids_all_cj_temp;

CREATE TABLE if not exists dev.asp_fedids_all_cj_temp AS
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
  WHERE (partition_date_hour_denver between '2018-01-01_00' AND '2018-01-16_23')
        AND source_app = 'portals-idp'
        AND response_code = 'UNIQUE_AUTH'
) n
LEFT JOIN (SELECT username, footprint, account_number_aes, date_month, account_is_active, account_type
             FROM dev_tmp.identities_history_new
            WHERE (is_primary_user = 'TRUE' OR is_sub_user = 'TRUE')) i
    ON n.username = i.username
    AND n.footprint = i.footprint
WHERE i.date_month = '2017-12-01'
AND n.footprint in ('TWC', 'BHN')
AND UPPER(i.account_is_active)='TRUE'
GROUP BY
    n.username
    , n.footprint
    --, n.event_timestamp
    , i.account_type
    , i.account_number_aes
;

SELECT count(*) AS login_cnt
  FROM dev.asp_fedids_all_cj_temp
 WHERE footprint = 'BHN';

---------------- TWC accounts
drop table IF EXISTS dev.asp_TWC_account_biller_region_temp;

CREATE TABLE if not exists dev.asp_TWC_account_biller_region_temp AS
select fedid.username,
       prod.aes_decrypt(a.account__number_aes) as dec_account_num,
       a.system__sys,
       a.system__prin,
       a.system__agent,
       fedid.account_type,
       a.franchise__agent_cd
  from prod.twc_account_history a
 INNER join (select *
               from fedids_all_cj_temp
              where footprint = 'TWC') fedid
    ON prod.aes_decrypt(a.account__number_aes) = fedid.dec_account_num
 WHERE a.partition_date = '2018-01-16';

select * from dev.asp_TWC_account_biller_region_temp
 where system__sys = '202'
   and system__prin = '58'
   and system__agent = '86'
   and franchise__agent_cd = '864';

------------------ BHN accounts
drop table IF EXISTS dev.asp_BHN_account_biller_region_temp;

CREATE TABLE if not exists dev.asp_BHN_account_biller_region_temp AS
select fedid.username,
       prod.aes_decrypt(a.account__number_aes) as dec_account_num,
       a.system__company,
       a.system__division,
       a.system__franchise,
       a.franchise__agent_cd,
       fedid.account_type
  from prod.bhn_account_history a
inner join (select *
              from dev.asp_fedids_all_cj_temp
             where footprint = 'BHN') fedid
    ON substr(prod.aes_decrypt(a.account__number_aes),4) = fedid.dec_account_num
 WHERE a.partition_date = '2018-01-16';

SELECT count(*)
  FROM dev.asp_bhn_account_biller_region_temp
 where system__company = '11'
   AND system__division = '12'
   AND system__franchise = '180';

--- time between logins for BHN
drop table if exists dev.asp_tmp8307_bhn_data_2017a;

create table dev.asp_tmp8307_twc_data_2017a as
SELECT c.account_type,
       c.system__sys,
       c.system__prin,
       c.system__agent,
       c.franchise__agent_cd,
       SUM(c.avg_date_diff)/count(c.account_num) AS avg_date_diff
FROM
 (SELECT a.account_num,
         a.account_type,
         a.system__sys,
         a.system__prin,
         a.system__agent,
         a.franchise__agent_cd,
		 CASE
			 WHEN Max(a.row_number) > 1 THEN SUM(a.date_diff)/(Max(a.row_number) - 1)
			 ELSE null
     END as avg_date_diff
 FROM
		 (SELECT b.dec_account_num as account_num,
             b.account_type,
             b.system__sys,
             b.system__prin,
             b.system__agent,
             b.franchise__agent_cd,
				     row_number() OVER (PARTITION BY b.dec_account_num ORDER BY f.partition_date_hour_denver) as row_number,
				     DATEDIFF(f.partition_date_hour_denver, LAG(f.partition_date_hour_denver) OVER (PARTITION BY b.dec_account_num ORDER BY f.partition_date_hour_denver)) as date_diff
				FROM prod.federated_id f,
             dev.asp_TWC_account_biller_region_temp b
       where f.twc_login = b.username
			   and f.twc_login is not null
			   and f.twc_login != ''
         and f.twc_login not like '%INVALID_CREDS%'
			   and f.partition_date_hour_denver BETWEEN '2017-01-01' AND '2017-12-31') a
 GROUP BY a.account_type, a.system__sys, a.system__prin, a.system__agent, a.franchise__agent_cd, a.account_num) c
GROUP BY c.account_type,
       c.system__sys,
       c.system__prin,
       c.system__agent,
       c.franchise__agent_cd;

--time between logins for TWC
drop table if exists dev.asp_tmp8307_bhn_data_2017a;

create table dev.asp_tmp8307_bhn_data_2017a as
SELECT
  c.account_type,
  c.system__company,
  c.system__division,
  c.system__franchise,
  c.franchise__agent_cd,
  sum(c.avg_date_diff)/count(c.account_num) as avg_date_diff
FROM
   (SELECT a.account_type, a.system__company, a.system__division, a.system__franchise, a.franchise__agent_cd, a.account_num,
  		 CASE
				 WHEN Max(a.row_number) > 1 THEN SUM(a.date_diff)/(Max(a.row_number) - 1)
				 ELSE null
		   END as avg_date_diff
   FROM
    		 (SELECT b.dec_account_num as account_num,
                b.account_type,
                b.system__company,
                b.system__division,
                b.system__franchise,
                b.franchise__agent_cd,
    				     row_number() OVER (PARTITION BY b.dec_account_num ORDER BY f.partition_date_hour_denver) as row_number,
    				     DATEDIFF(f.partition_date_hour_denver, LAG(f.partition_date_hour_denver) OVER (PARTITION BY b.dec_account_num ORDER BY f.partition_date_hour_denver)) as date_diff
    				FROM prod.federated_id f,
                 dev.asp_BHN_account_biller_region_temp b
           WHERE f.bh_login = b.username
    			   AND f.bh_login is not null
    			   AND f.bh_login != ''
             AND f.bh_login not like '%INVALID_CREDS%'
    			   AND f.partition_date_hour_denver BETWEEN '2017-01-01' AND '2017-12-31') a
     GROUP BY a.account_type, a.system__company, a.system__division, a.system__franchise, a.franchise__agent_cd, a.account_num) c
GROUP BY
  c.account_type,
  c.system__company,
  c.system__division,
  c.system__franchise,
  c.franchise__agent_cd;
