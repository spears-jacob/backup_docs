set RUN_DATE = '2019-06-19';

--check non-employee account
drop table if EXISTS asp_migration_check;
create table IF NOT EXISTS asp_migration_check
AS
Select distinct
       b.wid as wid,
       b.company as company,
       b.account_number as account_number,
       b.system as system,
       b.prin as prin,
       b.agent as agent,
       bprin,
       bagent
FROM
      (select a.wid as wid,
             a.company as company,
             a.account_number as account_number,
             a.system as system,
             a.prin as prin,
             a.agent as agent,
             a.bprin,
             a.bagent,
             case when bagent = 'All' and bprin='All' THEN 1
                  when bagent = 'All' and bprin!= 'All' AND a.prin = bprin THEN 1
                  when bagent != 'All' and a.agent=bagent AND a.prin = bprin THEN 1
                  else 0 end as keep
        FROM
              (select b.wid as wid,
                     a.legacy_company as company,
                     a.encrypted_legacy_account_number_256 as account_number,
                     a.system as system,
                     a.prin as prin,
                     a.agent as agent,
                     b.prin as bprin,
                     b.agent as bagent
              from prod.quantum_atom_snapshot_accounts_v a,
                   asp_migration_spa b
              where a.partition_date_denver = ${hiveconf:RUN_DATE}
              AND a.account_type in ('SUBSCRIBER','NON-EMPLOYEE')
              and a.account_status='CONNECTED'
              and a.legacy_company = b.footprint
              AND b.footprint='TWC'
              and lower(a.extract_type) = Lower(b.customer_type)
              and a.system = b.sys) a ) b
WHERE keep = 1;

--FOR BHN
insert into asp_migration_check
Select distinct
       b.wid as wid,
       b.company as company,
       b.account_number as account_number,
       b.system as system,
       b.prin as prin,
       b.agent as agent,
       bprin,
       bagent
FROM
      (select a.wid as wid,
             a.company as company,
             a.account_number as account_number,
             a.system as system,
             a.prin as prin,
             a.agent as agent,
             a.bprin,
             a.bagent,
             case when bsys='All' and bagent = 'All' and bprin='All' THEN 1
                  when bagent = 'All' and bprin='All' THEN 1
                  when bagent = 'All' and bprin!= 'All' AND a.prin = bprin THEN 1
                  when bagent != 'All' and a.agent=bagent AND a.prin = bprin THEN 1
                  else 0 end as keep
        FROM
              (select b.wid as wid,
                     a.legacy_company as company,
                     a.encrypted_legacy_account_number_256 as account_number,
                     a.system as system,
                     a.prin as prin,
                     a.agent as agent,
                     b.sys as bsys,
                     b.prin as bprin,
                     b.agent as bagent
              from prod.quantum_atom_snapshot_accounts_v a,
                   asp_migration_spa b
              where a.partition_date_denver = ${hiveconf:RUN_DATE}
              AND a.account_type in ('SUBSCRIBER','NON-EMPLOYEE')
              and a.account_status='CONNECTED'
              and a.legacy_company = b.footprint
              AND b.footprint='BHN'
              and lower(a.extract_type) = Lower(b.customer_type)) a ) b
WHERE keep = 1;

--check employee account (using account_type)
insert into asp_migration_check
Select distinct
       b.wid as wid,
       b.company as company,
       b.account_number as account_number,
       b.system as system,
       b.prin as prin,
       b.agent as agent,
       bprin,
       bagent
FROM
      (select a.wid as wid,
             a.company as company,
             a.account_number as account_number,
             a.system as system,
             a.prin as prin,
             a.agent as agent,
             a.bprin,
             a.bagent,
             case when bagent = 'All' and bprin='All' THEN 1
                  else 0 end as keep
        FROM
              (select b.wid as wid,
                     a.legacy_company as company,
                     a.encrypted_legacy_account_number_256 as account_number,
                     a.system as system,
                     a.prin as prin,
                     a.agent as agent,
                     b.prin as bprin,
                     b.agent as bagent
              from prod.quantum_atom_snapshot_accounts_v a,
                   asp_migration_spa b
              where a.partition_date_denver = ${hiveconf:RUN_DATE}
              AND a.account_type ='EMPLOYEE'
              and a.account_status='CONNECTED'
              and a.legacy_company = b.footprint
              and lower(a.legacy_account_type) is null
              AND b.sys = 'All'
              AND b.prin='All'
              AND b.agent='All'
              AND b.customer_type='Employee') a ) b
WHERE keep = 1
;

--get sum(distinct) for each wave
select wid, company, COUNT(distinct prod.aes_decrypt256(account_number)) ct
from asp_migration_check
group by wid, company
order by wid
;

--get sum(distinct) for all waves
SELECT SUM(CT)
FROM (
  select wid, company, COUNT(distinct prod.aes_decrypt256(account_number)) ct
    from asp_migration_check
   group by wid, company) a
;

--get sum for each wave with SPA considered (This number matches results from 5a)
SELECT wid, company, sum(ct) ct
FROM
  (select wid, company, system, prin, agent,
         COUNT(distinct prod.aes_decrypt256(account_number)) ct
  from asp_migration_check
  group by wid, company, system, prin, agent) a
group by wid, company
order by wid;

--get sum for all waves with SPA considered (This number matches results from 5a)
SELECT sum(ct) ct
  FROM
      (select wid, company, system, prin, agent,
             COUNT(distinct prod.aes_decrypt256(account_number)) ct
      from asp_migration_check
      group by wid, company, system, prin, agent) a;
