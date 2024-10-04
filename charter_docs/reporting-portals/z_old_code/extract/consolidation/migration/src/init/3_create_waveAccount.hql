USE ${env:ENVIRONMENT};

SELECT "Running Create WaveAccount";
--for non-employee account
DROP TABLE IF EXISTS asp_migration_acct;
create table if NOT exists asp_migration_acct
AS
Select distinct
       b.wid as wid,
       b.company as company,
       b.customer_type as customer_type,
       b.system,
       b.prin,
       b.agent,
       b.acct_id as acct_id_256
  FROM
    (select a.wid as wid,
            a.company as company,
            a.customer_type as customer_type,
            a.acct_id as acct_id,
            a.system as system,
            a.prin as prin,
            a.agent as agent,
            case when bagent = 'All' and bprin='All' THEN 1
                 when bagent = 'All' and bprin!= 'All' AND a.prin = bprin THEN 1
                 when bagent != 'All' and a.agent=bagent AND a.prin = bprin THEN 1
                 else 0 end as keep
       FROM
          (select b.wid as wid,
                  a.legacy_company as company,
                  a.extract_type as customer_type,
                  a.encrypted_legacy_account_number_256 as acct_id,
                  a.system as system,
                  a.prin as prin,
                  a.agent as agent,
                  b.prin as bprin,
                  b.agent as bagent
             from prod.quantum_atom_snapshot_accounts_v a,
                  asp_migration_spa b
            where a.partition_date_denver = DATE_SUB("${env:MigrationStartDate}",2)
              AND a.account_type in ('SUBSCRIBER','NON-EMPLOYEE')
              and a.account_status='CONNECTED'
              and a.legacy_company = b.footprint
              AND b.footprint ='TWC'
              and lower(a.extract_type) = Lower(b.customer_type)
              and a.system = b.sys) a ) b
WHERE keep = 1;

--For BHN
insert into asp_migration_acct
Select distinct
       b.wid as wid,
       b.company as company,
       b.customer_type as customer_type,
       b.system,
       b.prin,
       b.agent,
       b.acct_id as acct_id_256
  FROM
    (select a.wid as wid,
            a.company as company,
            a.customer_type as customer_type,
            a.acct_id as acct_id,
            a.system as system,
            a.prin as prin,
            a.agent as agent,
            case when bsys = 'All' AND bagent = 'All' and bprin='All' THEN 1
                 when bagent = 'All' and bprin='All' THEN 1
                 when bagent = 'All' and bprin!= 'All' AND a.prin = bprin THEN 1
                 when bagent != 'All' and a.agent=bagent AND a.prin = bprin THEN 1
                 else 0 end as keep
       FROM
          (select b.wid as wid,
                  a.legacy_company as company,
                  a.extract_type as customer_type,
                  a.encrypted_legacy_account_number_256 as acct_id,
                  a.system as system,
                  a.prin as prin,
                  a.agent as agent,
                  b.sys as bsys,
                  b.prin as bprin,
                  b.agent as bagent
             from prod.quantum_atom_snapshot_accounts_v a,
                  asp_migration_spa b
            where a.partition_date_denver = DATE_SUB("${env:MigrationStartDate}",2)
              AND a.account_type in ('SUBSCRIBER','NON-EMPLOYEE')
              and a.account_status='CONNECTED'
              and a.legacy_company = b.footprint
              AND b.footprint='BHN'
              and lower(a.extract_type) = Lower(b.customer_type)) a ) b
WHERE keep = 1;

--for employee account
insert into asp_migration_acct
Select distinct
       b.wid as wid,
       b.company as company,
       b.customer_type as customer_type,
       b.system as system,
       b.prin as prin,
       b.agent as agent,
       b.acct_id as acct_id_256
  FROM
    (select a.wid as wid,
            a.company as company,
            a.acct_id as acct_id,
            a.customer_type as customer_type,
            a.system AS system,
            a.prin as prin,
            a.agent as agent,
            case when bagent = 'All' and bprin='All' THEN 1
                 else 0 end as keep
       FROM
          (select b.wid as wid,
                  a.legacy_company as company,
                  'Employee' as customer_type,
                  a.encrypted_legacy_account_number_256 as acct_id,
                  a.system as system,
                  a.prin as prin,
                  a.agent as agent,
                  b.prin as bprin,
                  b.agent as bagent
             from prod.quantum_atom_snapshot_accounts_v a,
                  asp_migration_spa b
            where a.partition_date_denver = DATE_SUB("${env:MigrationStartDate}",2)
              AND a.account_type='EMPLOYEE'
              and a.account_status='CONNECTED'
              and a.legacy_company = b.footprint
              --and lower(a.legacy_account_type) = Lower(b.customer_type)
              AND b.sys = 'All'
              AND b.prin='All'
              AND b.agent='All'
              AND b.customer_type='Employee'
) a ) b
WHERE keep = 1
;

--combine account with wave date
DROP VIEW IF EXISTS asp_v_migration_acct;
create VIEW if NOT exists asp_v_migration_acct
AS
SELECT * FROM
(Select a.wid as wid,
       a.company as company,
       a.customer_type as customer_type,
       a.system,
       a.prin,
       a.agent,
       a.acct_id_256 as acct_id_256,
       b.wstart as wstart,
       b.wend AS wend
  FROM asp_migration_acct a,
       asp_migration_date b
 WHERE a.wid = b.wid
   AND a.wid not in (10,12,16,17)
 UNION ALL
 Select 4 as wid,
        'TWC' as company,
        'employee' as customer_type,
        sys,
        prin,
        agnt,
        prod.aes_encrypt256(account) as acct_id_256,
        '2019-02-22' as wstart,
        '2019-04-04' AS wend
   FROM asp_migration_employee) a;

DROP TABLE IF EXISTS asp_migration_acct_8109;
create table if NOT exists asp_migration_acct_8109 (
  wid int,
  company string,
  customer_type string,
  system string,
  prin string,
  agent string,
  acct_count int,
  wstart string,
  wend string);

insert into asp_migration_acct_8109 Values
(10,'TWC','COMMERCIAL_BUSINESS','8109','2000','6000',5000,'2019-07-09','2019-07-23');
insert into asp_migration_acct_8109 Values
(12,'TWC','RESIDENTIAL','8109','2000','6000',25000,'2019-07-16','2019-07-30');
insert into asp_migration_acct_8109 Values
(16,'TWC','COMMERCIAL_BUSINESS','8109','1000','',11126,'2019-07-23','2019-08-06');
insert into asp_migration_acct_8109 Values
(17,'TWC','RESIDENTIAL','8109','1000','',407777,'2019-07-23','2019-08-06');
