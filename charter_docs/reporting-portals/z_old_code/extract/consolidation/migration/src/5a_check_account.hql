set RUN_DATE = '2019-06-19';

--use account_type
--legacy_account_type: Non-Employee or Null
--account_type: EMPLOYEE,NON-EMPLOYEE, SUBSCRIBER, ProcessTimestamp
--W5 is combination of W4, W5 and W6

DROP TABLE IF EXISTs asp_migration_acct_direct;
create table asp_migration_acct_direct
AS
SELECT
      ${hiveconf:RUN_DATE},
      'W4' as wave,
       --legacy_company,
       --account_type,
       --count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total,
       count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist
  from prod.quantum_atom_snapshot_accounts_v
 where partition_date_denver = ${hiveconf:RUN_DATE}
   and account_status='CONNECTED'
   AND legacy_company='TWC'
   and lower(account_type) = 'employee'
 GROUP BY legacy_company, account_type
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
from
      (SELECT 'W8' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8347'
      and prin='1000'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W9' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system='8347'
      and prin='1000'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W10' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8109'
      and prin='2000'
      AND agent='6000'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W12' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system='8109'
      and prin='2000'
      AND agent='6000'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W13' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8260'
      and prin in ('1300','1400','1600','1700','1800')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W14' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system='8260'
      and prin in ('1300','1400','1600','1700','1800')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W15' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8448'
      and prin in ('2000','4000','4100','4200','6000','6100','6200')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W16' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8109'
      and prin='1000'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W17' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system in ('8109')
      and prin='1000'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W18' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system='8150'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W19' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8150'
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W20' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system='8448'
      and prin in ('2000','4000','4100','4200','6000','6100','6200')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W21' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system in ('1','2')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
group by wave
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W22' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system in ('1','2','3','4','202')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W23' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='BHN'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      --AND system in ('10','11','12','20','21','22','23','30','31','32','33','40','50','60')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W24' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system='8448'
      and prin in ('3000')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W25' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      AND system='8448'
      and prin in ('3000')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W26' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system in ('3','4')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W27' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='TWC'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'residential'
      AND system in ('202')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
GROUP BY WAVE
;

INSERT INTO asp_migration_acct_direct
SELECT ${hiveconf:RUN_DATE},
       wave,
       sum(account_dist)
FROM
      (SELECT 'W28' as wave,
             legacy_company,
             extract_type,
             system,
             prin,
             agent,
             count(distinct prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_dist,
             count(prod.aes_decrypt256(encrypted_legacy_account_number_256)) as account_total
      from prod.quantum_atom_snapshot_accounts_v
      where partition_date_denver = ${hiveconf:RUN_DATE}
      and account_status='CONNECTED'
      AND legacy_company='BHN'
      AND account_type in ('SUBSCRIBER','NON-EMPLOYEE')
      and lower(extract_type) = 'commercial_business'
      --AND system in ('10','11','12','20','21','22','23','30','31','32','33','40','50','60')
      GROUP BY legacy_company, extract_type, system, prin, agent) a
group by wave
;
