USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

drop table if exists asp_digital_atom_account_${hiveconf:execid}_${hiveconf:stepid} PURGE;
Create table asp_digital_atom_account_${hiveconf:execid}_${hiveconf:stepid}
as
select encrypted_account_key_256,
       partition_date_denver,
       max(encrypted_padded_account_number_256) as encrypted_padded_account_number_256,
       max(encrypted_legacy_account_number_256) as encrypted_legacy_account_number_256,
       max(has_video_package) as has_video_package,
       max(has_voice_package) as has_voice_package,
       max(has_internet_package) as has_internet_package,
       collect_list(customer_type) as customer_type_list,
       max(legacy_company) as legacy_company,
       max(account_status) as account_status,
       max(has_recurring_payment) as has_recurring_payment,
       max(is_enterprise_customer) as is_enterprise_customer
FROM
(select encrypted_account_key_256,
        encrypted_padded_account_number_256,
        encrypted_legacy_account_number_256,
        has_video_package,
        has_voice_package,
        has_internet_package,
        customer_type,
        legacy_company,
        account_status,
        has_recurring_payment,
        is_enterprise_customer,
        video_package_type,
        internet_tier,
        internet_product_tier,
        voice_subscriber_count,
        partition_date_denver
from red_atom_snapshot_accounts_v
where legacy_company in ('CHR','TWC','BHN')
  and extract_source = 'P270'
  AND account_status = 'CONNECTED'
  AND account_type = 'SUBSCRIBER'
AND partition_date_denver between '${hiveconf:START_DATE_DENVER}' and DATE_ADD('${hiveconf:END_DATE_DENVER}',1)) a
group by encrypted_account_key_256, partition_date_denver;
