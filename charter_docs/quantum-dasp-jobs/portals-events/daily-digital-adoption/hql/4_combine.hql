USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

DROP TABLE if EXISTS asp_digital_combine_step_1_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_combine_step_1_${hiveconf:execid}_${hiveconf:stepid} as
SELECT a.encrypted_account_key_256,
       a.partition_date_denver,
       a.encrypted_padded_account_number_256,
       a.encrypted_legacy_account_number_256,
       a.has_video_package,
       a.has_voice_package,
       a.has_internet_package,
       a.customer_type_list,
       a.legacy_company,
       a.account_status,
       a.has_recurring_payment,
       a.is_enterprise_customer,
       b.account_key_callfeatures,
       b.call_end_date_denver_callfeatures,
       b.call_counts,
       b.truck_roll_flag,
       b.call_mso,
       b.cause_group,
       b.segment_counts,
       b.cause_list,
       b.description_list,
       b.customer_journey_list,
       b.customer_journey_calls_map,
       b.customer_journey_segments_map,
       b.digital_first_call_counts,
       b.digital_first_journey_action_map,
       b.digital_first_journey_segment_map,
       b.digital_first_journey_call_map
  from asp_digital_atom_account_${hiveconf:execid}_${hiveconf:stepid} a
  LEFT JOIN asp_digital_call_${hiveconf:execid}_${hiveconf:stepid} b
    ON a.encrypted_account_key_256=b.account_key_callfeatures
   and a.partition_date_denver=b.call_end_date_denver_callfeatures
 where a.partition_date_denver between '${hiveconf:START_DATE_DENVER}' and '${hiveconf:END_DATE_DENVER}';

DROP TABLE if EXISTS asp_digital_combine_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_combine_${hiveconf:execid}_${hiveconf:stepid} as
SELECT *
  from asp_digital_combine_step_1_${hiveconf:execid}_${hiveconf:stepid} a
  LEFT JOIN asp_digital_portals_spec_${hiveconf:execid}_${hiveconf:stepid} b
    ON a.encrypted_legacy_account_number_256=b.account_number_aes_256_spec
   and a.partition_date_denver=b.received_date_denver_spec
   AND a.legacy_company=b.visit_mso_spec
  LEFT JOIN asp_digital_portals_msa_${hiveconf:execid}_${hiveconf:stepid} c
    ON a.encrypted_legacy_account_number_256=c.account_number_aes_256_msa
   and a.partition_date_denver=c.received_date_denver_msa
   AND a.legacy_company=c.visit_mso_msa
  LEFT JOIN asp_digital_portals_smb_${hiveconf:execid}_${hiveconf:stepid} d
    ON a.encrypted_legacy_account_number_256=d.account_number_aes_256_smb
   and a.partition_date_denver=d.received_date_denver_smb
   AND a.legacy_company=d.visit_mso_smb
  LEFT JOIN asp_digital_portals_specmobile_${hiveconf:execid}_${hiveconf:stepid} e
    ON a.encrypted_padded_account_number_256=e.account_number_aes_256_specmobile
   and a.partition_date_denver=e.received_date_denver_specmobile
   AND a.legacy_company=e.visit_mso_specmobile
  LEFT JOIN asp_digital_portals_selfinstall_${hiveconf:execid}_${hiveconf:stepid} f
    ON a.encrypted_padded_account_number_256=f.account_number_aes_256_selfinstall
   and a.partition_date_denver=f.received_date_denver_selfinstall
   AND a.legacy_company=f.visit_mso_selfinstall
  LEFT JOIN asp_digital_portals_buyflow_${hiveconf:execid}_${hiveconf:stepid} g
    ON a.encrypted_padded_account_number_256=g.account_number_aes_256_buyflow
   and a.partition_date_denver=g.received_date_denver_buyflow
   AND a.legacy_company=g.visit_mso_buyflow;

   INSERT overwrite TABLE asp_digital_adoption_daily PARTITION(partition_date_denver)
   SELECT
          account_key_callfeatures,
          account_number_aes_256_msa,
          account_number_aes_256_smb,
          account_number_aes_256_spec,
          account_number_aes_256_specmobile,
          account_number_aes_256_selfinstall,
          account_number_aes_256_buyflow,
          account_status,
          action_count_msa,
          action_count_smb,
          action_count_spec,
          action_count_specmobile,
          action_count_selfinstall,
          action_count_buyflow,
          call_mso,
          call_counts,
          call_end_date_denver_callfeatures,
          cause_group,
          cause_list,
          customer_journey_calls_map,
          customer_journey_list,
          customer_journey_segments_map,
          customer_journey_msa,
          customer_journey_smb,
          customer_journey_spec,
          customer_journey_selfinstall,
          customer_journey_buyflow,
          customer_type_list,
          customer_visit_journey_msa,
          customer_visit_journey_smb,
          customer_visit_journey_spec,
          customer_visit_journey_selfinstall,
          customer_visit_journey_buyflow,
          description_list,
          digital_first_call_counts,
          digital_first_journey_action_map,
          digital_first_journey_call_map,
          digital_first_journey_segment_map,
          distinct_visits_msa,
          distinct_visits_smb,
          distinct_visits_spec,
          distinct_visits_specmobile,
          distinct_visits_selfinstall,
          distinct_visits_buyflow,
          encrypted_account_key_256,
          encrypted_legacy_account_number_256,
          encrypted_padded_account_number_256,
          has_internet_package,
          has_recurring_payment,
          has_video_package,
          has_voice_package,
          is_enterprise_customer,
          key_journey_map_list_msa,
          key_journey_map_list_smb,
          key_journey_map_list_spec,
          key_journey_map_list_selfinstall,
          key_journey_map_list_buyflow,
          legacy_company,
          page_name_list_smb,
          page_name_list_spec,
          page_name_list_selfinstall,
          page_name_list_buyflow,
          received_date_denver_msa,
          received_date_denver_smb,
          received_date_denver_spec,
          received_date_denver_specmobile,
          received_date_denver_selfinstall,
          received_date_denver_buyflow,
          segment_counts,
          support_count_smb,
          support_count_spec,
          support_count_selfinstall,
          support_count_buyflow,
          truck_roll_flag,
          visit_mso_msa,
          visit_mso_smb,
          visit_mso_spec,
          visit_mso_selfinstall,
          visit_mso_buyflow,
          partition_date_denver
     FROM asp_digital_combine_${hiveconf:execid}_${hiveconf:stepid};

DROP TABLE IF EXISTS asp_digital_atom_account_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_0_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_0_sspp_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_1_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_1_cqe_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_1_btm_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_1_atom_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_1_combine_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_for_call_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_step_2_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_spec_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_msa_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_smb_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_specmobile_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_selfinstall_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_portals_buyflow_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_step_1_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_step_2_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_step_3_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_step_4_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_step_5_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_step_6_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_call_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_combine_step_1_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
DROP TABLE IF EXISTS asp_digital_combine_${hiveconf:execid}_${hiveconf:stepid}  PURGE;
