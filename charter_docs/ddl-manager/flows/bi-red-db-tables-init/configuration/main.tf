locals {
  path_to_ddl_scripts_dir = "${path.module}/../scripts"

  tables_config = {
    "feeds-pii" = [
      {
        db_table_name = "${var.env}.atom_accounts_snapshot"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_account_snapshot.hql"
      },
      {
        db_table_name = "${var.env}.sigma_accounts_snapshot_20200106"
        ddl_path = "${local.path_to_ddl_scripts_dir}/sigma_accounts_snapshot_20200106.hql"
      },
      {
        db_table_name = "${var.env}.atom_call_care"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_call_care.hql"
      },
      {
        db_table_name = "${var.env}.atom_cs_call_care_data_3"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_cs_call_care_data.hql"
      },
      {
        db_table_name = "${var.env}.atom_demographic_2"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_demographic_2.hql"
      },
      {
        db_table_name = "${var.env}.atom_fiber_nodes"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_fiber_nodes.hql"
      },
      {
        db_table_name = "${var.env}.atom_homes_passed_latest_20181220"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_homes_passed_latest_20181220.hql"
      },
      {
        db_table_name = "${var.env}.atom_snapshot_equipment_20190201"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_snapshot_equipment_20190201.hql"
      },
      {
        db_table_name = "${var.env}.atom_voice_lines"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_voice_lines.hql"
      },
      {
        db_table_name = "${var.env}.atom_work_orders"
        ddl_path = "${local.path_to_ddl_scripts_dir}/atom_work_orders.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_messages"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_messages.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_client"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_client.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_conflict"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_conflict.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_episode"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_episode.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_error"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_error.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_schedule"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_schedule.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_series"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_series.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_storage"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_storage.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_summary"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_summary.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_dvr_tuner"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_dvr_tuner.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_guide"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_guide.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_instant_upgrade"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_instant_upgrade.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_main_menu"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_main_menu.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_misc"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_misc.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_player"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_player.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_pnm"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_pnm.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_ppv"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_ppv.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_remote_control"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_remote_control.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_start_over"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_start_over.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_vod_error"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_vod_error.hql"
      },
      {
        db_table_name = "${var.env}.atom_odn_vod"
        ddl_path = "${local.path_to_ddl_scripts_dir}/ODN/atom_odn_vod.hql"
      }
    ],
    "incoming-nopii" = [
      {
        db_table_name = "${var.env}.sdl_fiber_node_ingest"
        ddl_path = "${local.path_to_ddl_scripts_dir}/sdl_fiber_node_ingest.hql"
      }
    ]
  }

  s3_bucket_keys = keys(local.tables_config)

}

module "db_table_name_to_config_map_constructor" {
  source = "../../../core/utils/db-table-name-to-config-map-constructor"
  buckey_key_to_config_map = local.tables_config
}
