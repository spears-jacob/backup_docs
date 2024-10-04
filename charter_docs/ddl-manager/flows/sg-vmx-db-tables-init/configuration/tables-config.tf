locals {

  path_to_nifi_ddl_scripts = "${local.path_to_ddl_scripts_dir}/nifi"
  path_to_daily_ddl_scripts = "${local.path_to_ddl_scripts_dir}/daily"

  tables_config = map(
  "events-pii", [
      {
        db_table_name = "nifi.sg_linear_pe",
        ddl_path = "${local.path_to_nifi_ddl_scripts}/sg_linear_pe.hql"
      },
      {
        db_table_name = "nifi.sg_linear_te",
        ddl_path = "${local.path_to_nifi_ddl_scripts}/sg_linear_te.hql"
      },
      //daily
      {
        db_table_name = "${var.env}.sg_vod_content_asset_deduped_daily",
        ddl_path = "${local.path_to_daily_ddl_scripts}/sg_vod_content_asset_deduped_daily.hql"
      },
      {
        db_table_name = "${var.env}.sg_vod_streams_raw_init_hrly_load",
        ddl_path = "${local.path_to_daily_ddl_scripts}/sg_vod_streams_raw_init_hrly_load.hql"
      },
      {
        db_table_name = "${var.env}.sg_vod_events_deduped_daily",
        ddl_path = "${local.path_to_daily_ddl_scripts}/sg_vod_events_deduped_daily.hql"
      },
      {
        db_table_name = "${var.env}.sg_vod_title_asset_deduped_daily",
        ddl_path = "${local.path_to_daily_ddl_scripts}/sg_vod_title_asset_deduped_daily.hql"
      },
       {
        db_table_name = "${var.env}.sg_vod_streams_deduped_daily",
        ddl_path = "${local.path_to_daily_ddl_scripts}/sg_vod_streams_deduped_daily.hql"
      }
    ]
  )
}
